package container

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	//	"github.com/docker/docker/pkg/stdcopy"
)

type DockerFactory struct {
	cli *client.Client
	ctx context.Context
}

func InitDockerContainerFactory() *DockerFactory {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	dockerFact := &DockerFactory{cli, ctx}
	cf = dockerFact
	return dockerFact
}

func (cf *DockerFactory) Create(image string, opts *ContainerOptions) (ContainerID, error) {
	if !cf.HasImage(image) {
		_ = cf.PullImage(image)
		// error ignored, as we might still have a stale copy of the image
	}

	contResources := container.Resources{Memory: opts.MemoryMB * 1048576} // convert to bytes
	if opts.CPUQuota > 0.0 {
		contResources.CPUPeriod = 50000 // 50ms
		contResources.CPUQuota = (int64)(50000.0 * opts.CPUQuota)
	}

	resp, err := cf.cli.ContainerCreate(cf.ctx, &container.Config{
		Image: image,
		Cmd:   opts.Cmd,
		Env:   opts.Env,
		Tty:   false,
	}, &container.HostConfig{Resources: contResources}, nil, nil, "")

	if err != nil {
		log.Printf("Could not create the container: %v\n", err)
		return "", err
	}

	id := resp.ID

	r, err := cf.cli.ContainerInspect(cf.ctx, id)
	log.Printf("Container %s has name %s\n", id, r.Name)

	return id, err
}

func (cf *DockerFactory) CopyToContainer(contID ContainerID, content io.Reader, destPath string) error {
	return cf.cli.CopyToContainer(cf.ctx, contID, destPath, content, types.CopyToContainerOptions{})
}

func (cf *DockerFactory) Start(contID ContainerID) error {
	if err := cf.cli.ContainerStart(cf.ctx, contID, types.ContainerStartOptions{}); err != nil {
		return err
	}

	return nil
}

func (cf *DockerFactory) Destroy(contID ContainerID) error {
	// force set to true causes running container to be killed (and then
	// removed)
	return cf.cli.ContainerRemove(cf.ctx, contID, types.ContainerRemoveOptions{Force: true})
}

var mutex = sync.Mutex{}

func (cf *DockerFactory) HasImage(image string) bool {
	mutex.Lock()
	list, err := cf.cli.ImageList(context.TODO(), types.ImageListOptions{
		All:            false,
		Filters:        filters.Args{},
		SharedSize:     false,
		ContainerCount: false,
	})
	mutex.Unlock()
	if err != nil {
		fmt.Printf("image list error: %v\n", err)
		return false
	}
	for _, summary := range list {
		if len(summary.RepoTags) > 0 && strings.HasPrefix(summary.RepoTags[0], image) {
			// We have the image, but we may need to refresh it
			if config.GetBool(config.FACTORY_REFRESH_IMAGES, false) {
				if refreshed, ok := refreshedImages[image]; !ok || !refreshed {
					return false
				}
			}
			return true
		}
	}
	return false
}

func (cf *DockerFactory) PullImage(image string) error {
	pullResp, err := cf.cli.ImagePull(cf.ctx, image, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("Could not pull image '%s': %v", image, err)
	}

	defer func(pullResp io.ReadCloser) {
		err := pullResp.Close()
		if err != nil {
			log.Printf("Could not close the docker image pull response\n")
		}
	}(pullResp)
	// This seems to be necessary to wait for the image to be pulled:
	_, _ = io.Copy(io.Discard, pullResp)
	log.Printf("Pulled image: %s\n", image)
	refreshedImages[image] = true
	return nil
}

func (cf *DockerFactory) GetIPAddress(contID ContainerID) (string, error) {
	contJson, err := cf.cli.ContainerInspect(cf.ctx, contID)
	if err != nil {
		return "", err
	}
	return contJson.NetworkSettings.IPAddress, nil
}

func (cf *DockerFactory) GetMemoryMB(contID ContainerID) (int64, error) {
	contJson, err := cf.cli.ContainerInspect(cf.ctx, contID)
	if err != nil {
		return -1, err
	}
	return contJson.HostConfig.Memory / 1048576, nil
}

func (cf *DockerFactory) GetLog(contID ContainerID) (string, error) {
	logsReader, err := cf.cli.ContainerLogs(cf.ctx, contID, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Since:      "",
		Until:      "",
		Timestamps: false,
		Follow:     false,
		Tail:       "",
		Details:    false,
	})
	if err != nil {
		return "no logs", fmt.Errorf("can't get the logs: %v", err)
	}
	logs, err := io.ReadAll(logsReader)
	if err != nil {
		return "no logs", fmt.Errorf("can't read the logs: %v", err)
	}
	return string(logs[:]), nil
}
