package container

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/image"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	regName "github.com/google/go-containerregistry/pkg/name"
	regRemote "github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/utils"
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
	return cf.cli.CopyToContainer(cf.ctx, contID, destPath, content, container.CopyToContainerOptions{})
}

func (cf *DockerFactory) Start(contID ContainerID) error {
	if err := cf.cli.ContainerStart(cf.ctx, contID, container.StartOptions{}); err != nil {
		return err
	}

	return nil
}

func (cf *DockerFactory) Destroy(contID ContainerID) error {
	// force set to true causes running container to be killed (and then
	// removed)
	return cf.cli.ContainerRemove(cf.ctx, contID, container.RemoveOptions{Force: true})
}

var mutex = sync.Mutex{}

func (cf *DockerFactory) HasImage(img string) bool {
	mutex.Lock()
	list, err := cf.cli.ImageList(context.TODO(), image.ListOptions{
		All:        false,
		Filters:    filters.Args{},
		SharedSize: false,
	})
	mutex.Unlock()
	if err != nil {
		fmt.Printf("image list error: %v\n", err)
		return false
	}
	for _, summary := range list {
		if len(summary.RepoTags) > 0 && strings.HasPrefix(summary.RepoTags[0], img) {
			// We have the img, but we may need to refresh it
			if config.GetBool(config.FACTORY_REFRESH_IMAGES, false) {
				if refreshed, ok := refreshedImages[img]; !ok || !refreshed {
					return false
				}
			}
			return true
		}
	}
	return false
}

func (cf *DockerFactory) PullImage(img string) error {
	pullResp, err := cf.cli.ImagePull(cf.ctx, img, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("Could not pull image '%s': %v", img, err)
	}

	defer func(pullResp io.ReadCloser) {
		err := pullResp.Close()
		if err != nil {
			log.Printf("Could not close the docker image pull response\n")
		}
	}(pullResp)
	// This seems to be necessary to wait for the img to be pulled:
	_, _ = io.Copy(io.Discard, pullResp)
	log.Printf("Pulled image: %s\n", img)
	refreshedImages[img] = true
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
	logsReader, err := cf.cli.ContainerLogs(cf.ctx, contID, container.LogsOptions{
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

// GetImageArchitectures retrieves the supported CPU architectures for a given container image.
// It first checks etcd for cached information. If not found, it queries the remote registry
// and then caches the result in etcd. As for now, architectures of interest are: x86_64 (amd64) and arm64.
func (cf *DockerFactory) GetImageArchitectures(imageName string) ([]string, error) {
	etcdCli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get etcd client: %w", err)
	}

	const imageArchEtcdPrefix = "/serverledge/image_architectures/"
	etcdKey := imageArchEtcdPrefix + imageName
	ctx, cancel := context.WithTimeout(cf.ctx, 5*time.Second)
	defer cancel()

	// 1. Try to get architectures from etcd
	resp, err := etcdCli.Get(ctx, etcdKey)
	if err == nil && len(resp.Kvs) > 0 {
		var architectures []string
		if err := json.Unmarshal(resp.Kvs[0].Value, &architectures); err != nil {
			log.Printf("Warning: failed to unmarshal architectures from etcd for image %s: %v. Re-fetching from registry.", imageName, err)
		} else {
			log.Printf("Architectures for image %s found in etcd: %v", imageName, architectures)
			return architectures, nil
		}
	} else if err != nil {
		// Log the error if it's not just "key not found"
		if !strings.Contains(err.Error(), "key not found") { // etcd client returns error if key not found
			log.Printf("Warning: failed to get architectures from etcd for image %s: %v. Re-fetching from registry.", imageName, err)
		} else {
			log.Printf("Architectures for image %s not found in etcd. Fetching from registry.", imageName)
		}
	} else {
		log.Printf("Architectures for image %s not found in etcd. Fetching from registry.", imageName)
	}

	// 2. If not found in etcd (or unmarshal failed), query the remote registry
	ref, err := regName.ParseReference(imageName)
	if err != nil {
		return nil, fmt.Errorf("failed to parse image name %s: %w", imageName, err)
	}

	desc, err := regRemote.Get(ref)
	if err != nil {
		return nil, fmt.Errorf("failed to get remote image descriptor for %s: %w", imageName, err)
	}

	var supportedArchitectures []string  // the list we will return
	archSet := make(map[string]struct{}) // Use a set to avoid duplicates (it's just temporary, won't be returned)

	if desc.MediaType.IsIndex() { // Multi-platform images have an index manifest for the multiple architectures
		idx, err := desc.ImageIndex()
		if err != nil {
			return nil, fmt.Errorf("failed to get image index for %s: %w", imageName, err)
		}
		manifests, err := idx.IndexManifest()
		if err != nil {
			return nil, fmt.Errorf("failed to get index manifest for %s: %w", imageName, err)
		}

		for _, manifest := range manifests.Manifests {
			if manifest.Platform != nil {
				arch := manifest.Platform.Architecture
				// We are interested in "amd64" (x86_64) and "arm64", for the moment we don't care if more architectures are supported
				if arch == X86 || arch == ARM {
					if _, found := archSet[arch]; !found {
						supportedArchitectures = append(supportedArchitectures, arch)
						archSet[arch] = struct{}{} // to avoid duplicates
					}
				}
			}
		}
	} else if desc.MediaType.IsImage() { // Single-platform image
		img, err := desc.Image()
		if err != nil {
			return nil, fmt.Errorf("failed to get image for %s: %w", imageName, err)
		}
		cfg, err := img.ConfigFile()
		if err != nil {
			return nil, fmt.Errorf("failed to get image config for %s: %w", imageName, err)
		}
		arch := cfg.Architecture
		if arch == X86 || arch == ARM {
			supportedArchitectures = append(supportedArchitectures, arch)
		}
	} else {
		return nil, fmt.Errorf("unsupported media type for image %s: %s", imageName, desc.MediaType)
	}

	if len(supportedArchitectures) == 0 {
		return nil, fmt.Errorf("no architecture supported by Serverledge found for image %s", imageName)

	}

	// 3. Cache the result in etcd for future lookup (e.g.: another function executed in the same custom runtime, or can
	// be used by another node if/when the function is offloaded) without having to hit the registry again.
	// TODO maybe TTL for cache?
	archBytes, err := json.Marshal(supportedArchitectures)
	if err != nil {
		log.Printf("Warning: failed to marshal architectures for image %s: %v. Not caching in etcd.", imageName, err)
		// Return what we found, even if not cached, is an etcd problem, not a problem retrieving architectures
		return supportedArchitectures, nil
	}

	_, err = etcdCli.Put(ctx, etcdKey, string(archBytes))
	if err != nil {
		log.Printf("Warning: failed to put architectures to etcd for image %s: %v", imageName, err)
	} else {
		log.Printf("Architectures for image %s cached in etcd: %v", imageName, supportedArchitectures)
	}

	return supportedArchitectures, nil
}
