package container

import (
	"io"
)

const WASI_FACTORY_KEY = "wasi"
const DOCKER_FACTORY_KEY = "docker"

// A Factory to create and manage container.
type Factory interface {
	Create(string, *ContainerOptions) (ContainerID, error)
	CopyToContainer(ContainerID, io.Reader, string) error
	Start(ContainerID) error
	Destroy(ContainerID) error
	HasImage(string) bool
	PullImage(string) error
	GetIPAddress(ContainerID) (string, error)
	GetMemoryMB(id ContainerID) (int64, error)
}

// ContainerOptions contains options for container creation.
type ContainerOptions struct {
	Cmd      []string
	Env      []string
	MemoryMB int64
	CPUQuota float64
}

type ContainerID = string

// Factories for this node; currently supporting only Docker and WASI
var factories map[string]Factory

func GetFactoryFromRuntime(runtime string) Factory {
	if runtime == WASI_RUNTIME {
		return factories[WASI_FACTORY_KEY]
	} else {
		return factories[DOCKER_FACTORY_KEY]
	}
}

func DownloadImage(image string, forceRefresh bool, runtime string) error {
	cf := GetFactoryFromRuntime(runtime)
	if forceRefresh || !cf.HasImage(image) {
		return cf.PullImage(image)
	}
	return nil
}
