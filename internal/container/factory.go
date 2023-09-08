package container

import (
	"io"
)

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
	GetLog(id ContainerID) (string, error)
}

// ContainerOptions contains options for container creation.
type ContainerOptions struct {
	Cmd      []string
	Env      []string
	MemoryMB int64
	CPUQuota float64
}

type ContainerID = string

// cf is the container factory for the node
var cf Factory

func DownloadImage(image string, forceRefresh bool) error {
	if forceRefresh || !cf.HasImage(image) {
		return cf.PullImage(image)
	}
	return nil
}
