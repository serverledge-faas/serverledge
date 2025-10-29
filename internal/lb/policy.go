package lb

import (
	"errors"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"math/rand"
)

type policy interface {
	Route(funcName string) (string, error)
	OnNodeArrival(registration *registration.NodeRegistration)
	OnNodeDeletion(registration *registration.NodeRegistration)
	OnStatusUpdate(registration *registration.NodeRegistration, status registration.StatusInformation)
	OnRequestComplete(funcName string, nodeKey string)
}

type randomPolicy struct{}

func (r *randomPolicy) OnRequestComplete(funcName string, node string) {
	//TODO implement me
	panic("implement me")
}

func (r *randomPolicy) Route(funcName string) (string, error) {
	targetsMutex.RLock()
	defer targetsMutex.RUnlock()

	skip := rand.Intn(len(currentTargets))
	i := 0

	for _, value := range currentTargets {
		if i == skip {
			return value.Key, nil
		}
		i++
	}

	return "", errors.New("no targets")
}

func (r *randomPolicy) OnNodeArrival(registration *registration.NodeRegistration) {}

func (r *randomPolicy) OnNodeDeletion(registration *registration.NodeRegistration) {}

func (r *randomPolicy) OnStatusUpdate(registration *registration.NodeRegistration, status registration.StatusInformation) {
}
