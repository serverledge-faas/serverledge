package lb

import (
	"errors"
	"math/rand"
)

type policy interface {
	Route(funcName string) (string, error)
}

type randomPolicy struct{}

func (r *randomPolicy) Route(funcName string) (string, error) {
	targetsMutex.RLock()
	defer targetsMutex.RUnlock()

	skip := rand.Intn(len(currentTargets))
	i := 0

	for _, value := range currentTargets {
		if i == skip {
			return value.APIUrl(), nil
		}
		i++
	}

	return "", errors.New("no targets")
}
