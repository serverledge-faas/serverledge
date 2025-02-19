package lb

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var currentTargets []*middleware.ProxyTarget

func newBalancer(targets []*middleware.ProxyTarget) middleware.ProxyBalancer {
	return middleware.NewRoundRobinBalancer(targets)
}

func StartReverseProxy(e *echo.Echo, region string) {
	targets, err := getTargets(region)
	if err != nil {
		log.Printf("Cannot connect to registry to retrieve targets: %v\n", err)
		os.Exit(2)
	}

	log.Printf("Initializing with %d targets.\n", len(targets))
	balancer := newBalancer(targets)
	currentTargets = targets
	e.Use(middleware.Proxy(balancer))

	go updateTargets(balancer, region)

	portNumber := config.GetInt(config.API_PORT, 1323)
	if err := e.Start(fmt.Sprintf(":%d", portNumber)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		e.Logger.Fatal("shutting down the server")
	}
}

func getTargets(region string) ([]*middleware.ProxyTarget, error) {
	cloudNodes, err := registration.GetCloudNodes(region)
	if err != nil {
		return nil, err
	}

	targets := make([]*middleware.ProxyTarget, 0, len(cloudNodes))
	for _, addr := range cloudNodes {
		log.Printf("Found target: %v\n", addr)
		// TODO: etcd should NOT contain URLs, but only host and port...
		parsedUrl, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		targets = append(targets, &middleware.ProxyTarget{Name: addr, URL: parsedUrl})
	}

	log.Printf("Found %d targets\n", len(targets))

	return targets, nil
}

func updateTargets(balancer middleware.ProxyBalancer, region string) {
	for {
		time.Sleep(30 * time.Second) // TODO: configure

		targets, err := getTargets(region)
		if err != nil {
			log.Printf("Cannot update targets: %v\n", err)
		}

		toKeep := make([]bool, len(currentTargets))
		for i := range currentTargets {
			toKeep[i] = false
		}
		for _, t := range targets {
			toAdd := true
			for i, curr := range currentTargets {
				if curr.Name == t.Name {
					toKeep[i] = true
					toAdd = false
				}
			}
			if toAdd {
				log.Printf("Adding %s\n", t.Name)
				balancer.AddTarget(t)
			}
		}

		toRemove := make([]string, 0)
		for i, curr := range currentTargets {
			if !toKeep[i] {
				log.Printf("Removing %s\n", curr.Name)
				toRemove = append(toRemove, curr.Name)
			}
		}
		for _, curr := range toRemove {
			balancer.RemoveTarget(curr)
		}

		currentTargets = targets
	}
}
