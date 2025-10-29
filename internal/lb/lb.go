package lb

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

var client *http.Client
var lbPolicy policy
var targetsMutex sync.RWMutex
var currentTargets map[string]registration.NodeRegistration

func newPolicy() policy {
	policyName := config.GetString(config.LOAD_BALANCER_POLICY, "round-robin")
	if policyName == "random" {
		return &randomPolicy{}
	} else {
		panic("unknown policy: " + policyName)
	}
}

func handleInvoke(c echo.Context) error {

	funcName := c.Param("fun")
	// Select backend
	targetURL, err := lbPolicy.Route(funcName)

	// Create a new HTTP request to forward to the selected backend
	newURL, _ := url.JoinPath(targetURL, c.Request().RequestURI)
	req, err := http.NewRequest(c.Request().Method, newURL, c.Request().Body)
	if err != nil {
		return err
	}
	// Copy the request headers to the new request
	req.Header = c.Request().Header

	// Send the request to the backend using the global HTTP client
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// function has been actually executed
		// TODO: update info?
	}

	res := c.Response()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			res.Header().Add(key, value)
		}
	}

	// Set status code and copy body
	res.WriteHeader(resp.StatusCode)
	_, err = io.Copy(res.Writer, resp.Body)
	return err

}

func handleOtherRequest(c echo.Context) error {

	res := c.Response()
	// TODO: status should be handled differently

	var targetURL string
	for _, target := range currentTargets {
		targetURL = target.APIUrl()
		break
	}
	// Create a new HTTP request to forward to the selected backend
	newURL, _ := url.JoinPath(targetURL, c.Request().RequestURI)
	req, err := http.NewRequest(c.Request().Method, newURL, c.Request().Body)
	if err != nil {
		return err
	}
	// Copy the request headers to the new request
	req.Header = c.Request().Header

	// Send the request to the backend using the global HTTP client
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			res.Header().Add(key, value)
		}
	}

	// Set status code and copy body
	res.WriteHeader(resp.StatusCode)
	_, err = io.Copy(res.Writer, resp.Body)
	return err

}

func StartReverseProxy(e *echo.Echo, region string) {
	var err error

	currentTargets, err = registration.GetNodesInArea(region, false, 0)
	if err != nil {
		log.Printf("Cannot connect to registry to retrieve targets: %v\n", err)
		os.Exit(2)
	}

	log.Printf("Initializing with %d targets.\n", len(currentTargets))
	lbPolicy = newPolicy()

	go updateTargets(region)

	tr := &http.Transport{
		MaxIdleConns:        2500,
		MaxIdleConnsPerHost: 2500,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     10 * time.Minute,
	}
	client = &http.Client{Transport: tr}

	e.HideBanner = true
	e.Use(middleware.Recover())

	// Routes
	e.POST("/invoke/:fun", handleInvoke)
	e.Any("/*", handleOtherRequest)

	portNumber := config.GetInt(config.API_PORT, 1323)
	if err := e.Start(fmt.Sprintf(":%d", portNumber)); err != nil && !errors.Is(err, http.ErrServerClosed) {
		e.Logger.Fatal("shutting down the server")
	}

}

func updateTargets(region string) {
	for {
		time.Sleep(10 * time.Second) // TODO: configure

		newTargets, err := registration.GetNodesInArea(region, false, 0)
		if err != nil || newTargets == nil {
			log.Printf("Cannot update targets: %v\n", err)
		}

		targetsMutex.Lock()
		for k, _ := range currentTargets {
			if _, ok := newTargets[k]; !ok {
				// this target is not present any more
				log.Printf("Removing target: %s\n", k)
			}
		}

		for k, _ := range newTargets {
			if _, ok := currentTargets[k]; !ok {
				// this target was not present
				log.Printf("Adding new target: %s\n", k)
			}
		}

		currentTargets = newTargets
		targetsMutex.Unlock()
	}
}
