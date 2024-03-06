package container

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/grussorusso/serverledge/internal/executor"
)

// NewContainer creates and starts a new container.
func NewContainer(image, codeTar string, opts *ContainerOptions) (ContainerID, error) {
	contID, err := cf.Create(image, opts)
	if err != nil {
		log.Printf("Failed container creation")
		return "", err
	}

	if len(codeTar) > 0 {
		decodedCode, _ := base64.StdEncoding.DecodeString(codeTar)
		err = cf.CopyToContainer(contID, bytes.NewReader(decodedCode), "/app/")
		if err != nil {
			log.Printf("Failed code copy")
			return "", err
		}
	}

	err = cf.Start(contID)
	if err != nil {
		return "", err
	}

	return contID, nil
}

// Execute interacts with the Executor running in the container to invoke the
// function through a HTTP request.
func Execute(contID ContainerID, req *executor.InvocationRequest) (*executor.InvocationResult, time.Duration, error) {
	ipAddr, err := cf.GetIPAddress(contID)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to retrieve IP address for container: %v", err)
	}

	postBody, _ := json.Marshal(req)
	postBodyB := bytes.NewBuffer(postBody)
	resp, waitDuration, err := sendPostRequestWithRetries(fmt.Sprintf("http://%s:%d/invoke", ipAddr,
		executor.DEFAULT_EXECUTOR_PORT), postBodyB)
	if err != nil || resp == nil {
		return nil, waitDuration, fmt.Errorf("Request to executor failed: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error while closing response body")
		}
	}(resp.Body)

	d := json.NewDecoder(resp.Body)
	response := &executor.InvocationResult{}
	err = d.Decode(response)
	if err != nil {
		return nil, waitDuration, fmt.Errorf("Parsing executor response failed: %v", err)
	}

	return response, waitDuration, nil
}

func GetMemoryMB(id ContainerID) (int64, error) {
	return cf.GetMemoryMB(id)
}

func Destroy(id ContainerID) error {
	return cf.Destroy(id)
}

func sendPostRequestWithRetries(url string, body *bytes.Buffer) (*http.Response, time.Duration, error) {
	const TIMEOUT_MILLIS = 30000
	const MAX_BACKOFF_MILLIS = 500
	var backoffMillis = 25
	var totalWaitMillis = 0
	var attempts = 1

	var err error

	for totalWaitMillis < TIMEOUT_MILLIS {
		resp, err := http.Post(url, "application/json", body)
		if err == nil {
			return resp, time.Duration(totalWaitMillis * int(time.Millisecond)), err
		} else if attempts > 3 {
			// It is common to have a failure after a cold start, so
			// we avoid logging failures on the first attempt(s)
			log.Printf("Invocation POST failed (attempt %d): %v", attempts, err)
		}

		time.Sleep(time.Duration(backoffMillis * int(time.Millisecond)))
		totalWaitMillis += backoffMillis
		attempts += 1

		if backoffMillis < MAX_BACKOFF_MILLIS {
			backoffMillis = min(backoffMillis*2, MAX_BACKOFF_MILLIS)
		}
	}

	return nil, time.Duration(totalWaitMillis * int(time.Millisecond)), err
}

func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
