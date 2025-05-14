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

	"github.com/serverledge-faas/serverledge/internal/executor"
)

// NewContainer creates and starts a new container.
func NewContainer(image, codeTar string, opts *ContainerOptions) (*Container, error) {
	contID, err := cf.Create(image, opts) // cf = container factory
	if err != nil {
		log.Printf("Failed container creation\n")
		return nil, err
	}

	if len(codeTar) > 0 {
		decodedCode, errDecode := base64.StdEncoding.DecodeString(codeTar)
		if errDecode != nil {
			log.Printf("Failed code decode\n")
			return nil, errDecode
		}
		err = cf.CopyToContainer(contID, bytes.NewReader(decodedCode), "/app/")
		if err != nil {
			log.Printf("Failed code copy\n")
			return nil, err
		}
	}
	// Starts the container after copying the code in it
	err = cf.Start(contID)
	if err != nil {
		return nil, err
	}

	container := &Container{
		ID:            contID,
		RequestsCount: 0,
	}

	return container, nil
}

// Execute interacts with the Executor running in the container to invoke the
// function through a HTTP request.
func Execute(contID ContainerID, req *executor.InvocationRequest) (*executor.InvocationResult, time.Duration, error) {
	ipAddr, err := cf.GetIPAddress(contID)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to retrieve IP address for container: %v", err)
	}

	postBody, _ := json.Marshal(req)
	postBodyB := bytes.NewBuffer(postBody) // this buffer will be erased after send
	resp, waitDuration, err := sendPostRequestWithRetries(fmt.Sprintf("http://%s:%d/invoke", ipAddr,
		executor.DEFAULT_EXECUTOR_PORT), postBodyB)
	if err != nil {
		if resp != nil {
			buffer, err2 := io.ReadAll(resp.Body)
			if err2 != nil {
				return nil, waitDuration, fmt.Errorf("request to executor failed: %v - can't get response buffer: %v", err, err2)
			}
			return nil, waitDuration, fmt.Errorf("request to executor failed: %v - response: %s", err, buffer)
		}
		return nil, waitDuration, fmt.Errorf("request to executor failed: %v", err)
	}

	if resp.StatusCode != 200 {
		buffer, err2 := io.ReadAll(resp.Body)
		if err2 != nil {
			return nil, waitDuration, fmt.Errorf("function invocation  %v failed with status %s: - can't get response buffer %v", req, resp.Status, err2)
		}
		return nil, waitDuration, fmt.Errorf("function invocation %v failed with status %s: %s", req, resp.Status, buffer)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("Error while closing response body\n")
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

func GetLog(id ContainerID) (string, error) {
	return cf.GetLog(id)
}

func sendPostRequestWithRetries(url string, body *bytes.Buffer) (*http.Response, time.Duration, error) {
	const TIMEOUT_MILLIS = 30000
	const MAX_BACKOFF_MILLIS = 1000
	var backoffMillis = 50
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
			log.Printf("Warning: Retrying POST to executor (attempts: %d): %v\n", attempts, err)
		}

		time.Sleep(time.Duration(backoffMillis * int(time.Millisecond)))
		totalWaitMillis += backoffMillis
		attempts += 1

		if backoffMillis < MAX_BACKOFF_MILLIS {
			backoffMillis = minInt(backoffMillis*2, MAX_BACKOFF_MILLIS)
		}
	}

	return nil, time.Duration(totalWaitMillis * int(time.Millisecond)), err
}

func minInt(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
