package container

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"time"

	"github.com/grussorusso/serverledge/internal/executor"
	"github.com/grussorusso/serverledge/internal/function"
)

// NewContainer creates and starts a new container.
func NewContainer(image, base64Src string, opts *ContainerOptions, f *function.Function) (ContainerID, error) {
	cf := GetFactoryFromFunction(f)
	contID, err := cf.Create(image, opts)
	if err != nil {
		log.Printf("Failed container creation\n")
		return "", err
	}

	if len(base64Src) > 0 {
		var r io.Reader
		// Decoding src
		decodedSrc, _ := base64.StdEncoding.DecodeString(base64Src)
		// Check if decoded src is a url
		u, err := url.ParseRequestURI(string(decodedSrc))
		if err == nil && u.Scheme != "" && u.Host != "" {
			// src is url; it has to be downloaded
			resp, err := http.Get(string(decodedSrc))
			if err != nil {
				log.Printf("Failed to download code %s", decodedSrc)
				return "", err
			}
			r = resp.Body
		} else {
			// assuming decodedSrc is Base64 encoded tar
			r = bytes.NewReader(decodedSrc)
		}
		err = cf.CopyToContainer(contID, r, "/app/")
		if err != nil {
			log.Printf("Failed code copy\n")
			return "", err
		}
	}

	err = cf.Start(contID)
	if err != nil {
		return "", err
	}

	return contID, nil
}

func Execute(contID ContainerID, req *executor.InvocationRequest, f *function.Function) (*executor.InvocationResult, time.Duration, error) {
	if f.Runtime == WASI_RUNTIME {
		return wasiExecute(contID, req)
	} else {
		return dockerExecute(contID, req)
	}
}

func wasiExecute(contID ContainerID, req *executor.InvocationRequest) (*executor.InvocationResult, time.Duration, error) {
	wasiRunner := factories[WASI_FACTORY_KEY].(*WasiFactory).runners[contID]
	t0 := time.Now()

	if wasiRunner.wasiType == WASI_TYPE_MODULE {
		// Create an instance of the module
		instance, err := wasiRunner.linker.Instantiate(wasiRunner.store, wasiRunner.module)
		if err != nil {
			return nil, time.Now().Sub(t0), fmt.Errorf("Failed to instantiate WASI module: %v", err)
		}

		// Get the _start function (entrypoint of any wasm module)
		start := instance.GetFunc(wasiRunner.store, "_start")
		if start == nil {
			return nil, time.Now().Sub(t0), fmt.Errorf("WASI Module does not have a _start function")
		}

		// Call the _start function
		if _, err := start.Call(wasiRunner.store); err != nil {
			return nil, time.Now().Sub(t0), fmt.Errorf("Failed to run WASI module: %v", err)
		}

		// Read stdout from the temp file
		stdout, err := io.ReadAll(wasiRunner.stdout)
		if err != nil {
			return nil, time.Now().Sub(t0), fmt.Errorf("Failed to read stdout for WASI: %v", err)
		}

		// Read stderr from the temp file
		stderr, err := io.ReadAll(wasiRunner.stderr)
		if err != nil {
			return nil, time.Now().Sub(t0), fmt.Errorf("Failed to read stderr for WASI: %v", err)
		}

		// Populate result
		res := &executor.InvocationResult{Success: true, Result: string(stdout)}
		if req.ReturnOutput {
			res.Output = fmt.Sprintf("%s\n%s", string(stdout), string(stderr))
		}
		return res, time.Now().Sub(t0), nil
	} else if wasiRunner.wasiType == WASI_TYPE_COMPONENT {
		// Create wasmtime CLI command
		execCmd := exec.Command("wasmtime", wasiRunner.cliArgs...)

		// Save stdout and stderr to another buffer
		var stdoutBuffer, stderrBuffer bytes.Buffer
		execCmd.Stdout = &stdoutBuffer
		execCmd.Stderr = &stderrBuffer
		// Execute wasmtime CLI
		err := execCmd.Run()
		if err != nil {
			log.Printf("wasmtime failed with %v\n", err)
		}

		// Read stdout from temporary buffer
		stdout, err := io.ReadAll(&stdoutBuffer)
		if err != nil {
			log.Printf("Failed to read stdout: %v", err)
		}

		// Read stderr from temporary buffer
		stderr, err := io.ReadAll(&stderrBuffer)
		if err != nil {
			log.Printf("Failed to read stderr: %v", err)
		}

		// Create response
		resp := &executor.InvocationResult{Success: err == nil, Result: string(stdout)}
		if req.ReturnOutput {
			resp.Output = fmt.Sprintf("%s\n%s", string(stdout), string(stderr))
		}
		return resp, time.Now().Sub(t0), nil
	} else {
		return nil, 0, fmt.Errorf("Unrecognized WASI Type")
	}
}

// Execute interacts with the Executor running in the container to invoke the
// function through a HTTP request.
func dockerExecute(contID ContainerID, req *executor.InvocationRequest) (*executor.InvocationResult, time.Duration, error) {
	ipAddr, err := factories[DOCKER_FACTORY_KEY].GetIPAddress(contID)
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

func GetMemoryMB(id ContainerID, f *function.Function) (int64, error) {
	return GetFactoryFromFunction(f).GetMemoryMB(id)
}

func Destroy(id ContainerID, f *function.Function) error {
	return GetFactoryFromFunction(f).Destroy(id)
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
