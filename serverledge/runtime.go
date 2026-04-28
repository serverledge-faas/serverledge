package serverledge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

// Mutex to use if a request wants to capture output. Since it's shared amongst all the functions running in the container
// we have to use a mutex. If functions don't need the StdOut and StdErr to be captured, this will be ignored.
var outputMu sync.Mutex

// HandlerFunc is the signature of the handler that has to be provided to serverledge
type HandlerFunc func(params map[string]interface{}) (interface{}, error)

// executionResult is a data structure to capture execution output
type executionResult struct {
	Result interface{}
	Output string
	Err    error
}

// executeWithCapture is the wrapper used to run the function and capture its output.
func executeWithCapture(handler HandlerFunc, params map[string]interface{}) executionResult {
	// Lock beacuse stdout/stderr are shared amongst all the functions
	outputMu.Lock()
	defer outputMu.Unlock()

	// old values to be restored after execution
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	oldLog := log.Writer()

	// pipe used to read the stdout/err
	r, w, _ := os.Pipe()

	os.Stdout = w
	os.Stderr = w
	log.SetOutput(w)

	// channel to pass the output to the wrapper
	outC := make(chan string)

	// this goroutine will read from the pipe and wirte to the channel
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		outC <- buf.String()
	}()

	// running the user's handler now
	var res interface{}
	var err error

	// This function will close the pipe after we have executed the user's function
	func() {
		defer func() {
			_ = w.Close()
			os.Stdout = oldStdout
			os.Stderr = oldStderr
			log.SetOutput(oldLog)
		}()
		res, err = handler(params) // actual execution of the handler
	}()

	// wait until we read the output from the channel
	output := <-outC

	return executionResult{Result: res, Output: output, Err: err}
}

// Start is used to execute the given handler. Has to be used by the user of the library in their source code to launch
// their function (e.g.: serverledge.Start(myHandler))
func Start(handler HandlerFunc) {
	http.HandleFunc("/invoke", func(w http.ResponseWriter, r *http.Request) {
		// here we put the params obtained from the http request and the boolean to check if we need to also capture
		// the output of the function
		var req struct {
			Params       map[string]interface{} `json:"Params"`
			ReturnOutput bool                   `json:"ReturnOutput"`
		}

		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "Invalid Request", 500)
			return
		}

		var execRes executionResult

		// check if we need to wrap the execution to capture output, or if we can directly execute the handler
		if req.ReturnOutput {
			execRes = executeWithCapture(handler, req.Params)
		} else {
			res, err := handler(req.Params)
			execRes = executionResult{Result: res, Output: "", Err: err}
		}

		// Response to be sent outside the container back to the client
		resp := map[string]interface{}{
			"Success": true,
			"Result":  "",
			"Output":  execRes.Output,
		}

		if execRes.Err != nil { // failure
			resp["Success"] = false
			if resp["Output"].(string) != "" {
				resp["Output"] = resp["Output"].(string) + "\n" + execRes.Err.Error() // concatenate the error, no overwriting
			} else {
				resp["Output"] = execRes.Err.Error()
			}
		} else { // success
			resBytes, _ := json.Marshal(execRes.Result)
			resp["Result"] = string(resBytes)
		}

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(resp)
		if err != nil {
			log.Printf("Error encoding response: %v", err)
			return
		}
	})

	// start the http server inside the container

	port := 8080 // standard port for all containers
	log.Printf("Go Runtime listening on :%d", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}
