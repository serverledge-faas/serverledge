package main

import (
	"log"
	"math"
	"strconv"

	// Import the Serverledge Go SDK
	"github.com/serverledge-faas/serverledge/serverledge"
)

// Response defines the structure of the JSON output. This is just an example. It can be defined by the user as they wish.
type Response struct {
	Message string `json:"message"`
	Number  int    `json:"number,omitempty"`
	IsPrime bool   `json:"is_prime"`
}

// isPrime is a helper function for this example. It can be any user-defined function for their logic.
func isPrime(n int) bool {
	if n <= 1 {
		return false
	}
	for i := 2; i <= int(math.Sqrt(float64(n))); i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

// myHandler is the main entry point for your function logic. This is also defined by the user, but it has to use this
// signature to implement the interface of the Serverledge library.
// Arguments:
//   - params: A map[string]interface{} containing the input parameters provided by the caller.
//
// Returns:
//   - interface{}: The result payload (struct, map, string, etc.) to be returned as JSON.
//   - error: An error object if the execution failed.
func myHandler(params map[string]interface{}) (interface{}, error) {

	name := "World"
	if n, ok := params["name"].(string); ok {
		name = n
	}

	// If the function is invoked with logging enabled, (e.g.: serveledge-cli with "-o" flag or ReturnOutput=true in the request)
	// these lines will be captured and returned in the "Output" field of the response.
	log.Printf("Processing request for user: %s...", name)

	resp := Response{Message: "Hello " + name}

	if numStr, ok := params["num"].(string); ok {
		if num, err := strconv.Atoi(numStr); err == nil {
			resp.Number = num
			resp.IsPrime = isPrime(num)
			log.Printf("Checked number: %d (Prime: %v)", num, resp.IsPrime)
		}
	}

	return resp, nil
}

func main() {
	// Start the Serverledge runtime. This is a blocking function since it will start an HTTP server inside the
	// container, waiting for the signal to execute the function.
	serverledge.Start(myHandler)
}
