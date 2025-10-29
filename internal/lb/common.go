package lb

import (
	"encoding/json"
	"errors"
	"github.com/serverledge-faas/serverledge/internal/registration"
	"io"
	"log"
	"net/http"
	"net/url"
)

func getTargetStatus(targetUrl string) (registration.StatusInformation, error) {
	path, _ := url.JoinPath(targetUrl, "/status")
	resp, err := http.Get(path)
	if err != nil {
		log.Fatalf("Invocation to get status failed: %v", err)
		return registration.StatusInformation{}, err
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
		return registration.StatusInformation{}, err
	}

	// Check the status code
	if resp.StatusCode == http.StatusOK {
		var statusInfo registration.StatusInformation
		if err := json.Unmarshal(body, &statusInfo); err != nil {
			log.Fatalf("Error decoding JSON: %v", err)
			return registration.StatusInformation{}, errors.New("could not get status information")
		}
		return statusInfo, nil
	}

	return registration.StatusInformation{}, errors.New("could not get status information")
}
