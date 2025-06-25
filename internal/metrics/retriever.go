package metrics

import (
	"context"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"
	"log"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

var retrievedMetrics RetrievedMetrics

func retrieveSingleValue(query string, api v1.API, ctx context.Context) (float64, error) {

	result, warnings, err := api.Query(ctx, query, time.Now())
	if err != nil {
		return 0.0, fmt.Errorf("Failed query : %v\n", err)
	}

	if len(warnings) > 0 {
		log.Printf("Received warnings in the execution of the : %v\n", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return 0.0, fmt.Errorf("Could not convert the result of the query : %v\n", result)
	}
	if vector.Len() != 1 {
		return 0.0, fmt.Errorf("Expected 1 result; found %d\n", vector.Len())
	}

	sample := vector[0]
	return float64(sample.Value), nil
}

func retrieveByFunction(query string, api v1.API, ctx context.Context) (map[string]float64, error) {

	result, warnings, err := api.Query(ctx, query, time.Now())
	if err != nil {
		return nil, fmt.Errorf("Failed query : %v\n", err)
	}

	if len(warnings) > 0 {
		log.Printf("Received warnings in the execution of the : %v\n", warnings)
	}

	functionValues := make(map[string]float64)
	if vector, ok := result.(model.Vector); ok {
		for _, sample := range vector {
			value := float64(sample.Value)
			functionName, found := sample.Metric[model.LabelName("function")]
			if !found {
				log.Printf("Could not find the function name in the result : %v\n", sample)
				continue
			} else {
				functionValues[string(functionName)] = value
			}
		}
	} else {
		return nil, fmt.Errorf("Unexpected Result %v\n", result)
	}

	return functionValues, nil
}

func MetricsRetriever() {
	prometheusHost := config.GetString(config.METRICS_PROMETHEUS_HOST, "127.0.0.1")
	prometheusPort := config.GetInt(config.METRICS_PROMETHEUS_PORT, 9090)
	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:%d", prometheusHost, prometheusPort),
	})
	if err != nil {
		log.Printf("Error in Prometheus client creation: %v\n", err)
		return
	}

	// API of Prometheus
	api := v1.NewAPI(client)
	ctx := context.Background()

	ticker := time.NewTicker(time.Duration(config.GetInt(config.METRICS_RETRIEVER_INTERVAL, 10)) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			query := fmt.Sprintf("%s{node=\"%s\"}", COMPLETIONS, node.NodeIdentifier)
			completionsPerFunction, err := retrieveByFunction(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v\n", err)
			}
			retrievedMetrics.Completions = completionsPerFunction

			query = fmt.Sprintf("%s_sum{node=\"%s\"}/%s_count{node=\"%s\"}",
				EXECUTION_TIME, node.NodeIdentifier, EXECUTION_TIME, node.NodeIdentifier)
			avgFunDuration, err := retrieveByFunction(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v\n", err)
			}
			retrievedMetrics.AvgExecutionTime = avgFunDuration

			fmt.Println("All queries completed")

		}
	}

}

func GetMetrics() RetrievedMetrics {
	// TODO: deep copy?
	return retrievedMetrics
}
