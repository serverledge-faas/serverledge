package metrics

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/common/model"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"

	promapi "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

var retrievedMetrics RetrievedMetrics

type metricSample struct {
	Value  float64
	Labels map[string]string
}
type metricProcessor[T any] func(samples []metricSample) (T, error)

func executeQuery(query string, api v1.API, ctx context.Context) (model.Vector, error) {
	result, warnings, err := api.Query(ctx, query, time.Now())
	if err != nil {
		return nil, fmt.Errorf("failed query: %v", err)
	}

	if len(warnings) > 0 {
		log.Printf("received warnings in the execution: %v", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("could not convert the result of the query: %v", result)
	}

	return vector, nil
}

func extractSampleWithLabels(sample *model.Sample, requiredLabels []string) (*metricSample, error) {
	labels := make(map[string]string)

	for _, labelName := range requiredLabels {
		labelValue, found := sample.Metric[model.LabelName(labelName)]
		if !found {
			return nil, fmt.Errorf("could not find the %s label in the result: %v", labelName, sample)
		}
		labels[labelName] = string(labelValue)
	}

	return &metricSample{
		Value:  float64(sample.Value),
		Labels: labels,
	}, nil
}

func retrieveMetrics[T any](query string, api v1.API, ctx context.Context, requiredLabels []string, processor metricProcessor[T]) (T, error) {
	var zero T

	vector, err := executeQuery(query, api, ctx)
	if err != nil {
		return zero, err
	}

	var samples []metricSample
	for _, sample := range vector {
		extracted, err := extractSampleWithLabels(sample, requiredLabels)
		if err != nil {
			log.Printf("skipping sample: %v", err)
			continue
		}
		samples = append(samples, *extracted)
	}

	return processor(samples)
}

func retrieveSingleValue(query string, api v1.API, ctx context.Context) (float64, error) {
	return retrieveMetrics(query, api, ctx, []string{}, func(samples []metricSample) (float64, error) {
		if len(samples) != 1 {
			// This will cause the function to return zero value, but we should handle this better
			return 0.0, fmt.Errorf("Expected 1 result; found %d\n", len(samples))
		}
		return samples[0].Value, nil
	})
}

func retrieveByFunction(query string, api v1.API, ctx context.Context) (map[string]float64, error) {
	return retrieveMetrics(query, api, ctx, []string{"function"}, func(samples []metricSample) (map[string]float64, error) {
		result := make(map[string]float64)
		for _, sample := range samples {
			result[sample.Labels["function"]] = sample.Value
		}
		return result, nil
	})
}

func retrieveByFunctionAndNode(query string, api v1.API, ctx context.Context) (map[string]map[string]float64, error) {
	return retrieveMetrics(query, api, ctx, []string{"function", "node"}, func(samples []metricSample) (map[string]map[string]float64, error) {
		result := make(map[string]map[string]float64)
		for _, sample := range samples {
			nodeName := sample.Labels["node"]
			functionName := sample.Labels["function"]

			if _, exists := result[nodeName]; !exists {
				result[nodeName] = make(map[string]float64)
			}
			result[nodeName][functionName] = sample.Value
		}
		return result, nil
	})
}

func retrieveByTaskAndNextTask(query string, api v1.API, ctx context.Context) (map[string]map[string]float64, error) {
	return retrieveMetrics(query, api, ctx, []string{"task", "next_task"}, func(samples []metricSample) (map[string]map[string]float64, error) {
		values := make(map[string]map[string]float64)

		// Build the raw values map
		for _, sample := range samples {
			taskId := sample.Labels["task"]
			nextTaskId := sample.Labels["next_task"]

			if _, exists := values[taskId]; !exists {
				values[taskId] = make(map[string]float64)
			}
			values[taskId][nextTaskId] = sample.Value
		}

		// Normalize to probabilities
		for taskId, innerMap := range values {
			sum := 0.0
			for _, value := range innerMap {
				sum += value
			}

			if sum > 0 {
				for nextTaskId, value := range innerMap {
					values[taskId][nextTaskId] = value / sum
				}
			}
		}

		return values, nil
	})
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

	ticker := time.NewTicker(time.Duration(config.GetInt(config.METRICS_RETRIEVER_INTERVAL, 5)) * time.Second)
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

			query = fmt.Sprintf("%s_sum{}/%s_count{}", EXECUTION_TIME, EXECUTION_TIME)
			avgFunDurationAllNodes, err := retrieveByFunctionAndNode(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v\n", err)
			}
			retrievedMetrics.AvgExecutionTimeAllNodes = avgFunDurationAllNodes

			query = fmt.Sprintf("%s_sum{}/%s_count{}", OUTPUT_SIZE, OUTPUT_SIZE)
			avgOutputSize, err := retrieveByFunction(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByFunction: %v\n", err)
			}
			retrievedMetrics.AvgOutputSize = avgOutputSize

			query = fmt.Sprintf("%s{}", BRANCH_COUNT)
			frequencyPerTaskAndNextOne, err := retrieveByTaskAndNextTask(query, api, ctx)
			if err != nil {
				log.Printf("Error in retrieveByTaskAndNextTask: %v\n", err)
			}
			retrievedMetrics.BranchFrequency = frequencyPerTaskAndNextOne

			fmt.Println("All queries completed")
			fmt.Println(retrievedMetrics)

		}
	}

}

func GetMetrics() RetrievedMetrics {
	// TODO: deep copy?
	return retrievedMetrics
}
