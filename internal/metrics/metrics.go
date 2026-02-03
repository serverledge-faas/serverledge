package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/push"
	"log"
	"time"

	"net/http"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var Enabled bool
var registry = prometheus.NewRegistry()
var ScrapingHandler http.Handler = nil
var durationBuckets = prometheus.ExponentialBuckets(0.01, 2, 15)

const (
	COMPLETIONS         = "completed_count"
	COLD_STARTS         = "cold_starts_count"
	EXECUTION_TIME      = "execution_time"
	INITIALIZATION_TIME = "init_time"
	OUTPUT_SIZE         = "output_size"
	BRANCH_COUNT        = "branch_count"
)

var (
	metricCompletions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: COMPLETIONS,
		Help: "Number of completed function invocations per area",
	}, []string{"area", "function"})
	metricColdStarts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: COLD_STARTS,
		Help: "Number of cold starts per function and area",
	}, []string{"area", "function"})
	metricExecutionTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    EXECUTION_TIME,
		Help:    "Function duration",
		Buckets: durationBuckets,
	}, []string{"node", "function"})
	metricInitializationTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: INITIALIZATION_TIME,
		Help: "Function initialization time (cold start duration)",
	}, []string{"node", "function"})
	metricOutputSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: OUTPUT_SIZE,
		Help: "Function output size",
	}, []string{"function"})
	metricBranchCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: BRANCH_COUNT,
		Help: "Number of executions of a task among multiple alternatives",
	}, []string{"task", "next_task"})
)

type RetrievedMetrics struct {
	RemoteColdStartProbability map[string]float64
	AvgRemoteExecutionTime     map[string]float64
	AvgEdgeExecutionTime       map[string]map[string]float64
	AvgRemoteInitTime          map[string]float64
	AvgEdgeInitTime            map[string]map[string]float64
	AvgOutputSize              map[string]float64
	BranchFrequency            map[string]map[string]float64
}

func (r RetrievedMetrics) String() string {
	s := ""
	s += "REMOTE COLD START PROB:\n"
	s += fmt.Sprintf("  %v\n\n", r.RemoteColdStartProbability)
	s += "REMOTE EXEC TIMES:\n"
	s += fmt.Sprintf("  %v\n\n", r.AvgRemoteExecutionTime)
	s += "EDGE EXEC TIMES:\n"
	s += fmt.Sprintf("  %v\n\n", r.AvgEdgeExecutionTime)
	s += "REMOTE INIT TIMES:\n"
	s += fmt.Sprintf("  %v\n\n", r.AvgRemoteInitTime)
	s += "EDGE INIT TIMES:\n"
	s += fmt.Sprintf("  %v\n\n", r.AvgEdgeInitTime)
	s += "OUTPUT SIZE:\n"
	s += fmt.Sprintf("  %v\n\n", r.AvgOutputSize)
	s += "BRANCH FREQ:\n"
	s += fmt.Sprintf("  %v\n\n", r.BranchFrequency)

	return s
}

func Init() {
	if config.GetBool(config.METRICS_ENABLED, false) {
		log.Println("Metrics enabled.")
		Enabled = true
	} else {
		Enabled = false
		return
	}

	registry.MustRegister(metricCompletions)
	registry.MustRegister(metricColdStarts)
	registry.MustRegister(metricExecutionTime)
	registry.MustRegister(metricInitializationTime)
	registry.MustRegister(metricOutputSize)
	registry.MustRegister(metricBranchCount)

	ScrapingHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true})

	pushgatewayHost := config.GetString(config.METRICS_PROMETHEUS_PUSHGATEWAY_HOST, "")
	if pushgatewayHost != "" {

		pushgatewayPort := config.GetInt(config.METRICS_PROMETHEUS_PUSHGATEWAY_PORT, 9091)
		hostport := fmt.Sprintf("http://%s:%d", pushgatewayHost, pushgatewayPort)

		log.Println("Using Prometheus Pushgateway at:", hostport)

		go func(pushgatewayUrl string) {
			ticker := time.NewTicker(30 * time.Second)

			for {
				select {
				case <-ticker.C:
					// Push the entire registry in one go
					err := push.New(pushgatewayUrl, "serverledge").Gatherer(registry).Push()
					if err != nil {
						log.Printf("Could not push metrics: %v", err)
					}
				}
			}
		}(hostport)

	}

	go MetricsRetriever()
}

func AddCompletedInvocation(funcName string, coldStart bool) {
	metricCompletions.With(prometheus.Labels{"function": funcName, "area": node.LocalNode.Area}).Inc()
	if coldStart {
		metricColdStarts.With(prometheus.Labels{"function": funcName, "area": node.LocalNode.Area}).Inc()
	}
}
func AddFunctionDurationValue(funcName string, duration float64) {
	metricExecutionTime.With(prometheus.Labels{"function": funcName, "node": node.LocalNode.String()}).Observe(duration)
}
func AddFunctionInitTimeValue(funcName string, initTime float64) {
	metricInitializationTime.With(prometheus.Labels{"function": funcName, "node": node.LocalNode.String()}).Observe(initTime)
}
func AddFunctionOutputSizeValue(funcName string, size float64) {
	metricOutputSize.With(prometheus.Labels{"function": funcName}).Observe(size)
}
func AddBranchCount(taskId string, nextTaskId string) {
	metricBranchCount.With(prometheus.Labels{"task": taskId, "next_task": nextTaskId}).Inc()
}
