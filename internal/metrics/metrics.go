package metrics

import (
	"log"

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
var durationBuckets = []float64{0.002, 0.005, 0.010, 0.02, 0.03, 0.05, 0.1, 0.15, 0.3, 0.6, 1.0}

const (
	COMPLETIONS    = "completed_total"
	EXECUTION_TIME = "execution_time"
	OUTPUT_SIZE    = "output_size"
	BRANCH_COUNT   = "branch_count"
)

var (
	metricCompletions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: COMPLETIONS,
		Help: "Number of completed function invocations",
	}, []string{"node", "function"})
	metricExecutionTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    EXECUTION_TIME,
		Help:    "Function duration",
		Buckets: durationBuckets,
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
	Completions              map[string]float64
	AvgExecutionTime         map[string]float64
	AvgExecutionTimeAllNodes map[string]map[string]float64
	AvgOutputSize            map[string]float64
	BranchFrequency          map[string]map[string]float64
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
	registry.MustRegister(metricExecutionTime)
	registry.MustRegister(metricOutputSize)
	registry.MustRegister(metricBranchCount)

	ScrapingHandler = promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true})

	go MetricsRetriever()
}

func AddCompletedInvocation(funcName string) {
	metricCompletions.With(prometheus.Labels{"function": funcName, "node": node.NodeIdentifier}).Inc()
}
func AddFunctionDurationValue(funcName string, duration float64) {
	metricExecutionTime.With(prometheus.Labels{"function": funcName, "node": node.NodeIdentifier}).Observe(duration)
}
func AddFunctionOutputSizeValue(funcName string, size float64) {
	metricOutputSize.With(prometheus.Labels{"function": funcName}).Observe(size)
}
func AddBranchCount(taskId string, nextTaskId string) {
	metricBranchCount.With(prometheus.Labels{"task": taskId, "next_task": nextTaskId}).Inc()
}
