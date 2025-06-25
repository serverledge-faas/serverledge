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
)

type RetrievedMetrics struct {
	Completions      map[string]float64
	AvgExecutionTime map[string]float64
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
