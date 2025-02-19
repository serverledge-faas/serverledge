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
var nodeIdentifier string

func Init() {
	if config.GetBool(config.METRICS_ENABLED, false) {
		log.Println("Metrics enabled.")
		Enabled = true
	} else {
		Enabled = false
		return
	}

	nodeIdentifier = node.NodeIdentifier
	registerGlobalMetrics()

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true})
	http.Handle("/metrics", handler)
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		log.Printf("Listen and serve terminated with error: %s\n", err)
		return
	}
}

// Global metrics
var (
	CompletedInvocations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sedge_completed_total",
		Help: "The total number of completed function invocations",
	}, []string{"node", "function"})
	ExecutionTimes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sedge_exectime",
		Help:    "Function duration",
		Buckets: durationBuckets,
	},
		[]string{"node", "function"})
)

var durationBuckets = []float64{0.002, 0.005, 0.010, 0.02, 0.03, 0.05, 0.1, 0.15, 0.3, 0.6, 1.0}

func AddCompletedInvocation(funcName string) {
	CompletedInvocations.With(prometheus.Labels{"function": funcName, "node": nodeIdentifier}).Inc()
}
func AddFunctionDurationValue(funcName string, duration float64) {
	ExecutionTimes.With(prometheus.Labels{"function": funcName, "node": nodeIdentifier}).Observe(duration)
}

func registerGlobalMetrics() {
	registry.MustRegister(CompletedInvocations)
	registry.MustRegister(ExecutionTimes)
}
