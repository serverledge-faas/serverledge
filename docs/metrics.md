# Metrics

A simple metrics system is available to export/retrieve metrics to/from Prometheus.

The metrics system must be enabled via `metrics.enabled`.
If enabled, metrics can be scraped by any Prometheus server through the `/metrics` 
API of every Serverledge node.

If you have a local Serverledge node, you can check that the metrics system is
working without starting a Prometheus server:

	$ curl 127.0.0.1:1323/metrics 

If you start a local Prometheus server, you can browse `http://127.0.0.1:9090`.

## Available metrics

A few metrics are currently updated (see `internal/metrics/metrics.go`).

## Configuration

Relevant configuration options:

- `metrics.prometheus.host`: Prometheus server IP/hostname (for queries)
- `metrics.prometheus.port`: Prometheus server port (for queries)
- `metrics.retriever.interval`: Interval (in seconds) for metrics retrieval from Prometheus
