# Export Metrics

Metrics monitoring is a critical component for ensuring optimal ingester performance and reliability. It provides:

- Real-time visibility into ingester health and performance
- Proactive monitoring capabilities for maintaining system stability
- Rapid troubleshooting and issue resolution

For detailed information about available metrics, including descriptions and usage guidelines, see our [comprehensive metrics documentation](../docs/metrics-display.md#list-of-metrics-constantly-updated).

## Start Metrics Exporter

To start an HTTP server that serves Prometheus metrics, initialize the metrics exporter before starting the GreptimeDB ingester:

```java
    MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
    metricsExporter.init(ExporterOptions.newDefault());
    // Start GreptimeDB ingester
    // ...
```

## Get Metrics

You can check the output of `curl http://<host>:<port>/metrics` by getting the latest metrics of the ingester.

## Export Metrics to Prometheus

The ingester supports exporting metrics to Prometheus. Before configuring export of metrics, you need to setup Prometheus by following their official [documentation](https://prometheus.io/docs/prometheus/latest/installation/).

To scrape metrics from the ingester, write a Prometheus configuration file and save it as `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'greptimedb-ingester'
    static_configs:
      # Assuming that the ingester is running locally.
      # The default HTTP port of 8090.
      - targets: ['localhost:8090']
```

Start Prometheus using the configuration file:

```bash
./prometheus --config.file=prometheus.yml
```

## Grafana Dashboard

You can import dashboard via JSON model: [`greptimedb-ingester-dashboard.json`](../grafana/greptimedb-ingester-dashboard.json)
