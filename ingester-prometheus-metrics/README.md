# Export Metrics

Monitoring metrics is essential for maintaining a healthy ingester deployment. It enables you to:

- Assess the current state of your ingester
- Proactively maintain your deployment
- Quickly diagnose and resolve issues when they arise

For a comprehensive list of available metrics and their descriptions, please refer to our [detailed metrics documentation](../docs/metrics-display.md#list-of-metrics-constantly-updated).

## Start Metrics Exporter

To start an HTTP server that serves Prometheus metrics, initialize the metrics exporter before starting the GreptimeDB ingester:

```java
    MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
    metricsExporter.init(ExporterOptions.newDefault());
    // Start GreptimeDB ingester
    // ...
```

## Get Metrics

You can check the output of `curl http://<host>:<port>/metrics` by getting the latest metrics of Ingester.

## Export metrics to Prometheus

Ingester supports exporting metrics to Prometheus. Before configuring export of metrics, you need to setup Prometheus by following their official [documentation](https://prometheus.io/docs/prometheus/latest/installation/).

To scrape metrics from Ingester, write a Prometheus configuration file and save it as `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'greptimedb ingester'
    static_configs:
      # Assuming that Ingester is running locally.
      # The default HTTP port of 8090.
      - targets: ['localhost:8090']
```

Start Prometheus using the configuration file:

```yaml
./prometheus --config.file=prometheus.yml
```

## Grafana Dashboard

You can import dashboard via JSON model: [`greptimedb-ingester-dashboard.json`](../grafana/greptimedb-ingester-dashboard.json)
