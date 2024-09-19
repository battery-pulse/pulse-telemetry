# Pulse Telemetry

This repository implements Spark applications for transforming raw incoming data into a set of schemas for analysis. You can extend these schemas by deploying additional Spark applications.

## Transformed Data

- `timeseries` - Common schema for individual telemetry records from the battery.
- `statistics_steps` - Aggregates data at the charge/discharge step level, providing statistics such as average voltage, maximum current, total energy, and average temperature.
- `statistics_cycles` - Aggregates data over full cycles of charge and discharge, including summaries like total energy discharged, total cycle time, and health indicators.

## Testing

- `unit` - unit tests are for transformations against static data files.
- `integration` - integration tests are for data source and sink connectors.
- `system` - system tests are for applications against realistic data sources and sinks.

## Deployment

You can opt for leveraging a managed service (GCP Dataproc, AWS EMR, Databricks, etc.) for deploying the Spark applications or use the provided helm chart. The provided helm chart leverages the [Spark Operator](https://github.com/kubeflow/spark-operator).

### Helm Chart

This chart packages all of the Spark applications into one distribution.

See the [chart documentation](LINKHERE) for a list of the available configuration variables.

### Dependencies

#### Kafka Broker

You can deploy yourself using [Strimzi operator](https://github.com/strimzi/strimzi-kafka-operator) or use a managed service with compatible API (reccomended).

#### Object Storage

You can deploy yourself using [Minio operator](https://github.com/minio/operator) or use a managed service (reccomended).
