# Pulse Application Core

This repository implements Spark applications for transforming raw incoming data into a set of schemas for analysis. You can extend these schemas by deploying additional Spark applications.

## Data Flow Overview

### Sourced from Postgres

Transactional metadata on the devices under test and the test sequences for each device.

- `device_metadata` - Stores static information about each device under test.
- `device_sequence` - Contains the test sequences for each device.

### Sourced from Kafka

Event and telemetry streams from Kafka are consumed and persisted.

- `event_log` - Logs discrete events like 'test started', 'test stopped', and other significant occurrences.
- `timeseries_raw` - Holds raw data from each test sequence, such as voltage, current, temperature, etc.

### Transformed Data

Batch processing jobs concatenate telemetry sequences and perform aggregations.

- `timeseries_aggregated` - Aggregates data from multiple sequences, providing a comprehensive history.
- `statistics_steps` - Aggregates data at the charge/discharge step level, providing statistics such as average voltage, maximum current, total energy, etc.
- `statistics_cycles` - Aggregates data over full cycles of charge and discharge, including summaries like total energy discharged, total cycle time, and health indicators.

## Persistance Options

### PostgreSQL

The same PostgreSQL schema that houses the device metadata can be used to persist the incoming and transformed data. Only reccomended for small deployments.

### Delta Lake

Object storage can be used for larger deployments. This is the cheapest option per GB for storage. The Hive metastore allows you to query this backend as a database using SQL.

## Spark Applications

### Streaming

Streaming applications ingest data from Kafka into persistant storage using Spark structured-streaming. There is a partition in Kafka by sequence id that the consumer takes advantage of.

### Batch

Batch applications implement incremental data processing. Any devices with test sequences that have been updated within the look-back window are processed by the Spark engine.

There is also a maintance job (for Delta storage only) for vacuum and compaction operations.

## Deployment

You can opt for leveraging a managed service (GCP Dataproc, AWS EMR, Databricks, etc.) for deploying the Spark applications or use the provided helm chart. The provided helm chart leverages the [Spark Operator](https://github.com/kubeflow/spark-operator).

### Helm Chart

This chart packages all of the Spark applications into one deployment. The streaming jobs are deployed as `SparkApplication` and batch jobs as `ScheduledSparkApplication`.

See the [chart documentation](LINKHERE) for all of the available configuration variables.

### Dependencies

#### Kafka Broker

You can deploy yourself using [Strimzi operator](https://github.com/strimzi/strimzi-kafka-operator) or use a managed service with compatible API (reccomended).

#### PostgreSQL

You can deploy yourself using [Postgres operator](https://github.com/zalando/postgres-operator) or use a managed service to handle backups (reccomended).

#### Object Storage

You can deploy yourself using [Minio operator](https://github.com/minio/operator) or use a managed service (reccomended).

*Only required for Delta Lake setup.*
