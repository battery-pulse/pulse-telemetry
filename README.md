# Pulse Application Core

This repository foundational Spark applications for transforming raw incoming data into a set of schemas for analysis. You can extend these schemas with additional Spark applications.

## Data Flow Overview

### Sourced from Postgres

Static metadata on the devices under test and the test sequences for each device.

- `device_metadata` - Stores static information about each device under test.
- `device_sequence` - Contains the test sequences for each device.

### Sourced from Kafka

Data is consumed from Kafka using structured streaming and persisted.

- `event_log` - Logs discrete events like 'test started', 'test stopped', and other significant occurrences. Unstructured data.
- `timeseries_raw` - Holds raw data collected from each test sequence, such as voltage, current, temperature, etc. Structured data.

### Transformed Data

Batch processing jobs concatenate multiple test sequences and perform aggregations.

- `timeseries_aggregated` - Aggregated data from multiple sequences, providing a comprehensive history of the device.
- `statistics_steps` - Aggregates data at the charge/discharge step level, providing statistics such as average voltage, maximum current, total energy, etc.
- `statistics_cycles` - Aggregates data over full cycles of charge and discharge, including summaries like cycle life, average discharge capacity, and health indicators.

## Persistance Options

### PostgreSQL

The same PostgreSQL schema that houses the device metadata can be used to persist the data from Kafka and the transformed data. Only reccomended for small deployments.

### Delta Lake

S3 compatible storage can be used for larger deployments. This is the cheapest option per GB for storage. The Hive metastore allows you to query this backend as a database using SQL.

## Spark Applications

### Streaming

Streaming applications ingest data from Kafka into the storage layer. Streaming is implemented using the Spark structured-streaming dataframe API. There is a partition in Kafka by sequence id that the Spark consumer can take advantage of.

### Batch

Batch job applications first load the metadata in postgres and the last updated timestamps for each device sequence. Any devices with sequences that have been updated within the look-back window are processed by the Spark engine.

There is also a maintance job (for Delta storage only) for vacuum and compaction operations.

## Deployment & Monitoring

You can opt for leveraging a managed service (GCP Dataproc, Databricks, etc.) for deploying the Spark applications or use the provided helm chart. The provided helm chart leverages the [Spark Operator](https://github.com/kubeflow/spark-operator).

### Spark Helm Chart

The streaming jobs are deployed as `SparkApplication` and batch jobs as `ScheduledSparkApplication`. See the [documentation](LINKHERE) for available chart variables.

### Dependencies

#### Kafka Broker

You can deploy yourself using Strimzi or use a managed service with compatible API.

#### PostgreSQL

Reccomended to use managed service to handle scaling and backups.

#### Object Storage

Only if opting for data lake setup using Delta format (reccomended).
