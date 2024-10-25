# Pulse Telemetry

<!-- ![GitHub Actions Status](https://github.com/battery-pulse/pulse-telemetry/actions/workflows/CI.yml/badge.svg?branch=main)
![Python Version](https://img.shields.io/badge/python-3.11-blue.svg) -->
<!-- ![Docker Image Version](https://img.shields.io/docker/v/ghcr.io/battery-pulse/pulse-telemetry?label=Docker%20Image&sort=semver) -->


Welcome to the Pulse Telemetry repository. This project contains Spark applications designed to transform raw telemetry data into a series of structured schemas for comprehensive analysis. The flexible architecture allows for easy extension—developers can deploy additional Spark applications to create new schemas or enhance existing ones to meet evolving analytical needs.


## Schema Overview

### Telemetry

Enhanced schema for individual records from the battery:
- **Identifiers**: device ID, test ID, step number, etc.
- **Instantaneous Quantities**: timestamp, voltage, power, etc., measured at each individual record.
- **Differential Quantities**: duration, differential capacity, etc., changes between consecutive records.
- **Accumulated Quantities**: step duration, step capacity charged, etc., total values accumulated over the step.
- **User-Defined Data**: auxiliary measurements (e.g., temperature) and metadata for additional context.

### Step Statistics

Aggregation of telemetry data at the charge/discharge step level:
- **Time**: start time, end time, and duration of the step.
- **Instantaneous Aggregations**: minimum, maximum, start, end, etc., during the step.
- **Step Accumulations**: total step capacity and energy.
- **Data Diagnostics**: maximum voltage delta, maximum duration, etc., for diagnosing data resolution.
- **User-Defined Data**: aggregated auxiliary measurements and metadata.

### Cycle Statistics

Aggregation of telemetry data at the cycle level:
- **Time**: start time, end time, and duration of the step.
- **Instantaneous Aggregations**: minimum, maximum, start, end, etc., during the cycle.
- **Cycle Accumulations**: total cycle capacity and energy.
- **Data Diagnostics**: maximum voltage delta, maximum duration, etc., for diagnosing data resolution.
- **User-Defined Data**: aggregated auxiliary measurements and metadata.


## Supported Test Vendors

Coming soon...


## Developer Notes

See the [developer guide](https://github.com/battery-pulse/developer-guide/blob/main/SETUP_GUIDE.md) for notes on configuring your development environment.

### Test Coverage

- **Data Generators**: We've implemented mock data sources that can asynchronously produce data, configurable to emit data at a desired frequency, such as a series of test or asset channels.

- **Unit Tests**: Validate the correctness of individual transformations, ensuring each transformation produces correct values and handles edge cases, such as null values or empty input data.

- **Integration Tests**: Verify the data source and sink connectors, ensuring that the application can successfully connect to, read from, and write to external systems. Rely on Hive and Minio running in a local kind cluster.

- **End-to-end Tests**: Confirm the behavior of applications within Kubernetes, ensuring that Spark applications are correctly deployed as custom resources. These tests check the end-to-end functionality, from deployment to data processing, ensuring that the applications behave as expected when deployed in the cluster.

### Partitions and Sort Order

- **Partitioning**: The default partitioning scheme in the application is tuned assuming that individual channels are generating data at ~1 Hz acquisition frequency. For datasets with higher or lower acquisition frequency, you may want to tune the time based partition in the `telemetry` table.

- **Sort Order**: Adding sort-ordering on cycle, step, and record number can speed up end-user queries. We have opted for global sort order within partitions, which has a tradeoff of inducing some write overhead. Between partitions on test-id and sort ordering, `telemetry` reads are highly optimized.

### UTC as the Standard Timezone

- **Spark Timezones**: Timestamps in Spark are timezone naive, meaning they do not store timezone info. To consistently manage the timezone that timestamps are created in within your Spark application, ensure that you set the timezone in the Spark session to UTC `builder.config("spark.sql.session.timeZone", "UTC")`.

- **Python-Spark Conversions**: When Python datetime objects are read into Spark, they will be adjusted to the session timezone. When the timestamp is read back into python, the time will be adjusted to the local timezone, but no timezone info will be attached. This is ambiguous, so it is recommended to convert these to UTC `ts.astimezone(datetime.UTC)`.

- **Additional Timestamp Sources**: Data entering the system from outside sources will be assumed to be in UTC time. Within this project, we have made every effort to work with UTC as the standard to facilitate logical reasoning about timed quantities.

### Reservation of Window Functions

- **Window Function Usage**: Window functions, such as `lead()` and `lag()`, significantly increase the size of state within a Spark application, which can lead to increased memory usage and complexity, especially in streaming contexts. To mitigate these issues, we have made a conscious decision to construct our schemas in a way that they are used in preprocessing steps only.

- **Trade-off and Data Quality**: We acknowledge that this design introduces a trade-off: errors due to late-arriving data during preprocessing will propagate down the pipeline. This may affect the accuracy of derived metrics if late data modifies the computed fields (e.g. `timeseries.duration__s`). However, the benefits of reduced complexity, improved performance, and lower memory usage outweigh the potential risks.
