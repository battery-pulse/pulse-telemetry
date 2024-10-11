# Pulse Telemetry

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


## Developer Notes

### UTC as the Standard Timezone

- **Spark Timezones**: Timestamps in Spark are timezone naive, meaning they do not store timezone info. To consistently manage the timezone that timestamps are created in within your Spark application, ensure that you set the timezone in the Spark session to UTC `builder.config("spark.sql.session.timeZone", "UTC")`.

- **Python-Spark Conversions**: When Python datetime objects with timezones are read into Spark, they will be converted to the session timezone. When the timestamp is read back into python, the time will be adjusted to the local timezone, but no timezone info will be attached. It is recommended to convert these to UTC `ts.astimezone(datetime.UTC)`.

- **Additional Timestamp Sources**: Data entering the system from outside sources will be assumed to be in UTC time. Within this project, we have made every effort to work with UTC as the standard to facilitate logical reasoning about timed quantities.

### Reservation of Window Functions

- **Window Function Usage**: Window functions, such as `lead()` and `lag()`, significantly increase the size of state within a Spark application, which can lead to increased memory usage and complexity, especially in streaming contexts. To mitigate these issues, we have made a conscious decision to limit the use of window functions to preprocessing steps only, which are closer to the source systems.

- **Trade-off and Data Quality**: We acknowledge that this design introduces a trade-off: errors due to late-arriving data during preprocessing will propagate down the pipeline. This may affect the accuracy of derived metrics if late data modifies the computed fields (e.g. `timeseries.duration__s`). However, the benefits of reduced complexity, improved performance, and lower memory usage outweigh the potential risks.

### Unit and Integration Testing

#### Data Generators
  - Implement mock data sources that can asynconously produce data.
  - Configurable to emit data at a desired frequency, like a series of test channels.

#### Unit Tests 
- Focus on verifying the correctness of individual transformations. 
- These tests ensure that each transformation gives correct values over test data, handles nullability constraints and semantics correctly, operates correctly when presented with empty input data.

#### Integration Tests
- Validate the proper functioning of data source and sink connectors.
- These tests ensure that the application can connect to, read from, and write to external systems.
