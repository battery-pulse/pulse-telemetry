# Pulse Telemetry

Welcome to the Pulse Telemetry repository. This project contains Spark applications designed to transform raw telemetry data into a series of structured schemas for comprehensive analysis. The flexible architecture allows for easy extension—developers can deploy additional Spark applications to create new schemas or enhance existing ones to meet evolving analytical needs.

## Schema Overview

### Telemetry

Enhanced schema for individual records from the battery. Encompasses:
- **Identifiers**: device ID, test ID, step number, etc.
- **Instantaneous Quantities**: timestamp, voltage, power, etc., measured at each individual record.
- **Differential Quantities**: duration, voltage delta, differential capacity, etc., changes between consecutive records.
- **Accumulated Quantities**: step duration, step capacity charged, etc., total values accumulated over the step.
- **User-Defined Data**: auxiliary measurements (e.g., temperature) and metadata for additional context.

### Step Statistics

Aggregation of telemetry data at the charge/discharge step level. Encompasses:
- **Time**: start time, end time, and duration of the step.
- **Instantaneous Aggregations**: minimum, maximum, start, end, etc., during the step.
- **Step Accumulations**: total step capacity and energy.
- **Data Diagnostics**: maximum voltage delta, maximum duration, etc., for diagnosing data resolution.
- **User-Defined Data**: aggregated auxiliary measurements and metadata.

### Cycle Statistics

Aggregation of telemetry data at the cycle level. Encompasses:
- **Time**: start time, end time, and duration of the step.
- **Instantaneous Aggregations**: minimum, maximum, start, end, etc., during the cycle.
- **Step Accumulations**: total step capacity and energy.
- **Data Diagnostics**: maximum voltage delta, maximum duration, etc., for diagnosing data resolution.
- **User-Defined Data**: aggregated auxiliary measurements and metadata.


## Developer Notes

### Reservation of Window Functions to Preprocessing

- **Window Function Usage**: Window functions, such as `lead()` and `lag()`, significantly increase the size of state within a Spark application, which can lead to increased memory usage and complexity, especially in streaming contexts. To mitigate these issues, we have made a conscious decision to limit the use of window functions to preprocessing steps only, which are closer to the source systems.

- **Trade-off and Data Quality**: We acknowledge that this design introduces a trade-off: errors due to late-arriving data during preprocessing will propagate down the pipeline. This may affect the accuracy of derived metrics if late data modifies the computed fields (e.g. `timeseries.duration__s`). However, we believe that the benefits of reduced complexity, improved performance, and lower memory usage outweigh the potential risks.

### Unit and Integration Testing Approach

The testing approach is designed to ensure the reliability and robustness of the Pulse Telemetry application:

- **Data Generators**:
  - Implement mock data sources that can asynconously produce data.
  - Configurable to emit data at a desired frequency, like a series of test channels.

- **Unit Tests**: 
  - Focus on verifying the correctness of individual transformations. 
  - These tests ensure that each transformation: 
    - Gives correct, expected values over test data.
    - Handles nullability constraints and semantics correctly.
    - Operate correctly when presented empty input data.

- **Integration Tests**: 
  - Validate the proper functioning of data source and sink connectors.
  - These tests ensure that the application can connect to, read from, and write to external systems.
