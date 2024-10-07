# Pulse Telemetry

Welcome to the Pulse Telemetry repository. This project contains Spark applications designed to transform raw telemetry data into a series of structured schemas for comprehensive analysis. The flexible architecture allows for easy extension—developers can deploy additional Spark applications to create new schemas or enhance existing ones to meet evolving analytical needs.

## Schema Overview

### Telemetry

Common schema for individual records from the battery. Encompasses:
- **Identifiers**: Device ID, Test ID, Step Number, etc.
- **Instantaneous Quantities**: Timestamp, Voltage, Power, etc., measured at each individual record.
- **Differential Quantities**: Duration, Voltage Delta, Differential Capacity, etc., representing changes between consecutive records.
- **Accumulated Quantities**: Step Duration, Step Capacity Charged, Step Energy Charged, etc., representing the total values accumulated over the step.
- **User-Defined Data**: Auxiliary measurements (e.g., temperature) and Metadata for additional context.

### Step Statistics

Aggregation of telemetry data at the charge/discharge step level. Encompasses:
- **Time**: Start Time, End Time, and Duration of the step.
- **Instantaneous Aggregations**: Minimum, Maximum, Start, End, etc., for Voltage, Current, and Power during the step.
- **Step Accumulations**: Total Step Capacity and Energy.
- **Data Diagnostics**: Maximum Voltage Delta, Maximum Current Delta, Maximum Duration, etc., for diagnosing data resolution.
- **User-Defined Data**: Aggregated Auxiliary Measurements and Metadata.

### Cycle Statistics

Aggregation of telemetry data at the cycle level. Encompasses:
- **Time**: Start Time, End Time, and Duration of the cycle.
- **Instantaneous Aggregations**: Minimum, Maximum, Start, End, etc., for Voltage, Current, and Power during the cycle.
- **Cycle Accumulations**: Total Capacity and Energy over the cycle.
- **Data Diagnostics**: Maximum Voltage Delta, Maximum Current Delta, Maximum Duration, etc., for diagnosing data resolution.
- **User-Defined Data**: Aggregated Auxiliary Measurements and Metadata.


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
