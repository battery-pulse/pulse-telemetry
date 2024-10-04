# Pulse Telemetry

Welcome to the Pulse Telemetry repository. This project contains Spark applications designed to transform raw telemetry data into a series of structured schemas for comprehensive analysis. The flexible architecture allows for easy extension—developers can deploy additional Spark applications to create new schemas or enhance existing ones to meet evolving analytical needs.

## Schema Overview

- `timeseries` - Common schema for individual telemetry records from the battery.
- `statistics_steps` - Aggregates data at the charge/discharge step level, providing statistics such as average voltage, maximum current, total energy, and average temperature.
- `statistics_cycles` - Aggregates data over full cycles of charge and discharge, including summaries like total energy discharged, total cycle time, and health indicators.

## Developer Notes

### Reservation of Window Functions to Preprocessing

- **Window Function Usage**: Window functions, such as `lead()` and `lag()`, significantly increase the size of state within a Spark application, which can lead to increased memory usage and complexity, especially in streaming contexts. To mitigate these issues, we have made a conscious decision to limit the use of window functions to preprocessing steps only, which are closer to the source systems.

- **Transformed Schema Design**: In our design, the transformed schemas are constructed in such a way that all necessary calculations involving window functions are performed during the preprocessing stage, ensuring that downstream transformations do not need to re-evaluate window-based metrics like `timeseries.duration__s`. By doing so, we eliminate the need for managing large states during subsequent transformations, leading to simpler and more efficient Spark jobs.

- **Trade-off and Data Quality**: We acknowledge that this design introduces a trade-off: errors due to late-arriving data during preprocessing will propagate down the pipeline. This may affect the accuracy of derived metrics if late data modifies the computed `timeseries.duration__s`. However, we believe that the benefits of reduced complexity, improved performance, and lower memory usage outweigh the potential risks.

- **Error Management and Monitoring**: To minimize the impact of late data, we are implementing both streaming and batch jobs as part of a lambda architecture. This approach offers the best of both worlds between timeliness and correctness of the data. Users are encouraged to deploy only the batch jobs, unless near-real time analytics are required.

### Unit and Integration Testing Approach

The testing approach is designed to ensure the reliability and robustness of the Pulse Telemetry application:

- **Data Generators**:
  - Implement mock data sources that can asynconously produce data.
  - Used in unit, integration, and system tests; they can replicate multiple channels publishing data in parallel.

- **Unit Tests**: 
  - Focus on verifying the correctness of individual transformations. 
  - These tests ensure that each transformation works as expected in isolation.

- **Integration Tests**: 
  - Validate the proper functioning of data source and sink connectors.
  - These tests ensure that the application can correctly connect to, read from, and write to external systems.

- **System Tests**: 
  - Test the complete applications against realistic data sources and sinks.
  - These tests provide an end-to-end validation of the system in a near-production environment to ensure that all components work together seamlessly under realistic conditions.
