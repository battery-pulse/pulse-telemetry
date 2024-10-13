import pulse_telemetry.sparklib.transformations.statistics_cycle as statistics_cycle
import pulse_telemetry.sparklib.transformations.statistics_step as statistics_step
import pulse_telemetry.sparklib.transformations.telemetry as telemetry

import pulse_telemetry.sparklib.connectors.iceberg as iceberg

import pyspark.errors.exceptions.captured as cap
cap.UnsupportedOperationException


def test_create_tables(spark_session, telemetry_df, statistics_step_df, statistics_cycle_df):
    
    # Create from scratch
    created = iceberg.create_table_if_not_exists(
        spark=spark_session,
        database_name="integration_testing",
        table_name="telemetry",
        table_comment=telemetry.telemetry_schema_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=["device_id", "test_id", "year(timestamp)"]
    )
    assert created is True

    # Idempotence (should not create)
    created = iceberg.create_table_if_not_exists(
        spark=spark_session,
        database_name="integration_testing",
        table_name="telemetry",
        table_comment=telemetry.telemetry_schema_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=["device_id", "test_id", "year(timestamp)"]
    )
    assert created is False
