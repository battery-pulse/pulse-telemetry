from pulse_telemetry.apps import telemetry_statistics
from pulse_telemetry.sparklib import iceberg, telemetry


def test_main(spark_session, telemetry_df, statistics_step_df, statistics_cycle_df):
    # Run on database that does not exist yet
    telemetry_statistics.main(
        spark=spark_session,
        catalog="lakehouse",
        database="telemetry_statistics",
        watermark_buffer_minutes=60,
        partition_cutoff_days=30,
    )
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="telemetry_statistics", table_name="telemetry"
    )
    assert records.count() == 0, "Expecting empty records in telemetry."
    records = iceberg.read_table(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="telemetry_statistics",
        table_name="statistics_step",
    )
    assert records.count() == 0, "Expecting empty records in step statistics."
    records = iceberg.read_table(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="telemetry_statistics",
        table_name="statistics_cycle",
    )
    assert records.count() == 0, "Expecting empty records in cycle statistics."

    # Insert the telemetry records and run again
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="telemetry_statistics",
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
    telemetry_statistics.main(
        spark=spark_session,
        catalog="lakehouse",
        database="telemetry_statistics",
        watermark_buffer_minutes=60,
        partition_cutoff_days=30,
    )
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="telemetry_statistics", table_name="telemetry"
    )
    assert records.count() == telemetry_df.count(), "Expecting records in telemetry."
    records = iceberg.read_table(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="telemetry_statistics",
        table_name="statistics_step",
    )
    assert records.count() == statistics_step_df.count(), "Expecting records in step statistics."
    records = iceberg.read_table(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="telemetry_statistics",
        table_name="statistics_cycle",
    )
    assert records.count() == statistics_cycle_df.count(), "Expecting records in cycle statistics."
