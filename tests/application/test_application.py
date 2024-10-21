from pulse_telemetry.sparklib import iceberg, telemetry

from .conftest import launch_spark_application


def test_telemetry_statistics(
    kubernetes_services, spark_session, telemetry_df, statistics_step_df, statistics_cycle_df
):
    # Running the app should create tables
    launch_spark_application(
        application_name="telemetry-statistics", manifest_file_name="telemetry-statistics.yaml", timeout_seconds="240"
    )
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert records.count() == 0, "Running the app should create tables"
    # Running the app with data should populate tables
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
    launch_spark_application(
        application_name="telemetry-statistics", manifest_file_name="telemetry-statistics.yaml", timeout_seconds="240"
    )
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert records.count() == telemetry_df.count(), "Running the app with data should populate tables - telemetry"
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_step"
    )
    assert (
        records.count() == statistics_step_df.count()
    ), "Running the app with data should populate tables - statistics_step"
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_cycle"
    )
    assert (
        records.count() == statistics_cycle_df.count()
    ), "Running the app with data should populate tables - statistics_cycle"


def test_table_maintenance():
    pass
