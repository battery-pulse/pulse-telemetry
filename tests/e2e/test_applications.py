import subprocess

from pulse_telemetry.sparklib import iceberg, telemetry

from .conftest import launch_spark_application


def test_applications(kubernetes_services, spark_session, telemetry_df, statistics_step_df, statistics_cycle_df):
    # Table maintenance should fail if no schema present
    try:
        launch_spark_application(
            application_name="table-maintenance",
            manifest_file_name="table-maintenance.yaml",
            timeout_seconds="240",
        )
        result = "success"
    except subprocess.CalledProcessError:
        result = "failure"
    assert result == "failure"

    # Running the statistics app should create schema and tables without errors
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
    assert records.count() == telemetry_df.count(), "Running with data populates tables - telemetry"
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_step"
    )
    assert records.count() == statistics_step_df.count(), "Running with data populates tables - statistics_step"
    records = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_cycle"
    )
    assert records.count() == statistics_cycle_df.count(), "Running with data populates tables - statistics_cycle"

    # Table maintenance should succeed after the statistics app has run
    try:
        launch_spark_application(
            application_name="table-maintenance",
            manifest_file_name="table-maintenance.yaml",
            timeout_seconds="240",
        )
        result = "success"
    except subprocess.CalledProcessError:
        result = "failure"
    assert result == "success"
