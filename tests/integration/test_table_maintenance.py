from pulse_telemetry.apps import table_maintenance
from pulse_telemetry.sparklib import iceberg, telemetry


def test_main(spark_session, telemetry_df):
    # Can not run job against schema that does not exist
    try:
        table_maintenance.main(
            spark=spark_session,
            catalog="lakehouse",
            database="table_maintenance",
            older_than_days=5,
            retain_last=3,
        )
        result = "passed"
    except table_maintenance.NoTablesFoundError:
        result = "failed"
    assert result == "failed", "Can not run job against schema that does not exist"

    # Job should fail when schema exists but there are no tables
    spark_session.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.table_maintenance")
    try:
        table_maintenance.main(
            spark=spark_session,
            catalog="lakehouse",
            database="table_maintenance",
            older_than_days=5,
            retain_last=3,
        )
        result = "passed"
    except table_maintenance.NoTablesFoundError:
        result = "failed"
    assert result == "failed", "Job should fail if there is a schema but no tables"

    # Job should pass when a table exists
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="table_maintenance",
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=telemetry.telemetry_write_order,
    )
    try:
        table_maintenance.main(
            spark=spark_session,
            catalog="lakehouse",
            database="table_maintenance",
            older_than_days=5,
            retain_last=3,
        )
        result = "passed"
    except table_maintenance.NoTablesFoundError:
        result = "failed"
    assert result == "passed", "Job should pass on table that does exist"
