from pulse_telemetry.sparklib import iceberg, statistics_cycle, statistics_step, telemetry


def test_telemetry(spark_session, telemetry_df):
    # Created table should be empty
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        table_comment=telemetry.telemetry_schema_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
    )
    empty = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert empty.count() == 0, "Expecting empty table on create"
    assert empty.schema == telemetry_df.schema, "Expecting correct schema on create"
    # Merged table should include all records
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        join_columns=telemetry.telemetry_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert full.count() == telemetry_df.count(), "Expecting full table on merge"
    # Merge should be idempotent
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        join_columns=telemetry.telemetry_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert full.count() == telemetry_df.count(), "Expecting idempotent merge into table"
    # Create table is idempotent
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        table_comment=telemetry.telemetry_schema_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert full.count() == telemetry_df.count(), "Expecting idempotent create table"
    assert full.schema == telemetry_df.schema, "Expecting correct schema after read/write operations"


def test_statistics_step(spark_session, statistics_step_df):
    # Created table should be empty
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step",
        table_comment=statistics_step.statistics_step_schema_comment,
        table_schema=statistics_step.statistics_step_schema,
        partition_columns=statistics_step.statistics_step_partitions,
    )
    empty = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_step"
    )
    assert empty.count() == 0, "Expecting empty table on create"
    # Merged table should include all records
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=statistics_step_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step",
        join_columns=statistics_step.statistics_step_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_step"
    )
    assert full.count() == statistics_step_df.count(), "Expecting full table on merge"


def test_statistics_cycle(spark_session, statistics_cycle_df):
    # Created table should be empty
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_cycle",
        table_comment=statistics_cycle.statistics_cycle_schema_comment,
        table_schema=statistics_cycle.statistics_cycle_schema,
        partition_columns=statistics_cycle.statistics_cycle_partitions,
    )
    empty = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_cycle"
    )
    assert empty.count() == 0, "Expecting empty table on create"
    # Merged table should include all records
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=statistics_cycle_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_cycle",
        join_columns=statistics_cycle.statistics_cycle_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_cycle"
    )
    assert full.count() == statistics_cycle_df.count(), "Expecting full table on merge"
