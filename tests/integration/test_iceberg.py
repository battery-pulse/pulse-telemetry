import pyspark.sql.functions as F
from pulse_telemetry.sparklib import iceberg, statistics_cycle, statistics_step, telemetry
from pyspark.sql import Window
from pyspark.sql.utils import AnalysisException


def test_telemetry(spark_session, telemetry_df):
    # Created table should be empty
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=telemetry.telemetry_write_order,
    )
    empty = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert empty.count() == 0, "Created table should be empty"
    assert empty.schema == telemetry_df.schema, "Expecting correct schema on create"
    # Merged table should include all records
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert full.count() == telemetry_df.count(), "Merged table should include all records"
    # Merge should be idempotent
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert full.count() == telemetry_df.count(), "Merge should be idempotent"
    # Create table should be idempotent
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=telemetry.telemetry_write_order,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="telemetry"
    )
    assert full.count() == telemetry_df.count(), "Create table should be idempotent"
    assert full.schema == telemetry_df.schema, "Expecting correct schema after read/write operations"
    # Merge should respect partitions
    parts = spark_session.sql("SELECT * FROM lakehouse.dev.telemetry.partitions")
    assert parts.count() == 5, "Merge should respect partitions"


def test_statistics_step(spark_session, statistics_step_df):
    # Created table should be empty
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step",
        table_comment=statistics_step.statistics_step_comment,
        table_schema=statistics_step.statistics_step_schema,
        partition_columns=statistics_step.statistics_step_partitions,
        write_order_columns=statistics_step.statistics_step_write_order,
    )
    empty = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_step"
    )
    assert empty.count() == 0, "Created table should be empty"
    # Merged table should include all records
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=statistics_step_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step",
        match_columns=statistics_step.statistics_step_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_step"
    )
    assert full.count() == statistics_step_df.count(), "Merged table should include all records"


def test_statistics_cycle(spark_session, statistics_cycle_df):
    # Created table should be empty
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_cycle",
        table_comment=statistics_cycle.statistics_cycle_comment,
        table_schema=statistics_cycle.statistics_cycle_schema,
        partition_columns=statistics_cycle.statistics_cycle_partitions,
        write_order_columns=statistics_cycle.statistics_cycle_write_order,
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
        match_columns=statistics_cycle.statistics_cycle_composite_key,
    )
    full = iceberg.read_table(
        spark=spark_session, catalog_name="lakehouse", database_name="dev", table_name="statistics_cycle"
    )
    assert full.count() == statistics_cycle_df.count(), "Merged table should include all records"


def test_merge_bad_data(spark_session, telemetry_df):
    # Should not be able to merge bad data
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_cycle",
        table_comment=statistics_cycle.statistics_cycle_comment,
        table_schema=statistics_cycle.statistics_cycle_schema,
        partition_columns=statistics_cycle.statistics_cycle_partitions,
        write_order_columns=statistics_cycle.statistics_cycle_write_order,
    )
    try:
        iceberg.merge_into_table(
            spark=spark_session,
            source_df=telemetry_df,
            catalog_name="lakehouse",
            database_name="dev",
            table_name="statistics_cycle",
            match_columns=statistics_cycle.statistics_cycle_composite_key,
        )
        failed = False
    except AnalysisException:
        failed = True
    assert failed, "Should not be able to merge bad data"


def test_changing_write_order(spark_session, telemetry_df):
    # Check initial write order
    result = spark_session.sql("SELECT record_number FROM lakehouse.dev.telemetry LIMIT 1").collect()[0][0]
    assert result == 1, "Check initial write order"
    # Check that write order can be changed
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=None,
    )
    spark_session.sql("DELETE FROM lakehouse.dev.telemetry")  # update preserves position
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df.orderBy("record_number", ascending=False),
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
    telemetry_table = spark_session.sql("SELECT record_number FROM lakehouse.dev.telemetry")
    window_spec = Window.orderBy("record_number")
    unordered_df = telemetry_table.withColumn("prev_record", F.lag("record_number").over(window_spec))
    unordered_check = unordered_df.filter(F.col("record_number") > F.col("prev_record")).count()
    assert unordered_check > 5, "Data should not be ordered in ascending order"


def test_partitioning_can_not_be_changed(spark_session):
    # Check initial partitions
    result = spark_session.sql("DESCRIBE EXTENDED lakehouse.dev.telemetry")
    result = result.filter(result["col_name"].contains("Part ")).count()
    assert result == 3, "Check initial partitions"
    # Check that partitions can not be changed after create
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=None,
        write_order_columns=telemetry.telemetry_write_order,
    )
    result = spark_session.sql("DESCRIBE EXTENDED lakehouse.dev.telemetry")
    result = result.filter(result["col_name"].contains("Part ")).count()
    assert result == 3, "Check that partitions can not be changed after create"


def test_table_maintenance(spark_session, telemetry_df):
    # Check initial metadata and file system
    iceberg.create_table_if_not_exists(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="table_maintenance",  # fresh database for this test
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=telemetry.telemetry_write_order,
    )
    iceberg.merge_into_table(
        spark=spark_session,
        source_df=telemetry_df,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
        match_columns=telemetry.telemetry_composite_key,
    )
    # Check number of expired snapshots after new data
    iceberg.expire_snapshots(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
    )
    iceberg.remove_orphan_files(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
    )
    iceberg.rewrite_data_files(
        spark=spark_session,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry",
    )
