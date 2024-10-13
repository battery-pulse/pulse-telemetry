import datetime

import pyspark.sql.types as T
import pytest
from pulse_telemetry.sparklib import processing_incremental, statistics_step
from pyspark.sql import DataFrame, Row


@pytest.fixture(scope="module")
def source_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("timestamp", dataType=T.TimestampType(), nullable=False),
            T.StructField("cycle", dataType=T.IntegerType(), nullable=False),
            T.StructField("step", dataType=T.IntegerType(), nullable=False),
            T.StructField("record", dataType=T.IntegerType(), nullable=False),
            T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
        ]
    )


@pytest.fixture(scope="module")
def empty_source(spark_session, source_schema) -> DataFrame:
    return spark_session.createDataFrame([], schema=source_schema)


@pytest.fixture(scope="module")
def populated_source(spark_session, source_schema) -> DataFrame:
    return spark_session.createDataFrame(
        [
            Row(
                timestamp=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                cycle=1,
                step=1,
                record=1,
                update_ts=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            ),
            Row(
                timestamp=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                cycle=1,
                step=1,
                record=2,
                update_ts=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
            ),
            Row(
                timestamp=datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
                cycle=1,
                step=2,
                record=1,
                update_ts=datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
            ),
        ],
        schema=source_schema,
    )


@pytest.fixture(scope="module")
def sink_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("cycle", dataType=T.IntegerType(), nullable=False),
            T.StructField("step", dataType=T.IntegerType(), nullable=False),
            T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
        ]
    )


@pytest.fixture(scope="module")
def empty_sink(spark_session, sink_schema) -> DataFrame:
    return spark_session.createDataFrame([], schema=sink_schema)


@pytest.fixture(scope="module")
def populated_sink(spark_session, sink_schema) -> DataFrame:
    return spark_session.createDataFrame(
        [
            Row(cycle=1, step=1, update_ts=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)),
            Row(cycle=1, step=2, update_ts=datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC)),
        ],
        schema=sink_schema,
    )


@pytest.fixture(scope="module")
def group_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("cycle", dataType=T.IntegerType(), nullable=False),
            T.StructField("step", dataType=T.IntegerType(), nullable=False),
        ]
    )


@pytest.fixture(scope="module")
def all_groups(spark_session, group_schema) -> DataFrame:
    return spark_session.createDataFrame(
        [
            Row(
                cycle=1,
                step=1,
            ),
            Row(
                cycle=1,
                step=2,
            ),
        ],
        schema=group_schema,
    )


def test_adjusted_watermark(empty_sink, populated_sink):
    # Test against an empty sink
    watermark = processing_incremental._adjusted_watermark(
        sink=empty_sink,
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(days=1),
        watermark_default=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
    )
    assert watermark == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), "For empty sink."

    # A sink with data
    watermark = processing_incremental._adjusted_watermark(
        sink=populated_sink,
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(days=1),
        watermark_default=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
    )
    assert watermark == datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC), "For sink with data."


def test_updated_groups_in_source(empty_source, populated_source, all_groups):
    group_by_columns = all_groups.columns

    # Empty because of watermark
    groups = processing_incremental._updated_groups_in_source(
        source=populated_source,
        group_by_columns=group_by_columns,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark=datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC),
        watermark_column="update_ts",
    )
    assert groups.count() == 0, "Expected to be empty because of watermark"

    # Empty because of partition cutoff
    groups = processing_incremental._updated_groups_in_source(
        source=populated_source,
        group_by_columns=group_by_columns,
        partition_cutoff=datetime.datetime(2024, 1, 4, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark=datetime.datetime(2023, 1, 3, tzinfo=datetime.UTC),
        watermark_column="update_ts",
    )
    assert groups.count() == 0, "Expected to be empty because of partition column"

    # All groups
    groups = processing_incremental._updated_groups_in_source(
        source=populated_source,
        group_by_columns=group_by_columns,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        watermark_column="update_ts",
    )
    assert groups.count() == 2, "Expected all groups"
    assert groups.collect() == all_groups.collect(), "Expected all groups"

    # Half of the groups
    groups = processing_incremental._updated_groups_in_source(
        source=populated_source,
        group_by_columns=group_by_columns,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        watermark_column="update_ts",
    )
    assert groups.count() == 1, "Expected one group"
    assert groups.collect()[0] == all_groups.collect()[1], "Expected one group"

    # Empty source
    groups = processing_incremental._updated_groups_in_source(
        source=empty_source,
        group_by_columns=group_by_columns,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        watermark_column="update_ts",
    )
    assert groups.count() == 0, "Expecting no groups for empty source"
    assert groups.columns == group_by_columns, "Expecting correct columns for empty source"


def test_source_records_for_updated_groups(empty_source, populated_source, all_groups):
    # Empty source with groups
    records = processing_incremental._source_records_for_updated_groups(
        source=empty_source,
        updated_groups=all_groups,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
    )
    assert records.count() == 0, "Expecting no records for empty source with groups"
    assert set(records.columns) == set(empty_source.columns), "Expecting correct columns for empty source with groups"

    # Empty source without groups
    records = processing_incremental._source_records_for_updated_groups(
        source=empty_source,
        updated_groups=all_groups.limit(0),
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
    )
    assert records.count() == 0, "Expecting no records for empty source without groups"
    assert set(records.columns) == set(
        empty_source.columns
    ), "Expecting correct columns for empty source without groups"

    # Populated source without groups
    records = processing_incremental._source_records_for_updated_groups(
        source=populated_source,
        updated_groups=all_groups.limit(0),
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
    )
    assert records.count() == 0, "Expecting no records for populated source without groups"
    assert set(records.columns) == set(
        empty_source.columns
    ), "Expecting correct columns for populated source without groups"

    # Populated source with groups
    records = processing_incremental._source_records_for_updated_groups(
        source=populated_source,
        updated_groups=all_groups,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
    )
    assert records.count() == 3, "Expecting records for populated source with groups"
    assert set(records.columns) == set(
        empty_source.columns
    ), "Expecting correct columns for populated source without groups"

    # Partition cutoff truncates results
    records = processing_incremental._source_records_for_updated_groups(
        source=populated_source,
        updated_groups=all_groups,
        partition_cutoff=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
        partition_column="timestamp",
    )
    assert records.count() == 2, "Expecting truncated records with partition cutoff"
    assert set(records.columns) == set(empty_source.columns), "Expecting correct columns with partition cutoff"


def test_processing_incremental(telemetry_df, statistics_step_df):
    # Large watermark buffer
    records = processing_incremental.processing_incremental(
        source=telemetry_df,
        sink=statistics_step_df,
        aggregation_function=statistics_step.statistics_step,
        group_by_columns=["device_id", "test_id", "cycle_number", "step_number"],
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(days=30),
    )
    assert records.count() == statistics_step_df.count(), "Expecting to calculate all steps with large buffer"
    assert set(records.columns) == set(statistics_step_df.columns), "Expecting correct columns with large buffer"

    # No watermark buffer
    records = processing_incremental.processing_incremental(
        source=telemetry_df,
        sink=statistics_step_df,
        aggregation_function=statistics_step.statistics_step,
        group_by_columns=["device_id", "test_id", "cycle_number", "step_number"],
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(days=0),
    )
    assert records.count() == 0, "Expecting to calculate no steps with small buffer"
    assert set(records.columns) == set(statistics_step_df.columns), "Expecting correct columns with small buffer"
