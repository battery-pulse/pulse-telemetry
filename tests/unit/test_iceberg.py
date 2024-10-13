import pyspark.sql.types as T
import pytest
from pulse_telemetry.sparklib.iceberg import _create_table_if_not_exists_statement

expected_with_partitions = """CREATE TABLE IF NOT EXISTS lakehouse.pulse_telemetry.telemetry (
  field_1 boolean COMMENT 'Contains a comment.'
)
USING iceberg
PARTITIONED BY (device_id, test_id, month(timestamp))
COMMENT 'Example table.'"""

expected_without_partitions = """CREATE TABLE IF NOT EXISTS lakehouse.pulse_telemetry.telemetry (
  field_1 boolean COMMENT 'Contains a comment.'
)
USING iceberg
COMMENT 'Example table.'"""


@pytest.fixture(scope="module")
def valid_schema() -> T.StructType:
    return T.StructType(
        [T.StructField("field_1", T.BooleanType(), nullable=True, metadata={"comment": "Contains a comment."})]
    )


@pytest.fixture(scope="module")
def invalid_schema() -> T.StructType:
    return T.StructType([T.StructField("field_1", T.BooleanType(), nullable=True, metadata={"comment": "   "})])


def test_create_table_if_not_exists_statement(valid_schema, invalid_schema):
    # Invalid field comments
    try:
        sql = _create_table_if_not_exists_statement(
            catalog_name="lakehouse",
            database_name="pulse_telemetry",
            table_name="telemetry",
            table_comment="Table comment.",
            table_schema=invalid_schema,
            partition_columns=["device_id", "test_id", "month(timestamp)"],
        )
    except ValueError:
        sql = "failure"
    assert sql == "failure"

    # Invalid table comment
    try:
        sql = _create_table_if_not_exists_statement(
            catalog_name="lakehouse",
            database_name="pulse_telemetry",
            table_name="telemetry",
            table_comment="    ",
            table_schema=valid_schema,
            partition_columns=["device_id", "test_id", "month(timestamp)"],
        )
    except ValueError:
        sql = "failure"
    assert sql == "failure"

    # With partition clause
    sql = _create_table_if_not_exists_statement(
        catalog_name="lakehouse",
        database_name="pulse_telemetry",
        table_name="telemetry",
        table_comment="Example table.",
        table_schema=valid_schema,
        partition_columns=["device_id", "test_id", "month(timestamp)"],
    )
    assert sql == expected_with_partitions

    # Without partition clause
    sql = _create_table_if_not_exists_statement(
        catalog_name="lakehouse",
        database_name="pulse_telemetry",
        table_name="telemetry",
        table_comment="Example table.",
        table_schema=valid_schema,
        partition_columns=None,
    )
    assert sql == expected_without_partitions
