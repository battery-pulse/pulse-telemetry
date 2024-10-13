from typing import TYPE_CHECKING

from pyspark.sql.utils import AnalysisException

if TYPE_CHECKING:
    import pyspark.sql.types as T
    from pyspark.sql import DataFrame, SparkSession


def create_table_if_not_exists(
    spark: "SparkSession",
    database_name: str,
    table_name: str,
    table_comment: str,
    table_schema: "T.StructType",
    partition_columns: list[str] | None = None,
) -> bool:
    """Creates an Iceberg table if it does not already exist in the specified database.

    Make sure to use a Spark session with a default metastore catalog defined (e.g., Hive catalog)
    to ensure that the table is created and registered properly in the metastore.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    database_name : str
        The name of the database where the table should be created.
    table_name : str
        The name of the table to be created.
    table_comment : str
        The comment for the table.
    table_schema : StructType
        The schema definition of the table as a PySpark StructType.
    partition_columns : list, optional
        List of column names to partition the table by.

    Returns
    -------
    bool
        Returns True if the table was created, and False if the table already exists.
    """
    # Creates the database if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS iceberg_catalog.{database_name}")
    try:
        # Check if the table exists in the catalog
        spark.catalog.getTable(f"iceberg_catalog.{database_name}.{table_name}")
        return False  # Table already exists
    except AnalysisException:
        # Table does not exist, proceed to create
        create_table_sql = _create_table_statement(
            database_name=database_name,
            table_name=table_name,
            table_comment=table_comment,
            table_schema=table_schema,
            partition_columns=partition_columns,
        )
        # Execute the SQL query
        spark.sql(create_table_sql)
        return True  # Table was created


def read_table(spark: "SparkSession", database_name: str, table_name: str) -> "DataFrame":
    """Reads a table from the specified database.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use the default catalog.
    database_name : str
        The name of the database where the table is located.
    table_name : str
        The name of the table to be read.

    Returns
    -------
    DataFrame
        A PySpark DataFrame representing the table.
    """
    return spark.sql(f"SELECT * FROM {database_name}.{table_name}")


def merge_into_table(
    spark: "SparkSession", source_df: "DataFrame", database_name: str, table_name: str, join_columns: list[str]
) -> bool:
    """Merges the source DataFrame into the target Iceberg table with update functionality.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use the default catalog.
    source_df : DataFrame
        The DataFrame containing new or updated data.
    database_name : str
        The name of the database where the target table is located.
    table_name : str
        The name of the target table for merging data.
    join_columns : List[str]
        A list of column names that are used to generate the join condition.

    Returns
    -------
    bool
        Returns True if the merge succeeds.

    Example
    -------
    ```
    merge_into_table(
        spark=spark,
        source_df=telemetry,
        database_name="pulse",
        table_name="telemetry",
        join_columns=["device_id", "test_id", "cycle_number", "step_number", "record_number"]
    )
    ```
    """
    source_df.createOrReplaceTempView("source")
    join_condition = " AND ".join([f"target.{col} = source.{col}" for col in join_columns])
    merge_query = f"""
        MERGE INTO {database_name}.{table_name} AS target
        USING source
        ON {join_condition}
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """
    spark.sql(merge_query)
    return True


def _create_clause(database_name: str, table_name: str, table_schema: "T.StructType") -> str:
    query = f"CREATE TABLE IF NOT EXISTS iceberg_catalog.{database_name}.{table_name} ("

    for field in table_schema:
        if not field.metadata or "comment" not in field.metadata or not field.metadata["comment"].strip():
            raise ValueError(f"Field '{field.name}' is missing a valid non-empty comment in the metadata.")

        field_comment = field.metadata["comment"].strip()
        query += f"\n  {field.name} {field.dataType.simpleString()} COMMENT '{field_comment}',"

    query = query.rstrip(",")  # Remove trailing comma
    query += "\n)"
    return query


def _partition_clause(partition_columns: list[str] | None) -> str:
    if partition_columns:
        return f"PARTITIONED BY ({', '.join(partition_columns)})"
    else:
        return ""


def _table_comment_clause(table_comment: str) -> str:
    if table_comment.strip():
        return f"COMMENT '{table_comment}'"
    else:
        raise ValueError("Table comment cannot be empty or whitespace.")


def _create_table_statement(
    database_name: str,
    table_name: str,
    table_comment: str,
    table_schema: "T.StructType",
    partition_columns: list[str] | None = None,
) -> str:
    create_clause = _create_clause(database_name, table_name, table_schema)
    partition_clause = _partition_clause(partition_columns)
    table_comment_clause = _table_comment_clause(table_comment) if table_comment else ""

    final_query = create_clause
    final_query += "\nUSING iceberg"
    if partition_clause:
        final_query += f"\n{partition_clause}"
    final_query += f"\n{table_comment_clause}"

    return final_query
