from typing import TYPE_CHECKING

from pyspark.sql.utils import AnalysisException

if TYPE_CHECKING:
    import pyspark.sql.types as T
    from pyspark.sql import DataFrame, SparkSession


def create_table_if_not_exists(
    spark: "SparkSession", database_name: str, table_name: str, schema: "T.StructType", partition_cols: list
):
    """Creates an Iceberg table if it does not already exist in the specified database.

    Make sure to use a Spark session with a metastore catalog defined (e.g., Hive catalog)
    to ensure that the table is created and registered properly in the metastore.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    database_name : str
        The name of the database where the table should be created.
    table_name : str
        The name of the table to be created.
    schema : StructType
        The schema definition of the table as a PySpark StructType.
    partition_cols : list
        List of column names to partition the table by.

    Returns
    -------
    None
    """
    try:
        # Check if the table exists in the catalog
        spark.catalog.getTable(f"{database_name}.{table_name}")
    except AnalysisException:
        # Table does not exist, proceed to create
        print(f"Table '{database_name}.{table_name}' does not exist. Creating it now.")
        (
            spark.createDataFrame([], schema)  # Create an empty DataFrame with the given schema
            .writeTo(f"{database_name}.{table_name}")
            .partitionedBy(*partition_cols)
            .create()
        )


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
    return spark.table(f"{database_name}.{table_name}")


def merge_into_table(
    spark: "SparkSession", source_df: "DataFrame", database_name: str, table_name: str, join_columns: list[str]
):
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
    None

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
