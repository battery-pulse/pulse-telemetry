from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyspark.sql.types as T
    from pyspark.sql import DataFrame, SparkSession


def create_table_if_not_exists(
    spark: "SparkSession",
    catalog_name: str,
    database_name: str,
    table_name: str,
    table_comment: str,
    table_schema: "T.StructType",
    partition_columns: list[str] | None = None,
) -> None:
    """Creates an Iceberg table if it does not already exist in the specified database.

    Additionally, creates the database within the catalog if it does not exist. Make sure
    to use a Spark session with an iceberg catalog and object storage configured.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    catalog_name : str
        The name of the catalog where the database should be created.
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
    None
    """
    # Creates the database if it does not exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{database_name}")
    # Creates the table if it does not exist
    create_table_if_not_exist_sql = _create_table_if_not_exists_statement(
        catalog_name=catalog_name,
        database_name=database_name,
        table_name=table_name,
        table_comment=table_comment,
        table_schema=table_schema,
        partition_columns=partition_columns,
    )
    spark.sql(create_table_if_not_exist_sql)


def read_table(spark: "SparkSession", catalog_name: str, database_name: str, table_name: str) -> "DataFrame":
    """Reads an Iceberg table from the specified database.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    catalog_name : str
        The name of the catalog, where the database is located.
    database_name : str
        The name of the database, where the table is located.
    table_name : str
        The name of the table to read.

    Returns
    -------
    DataFrame
        A PySpark DataFrame representing the table.
    """
    return spark.sql(f"SELECT * FROM {catalog_name}.{database_name}.{table_name}")


def merge_into_table(
    spark: "SparkSession",
    source_df: "DataFrame",
    catalog_name: str,
    database_name: str,
    table_name: str,
    match_columns: list[str],
) -> None:
    """Merges the source DataFrame into the target Iceberg table with update functionality.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use the default catalog.
    source_df : DataFrame
        The DataFrame containing new or updated data.
    catalog_name : str
        The name of the catalog where the database is located.
    database_name : str
        The name of the database where the table is located.
    table_name : str
        The name of the target table.
    match_columns : List[str]
        A list of column names that are used to generate the match condition.

    Returns
    -------
    None
    """
    source_df.createOrReplaceTempView("source")
    match_condition = " AND ".join([f"target.{col} = source.{col}" for col in match_columns])
    merge_query = f"""
        MERGE INTO {catalog_name}.{database_name}.{table_name} AS target
        USING source
        ON {match_condition}
        WHEN MATCHED THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
    """
    spark.sql(merge_query)


def _create_clause(catalog_name: str, database_name: str, table_name: str, table_schema: "T.StructType") -> str:
    query = f"CREATE TABLE IF NOT EXISTS {catalog_name}.{database_name}.{table_name} ("
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
        return f"\nPARTITIONED BY ({', '.join(partition_columns)})"
    else:
        return ""


def _comment_clause(table_comment: str) -> str:
    if table_comment.strip():
        return f"\nCOMMENT '{table_comment}'"
    else:
        raise ValueError("Table comment cannot be empty or whitespace.")


def _create_table_if_not_exists_statement(
    catalog_name: str,
    database_name: str,
    table_name: str,
    table_comment: str,
    table_schema: "T.StructType",
    partition_columns: list[str] | None = None,
) -> str:
    return "".join(
        [
            _create_clause(catalog_name, database_name, table_name, table_schema),
            "\nUSING iceberg",
            _partition_clause(partition_columns),
            _comment_clause(table_comment),
        ]
    )
