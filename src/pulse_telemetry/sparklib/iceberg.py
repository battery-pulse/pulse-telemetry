from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime

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
    write_order_columns: list[str] | None = None,
) -> None:
    """Creates an Iceberg table if it does not already exist in the specified database.

    Additionally:
     1. creates the database within the catalog if it does not exist. Additionally,
     2. alters the write order of the table (if no write order columns, than unordered)
     3. does not alter the partition order of a table, if it exists

    Make sure to use a Spark session with an iceberg catalog and object storage configured.

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
    write_order_columns : list, optional
        List of columns to enforce write order.

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
    if write_order_columns:
        alter_table_write_order_sql = _alter_table_write_order_statement(
            catalog_name=catalog_name,
            database_name=database_name,
            table_name=table_name,
            write_order_columns=write_order_columns,
        )
        spark.sql(alter_table_write_order_sql)
    else:
        spark.sql(f"ALTER TABLE {catalog_name}.{database_name}.{table_name} WRITE UNORDERED")


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

    Will raise an AnalysisException if the source schema does not match the target table.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
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


def expire_snapshots(
    spark: "SparkSession",
    catalog_name: str,
    database_name: str,
    table_name: str,
    older_than: "datetime.datetime",
    retain_last: int,
    max_concurrent_deletes: int = 8,
) -> int:
    """Removes old snapshots from the specified Iceberg table.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    catalog_name : str
        The name of the catalog where the database is located.
    database_name : str
        The name of the database where the table is located.
    table_name : str
        The name of the target table from which snapshots will be expired.
    older_than : datetime
        A datetime object specifying the cutoff for snapshot expiration.
    retain_last : int
        The minimum number of most recent snapshots to retain.
    max_concurrent_deletes : int
        The maximum number of concurrent delete threads for file deletion.

    Returns
    -------
    int
        The number of data files deleted during snapshot expiration.

    """
    older_than_str = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return spark.sql(f"""
        CALL {catalog_name}.system.expire_snapshots(
            table => '{database_name}.{table_name}',
            older_than => TIMESTAMP '{older_than_str}',
            retain_last => {retain_last},
            max_concurrent_deletes => {max_concurrent_deletes}
        )
    """).collect()[0]["deleted_data_files_count"]


def remove_orphan_files(
    spark: "SparkSession",
    catalog_name: str,
    database_name: str,
    table_name: str,
    older_than: "datetime.datetime",
    max_concurrent_deletes: int = 8,
) -> int:
    """Removes orphaned files from the specified Iceberg table.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    catalog_name : str
        The name of the catalog where the database is located.
    database_name : str
        The name of the database where the table is located.
    table_name : str
        The name of the target table from which orphaned files will be removed.
    older_than : datetime
        A datetime object specifying the cutoff for orphaned file removal.
    max_concurrent_deletes : int
        The maximum number of concurrent delete threads for file deletion during orphaned file removal.

    Returns
    -------
    int
        The count of orphaned files removed during the operation.

    """
    older_than_str = older_than.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return spark.sql(f"""
        CALL {catalog_name}.system.remove_orphan_files(
            table => '{database_name}.{table_name}',
            older_than => TIMESTAMP '{older_than_str}',
            max_concurrent_deletes => {max_concurrent_deletes}
        )
    """).count()


def rewrite_data_files(
    spark: "SparkSession",
    catalog_name: str,
    database_name: str,
    table_name: str,
) -> int:
    """Rewrites data files from the specified Iceberg table.

    Uses the 'sort' strategy and defaults to the table's sort-order.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    catalog_name : str
        The name of the catalog where the database is located.
    database_name : str
        The name of the database where the table is located.
    table_name : str
        The name of the target table from which orphaned files will be removed.

    Returns
    -------
    int
        The sum of the rewritten and new data files.

    """
    result = spark.sql(f"""
        CALL {catalog_name}.system.rewrite_data_files(
            table => '{database_name}.{table_name}',
            strategy => 'sort'
        )
    """).collect()[0]
    return result["rewritten_data_files_count"] + result["added_data_files_count"]


def rewrite_manifests(
    spark: "SparkSession",
    catalog_name: str,
    database_name: str,
    table_name: str,
) -> int:
    """Rewrites manifest files from the specified Iceberg table.

    Parameters
    ----------
    spark : SparkSession
        The Spark session object, configured to use a catalog that supports Iceberg tables.
    catalog_name : str
        The name of the catalog where the database is located.
    database_name : str
        The name of the database where the table is located.
    table_name : str
        The name of the target table from which orphaned files will be removed.

    Returns
    -------
    int
        The sum of the rewritten and new manifest files.

    """
    result = spark.sql(f"""
        CALL {catalog_name}.system.rewrite_manifests(
            table => '{database_name}.{table_name}'
        )
    """).collect()[0]
    return result["rewritten_manifests_count"] + result["added_manifests_count"]


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


def _alter_table_write_order_statement(
    catalog_name: str, database_name: str, table_name: str, write_order_columns: list[str]
) -> str:
    cols = ", ".join(write_order_columns)
    return f"ALTER TABLE {catalog_name}.{database_name}.{table_name} WRITE ORDERED BY {cols}"
