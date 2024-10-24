import concurrent.futures
import datetime
import functools

from pyspark.sql import SparkSession

import pulse_telemetry.logging
from pulse_telemetry.sparklib import iceberg

logger = pulse_telemetry.logging.get_logger()


class NoTablesFoundError(Exception):
    """Raised when no tables are found in the schema."""


def table_maintenance(
    spark: SparkSession, catalog: str, database: str, table: str, older_than_days: int, retain_last: int
):
    older_than = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(days=older_than_days)

    # Expire snapshots
    result = iceberg.expire_snapshots(spark, catalog, database, table, older_than, retain_last)
    logger.info(f"{table} - Data files deleted from expired snapshots: {result}")

    # Remove orphan files
    result = iceberg.remove_orphan_files(spark, catalog, database, table, older_than)
    logger.info(f"{table} - Orphan files removed: {result}")

    # Rewrite data files
    result = iceberg.rewrite_data_files(spark, catalog, database, table)
    logger.info(f"{table} - Modified data files: {result}")

    # Rewrite manifests
    result = iceberg.rewrite_manifests(spark, catalog, database, table)
    logger.info(f"{table} - Modified manifest files: {result}")


def main(spark: SparkSession, catalog: str, database: str, older_than_days: int, retain_last: int):
    logger.info("Initiating table maintenance...")
    logger.info(f"Catalog: {catalog}")
    logger.info(f"Database: {database}")
    logger.info(f"Older than (days): {older_than_days}")
    logger.info(f"Retain last: {retain_last}")

    # maps maintenance of tables in the database over multiple threads
    tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {catalog}.{database}")
        .filter("isTemporary = false")
        .select("tableName")
        .collect()
    ]
    logger.info(f"Performing maintenance on: {tables}")
    if not tables:
        logger.error("No tables found.")
        raise NoTablesFoundError(f"No tables found in schema {catalog}.{database}. Failing table maintenance.")
    _table_maintenance = functools.partial(
        table_maintenance,
        spark,  # everything before "table" should be args
        catalog,
        database,
        older_than_days=older_than_days,  # everything after "table" should be kwargs
        retain_last=retain_last,
    )
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for _ in executor.map(_table_maintenance, tables):  # need to exhaust iterator
            pass

    logger.info("Table maintenance job completed.")


if __name__ == "__main__":
    import os

    catalog = os.environ["PULSE_TELEMETRY_CATALOG"]
    database = os.environ["PULSE_TELEMETRY_DATABASE"]
    older_than_days = int(os.environ["PULSE_TELEMETRY_OLDER_THAN_DAYS"])
    retain_last = int(os.environ["PULSE_TELEMETRY_RETAIN_LAST"])

    spark = SparkSession.builder.appName("TableMaintenance").getOrCreate()
    main(
        spark=spark,
        catalog=catalog,
        database=database,
        older_than_days=older_than_days,
        retain_last=retain_last,
    )
