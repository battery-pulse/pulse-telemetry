import datetime
import logging
import sys

from pyspark.sql import SparkSession

from pulse_telemetry.sparklib import iceberg

logger = logging.getLogger("pulse-telemetry")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def main(spark: SparkSession, catalog: str, database: str, table: str, older_than_days: int, retain_last: int):
    logger.info("Initiating table maintenance...")
    logger.info(f"Catalog: {catalog}")
    logger.info(f"Database: {database}")
    logger.info(f"Table: {database}")
    logger.info(f"Older than (days): {older_than_days}")
    logger.info(f"Retain last: {retain_last}")

    older_than = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(days=older_than_days)

    # Expire snapshots
    result = iceberg.expire_snapshots(spark, catalog, database, table, older_than, retain_last)
    logger.info(f"Data files deleted from expired snapshots: {result}")

    # Remove orphan files
    result = iceberg.remove_orphan_files(spark, catalog, database, table, older_than)
    logger.info(f"Orphan files removed: {result}")

    # Rewrite data files
    result = iceberg.rewrite_data_files(spark, catalog, database, table)
    logger.info(f"Modified data files: {result}")

    # Rewrite manifests
    result = iceberg.rewrite_manifests(spark, catalog, database, table)
    logger.info(f"Modified manifest files: {result}")

    logger.info("Table maintenance job completed.")


if __name__ == "__main__":
    import os

    catalog = os.environ["PULSE_TELEMETRY_CATALOG"]
    database = os.environ["PULSE_TELEMETRY_DATABASE"]
    table = os.environ["PULSE_TELEMETRY_TABLE"]
    older_than_days = int(os.environ["PULSE_TELEMETRY_OLDER_THAN_DAYS"])
    retain_last = int(os.environ["PULSE_TELEMETRY_RETAIN_LAST"])

    spark = SparkSession.builder.appName("TableMaintenance").getOrCreate()
    main(
        spark=spark,
        catalog=catalog,
        database=database,
        table=table,
        older_than_days=older_than_days,
        retain_last=retain_last,
    )
