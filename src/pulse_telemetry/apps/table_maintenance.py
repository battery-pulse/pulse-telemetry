import datetime
import logging
import sys

from pyspark.sql import SparkSession

from pulse_telemetry.sparklib import iceberg

logger = logging.getLogger("pulse-telemetry")
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def main(spark: SparkSession, catalog: str, database: str, table: str):
    logger.info("Initiating table maintenance...")
    logger.info(f"Catalog: {catalog}")
    logger.info(f"Database: {database}")
    logger.info(f"Table: {database}")
    iceberg.expire_snapshots(spark, catalog, database, table)
    iceberg.remove_orphan_files(spark, catalog, database, table)
    iceberg.rewrite_data_files(spark, catalog, database, table)
    logger.info("Table maintenance job completed.")


if __name__ == "__main__":
    import os

    catalog = os.environ["PULSE_TELEMETRY_CATALOG"]
    database = os.environ["PULSE_TELEMETRY_DATABASE"]
    table = os.environ["PULSE_TELEMETRY_TABLE"]

    spark = SparkSession.builder.appName("TableMaintenance").getOrCreate()
    main(
        spark=spark,
        catalog=catalog,
        database=database,
        table=table,
    )
