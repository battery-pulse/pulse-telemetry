import datetime

from pyspark.sql import SparkSession

import pulse_telemetry.logging
from pulse_telemetry.sparklib import iceberg, processing_incremental, statistics_cycle, statistics_step, telemetry

logger = pulse_telemetry.logging.get_logger()


def create_tables_if_not_exist(spark: SparkSession, catalog_name: str, database_name: str):
    logger.info("Creating tables...")
    iceberg.create_table_if_not_exists(
        spark=spark,
        catalog_name=catalog_name,
        database_name=database_name,
        table_name="telemetry",
        table_comment=telemetry.telemetry_comment,
        table_schema=telemetry.telemetry_schema,
        partition_columns=telemetry.telemetry_partitions,
        write_order_columns=telemetry.telemetry_write_order,
    )
    iceberg.create_table_if_not_exists(
        spark=spark,
        catalog_name=catalog_name,
        database_name=database_name,
        table_name="statistics_step",
        table_comment=statistics_step.statistics_step_comment,
        table_schema=statistics_step.statistics_step_schema,
        partition_columns=statistics_step.statistics_step_partitions,
        write_order_columns=statistics_step.statistics_step_write_order,
    )
    iceberg.create_table_if_not_exists(
        spark=spark,
        catalog_name=catalog_name,
        database_name=database_name,
        table_name="statistics_cycle",
        table_comment=statistics_cycle.statistics_cycle_comment,
        table_schema=statistics_cycle.statistics_cycle_schema,
        partition_columns=statistics_cycle.statistics_cycle_partitions,
        write_order_columns=statistics_cycle.statistics_cycle_write_order,
    )
    logger.info("Tables created successfully.")


def process_statistics_step(
    spark: SparkSession,
    catalog_name: str,
    database_name: str,
    watermark_buffer_minutes: int,
    partition_cutoff_days: int,
):
    logger.info("Processing step statistics...")
    source = iceberg.read_table(
        spark=spark, catalog_name=catalog_name, database_name=database_name, table_name="telemetry"
    )
    sink = iceberg.read_table(
        spark=spark, catalog_name=catalog_name, database_name=database_name, table_name="statistics_step"
    )
    incremental = processing_incremental.processing_incremental(
        source=source,
        sink=sink,
        aggregation_function=statistics_step.statistics_step,
        group_by_columns=statistics_step.statistics_step_composite_key,
        partition_cutoff=datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=partition_cutoff_days),
        partition_column="timestamp",
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(minutes=watermark_buffer_minutes),
    )
    iceberg.merge_into_table(
        spark=spark,
        source_df=incremental,
        catalog_name=catalog_name,
        database_name=database_name,
        table_name="statistics_step",
        match_columns=statistics_step.statistics_step_composite_key,
    )
    logger.info("Step statistics processed successfully.")


def process_statistics_cycle(
    spark: SparkSession,
    catalog_name: str,
    database_name: str,
    watermark_buffer_minutes: int,
    partition_cutoff_days: int,
):
    logger.info("Processing cycle statistics...")
    source = iceberg.read_table(
        spark=spark, catalog_name=catalog_name, database_name=database_name, table_name="statistics_step"
    )
    sink = iceberg.read_table(
        spark=spark, catalog_name=catalog_name, database_name=database_name, table_name="statistics_cycle"
    )
    incremental = processing_incremental.processing_incremental(
        source=source,
        sink=sink,
        aggregation_function=statistics_cycle.statistics_cycle,
        group_by_columns=statistics_cycle.statistics_cycle_composite_key,
        partition_cutoff=datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=partition_cutoff_days),
        partition_column="start_time",
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(minutes=watermark_buffer_minutes),
    )
    iceberg.merge_into_table(
        spark=spark,
        source_df=incremental,
        catalog_name=catalog_name,
        database_name=database_name,
        table_name="statistics_cycle",
        match_columns=statistics_cycle.statistics_cycle_composite_key,
    )
    logger.info("Cycle statistics processed successfully.")


def main(spark: SparkSession, catalog: str, database: str, watermark_buffer_minutes: int, partition_cutoff_days: int):
    logger.info("Initiating telemetry statistics processing...")
    logger.info(f"Catalog: {catalog}")
    logger.info(f"Database: {database}")
    logger.info(f"Watermark buffer (minutes): {watermark_buffer_minutes}")
    logger.info(f"Partition cutoff (days): {partition_cutoff_days}")
    create_tables_if_not_exist(spark, catalog, database)
    process_statistics_step(spark, catalog, database, watermark_buffer_minutes, partition_cutoff_days)
    process_statistics_cycle(spark, catalog, database, watermark_buffer_minutes, partition_cutoff_days)
    logger.info("Telemetry statistics job completed.")


if __name__ == "__main__":
    import os

    catalog = os.environ["PULSE_TELEMETRY_CATALOG"]
    database = os.environ["PULSE_TELEMETRY_DATABASE"]
    watermark_buffer_minutes = int(os.environ["PULSE_TELEMETRY_WATERMARK_BUFFER_MINUTES"])
    partition_cutoff_days = int(os.environ["PULSE_TELEMETRY_PARTITION_CUTOFF_DAYS"])

    spark = SparkSession.builder.appName("TelemetryStatistics").getOrCreate()
    main(
        spark=spark,
        catalog=catalog,
        database=database,
        watermark_buffer_minutes=watermark_buffer_minutes,
        partition_cutoff_days=partition_cutoff_days,
    )
