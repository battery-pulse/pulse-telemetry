import datetime
import pulse_telemetry.sparklib.processing_incremental as processing_incremental
import pulse_telemetry.sparklib.statistics_cycle as statistics_cycle
import pulse_telemetry.sparklib.statistics_step as statistics_step
import pulse_telemetry.sparklib.iceberg as iceberg
from pyspark.sql import SparkSession


def spark():
    return (
        SparkSession.builder.appName("IntegrationTesting")
        # Iceberg and S3 packages
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Hive metastore configuration
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://localhost:9083")  # Hive Metastore
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/")
        # S3 configuration
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")  # MinIO
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Timezone
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_tables_if_not_exist():
    pass


def process_statistics_step(spark):
    source = iceberg.read_table(
        spark=spark,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="telemetry"
    )
    sink = iceberg.read_table(
        spark=spark,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step"
    )
    incremental = processing_incremental.processing_incremental(
        source=source,
        sink=sink,
        aggregation_function=statistics_step.statistics_step,
        group_by_columns=statistics_step.statistics_step_composite_key,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(days=30),
    )
    iceberg.merge_into_table(
        spark=spark,
        source_df=incremental,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step",
        match_columns=statistics_step.statistics_step_composite_key,
    )


def process_statistics_cycle(spark):
    source = iceberg.read_table(
        spark=spark,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_step"
    )
    sink = iceberg.read_table(
        spark=spark,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_cycle"
    )
    incremental = processing_incremental.processing_incremental(
        source=source,
        sink=sink,
        aggregation_function=statistics_cycle.statistics_cycle,
        group_by_columns=statistics_cycle.statistics_cycle_composite_key,
        partition_cutoff=datetime.datetime(2023, 1, 1, tzinfo=datetime.UTC),
        partition_column="timestamp",
        watermark_column="update_ts",
        watermark_buffer=datetime.timedelta(days=30),
    )
    iceberg.merge_into_table(
        spark=spark,
        source_df=incremental,
        catalog_name="lakehouse",
        database_name="dev",
        table_name="statistics_cycle",
        match_columns=statistics_cycle.statistics_cycle_composite_key,
    )


def main():
    spark_session = spark()
    create_tables_if_not_exist()
    process_statistics_step()
    process_statistics_cycle()


if __name__ == "__main__":
    main()
