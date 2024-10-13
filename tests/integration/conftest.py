import os
import pytest
import stat
import subprocess
from pulse_telemetry.sparklib.transformations.statistics_cycle import statistics_cycle
from pulse_telemetry.sparklib.transformations.statistics_step import statistics_step
from pulse_telemetry.sparklib.transformations.telemetry import telemetry_schema
from pulse_telemetry.utils import channel
from pulse_telemetry.utils.telemetry_generator import telemetry_generator
from pyspark.sql import DataFrame, SparkSession


current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
scripts_dir = os.path.join(current_dir, 'scripts')
manifest_dir = os.path.join(current_dir, 'manifests')


@pytest.fixture(scope="session")
def kind_cluster():
    subprocess.run(["kind", "create", "cluster"], check=True)
    yield
    subprocess.run(["kind", "delete", "cluster"], check=True)


@pytest.fixture(scope="session")
def kubernetes_services(kind_cluster):
    # Install stackable operators and setup hive and minio
    setup_script_path = os.path.join(scripts_dir, "setup.sh")
    os.chmod(setup_script_path, os.stat(setup_script_path).st_mode | stat.S_IEXEC)
    subprocess.run([setup_script_path], cwd=manifest_dir, capture_output=True, text=True, check=True)

    # Port-forward minio and hive services
    minio_forward = subprocess.Popen(
        ["kubectl", "port-forward", "svc/minio", "9000:9000"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    hive_forward = subprocess.Popen(
        ["kubectl", "port-forward", "svc/hive-metastore", "9083:9083"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    
    yield

    # Terminate port-forwarding processes
    minio_forward.terminate()
    hive_forward.terminate()


@pytest.fixture(scope="session")
def spark_session(kubernetes_services) -> SparkSession:
    return (
        SparkSession.builder.appName("IntegrationTesting")
        # Iceberg extensions
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Hive metastore configuration
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "hive")
        .config("spark.sql.catalog.iceberg_catalog.uri", f"thrift://localhost:9083")  # Hive Metastore
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://my-warehouse/")
        # Object storage configuration
        .config("spark.hadoop.fs.s3a.endpoint", f"http://localhost:9000")  # MinIO
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Performance tuning
        .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        # Timezone
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def telemetry_df(spark_session) -> DataFrame:
    # Runs the generator against a local buffer
    local_buffer = channel.LocalBuffer()
    channel.run_with_timeout(
        source=telemetry_generator,
        sink=local_buffer,
        topic="telemetry",
        num_channels=5,
        timeout_seconds=3,
        acquisition_frequency=10,
        points_per_step=5,
        lower_voltage_limit=3,  # V
        upper_voltage_limit=4,  # V
        current=1.0,  # A
    )

    # Collects the results in a pyspark dataframe
    telemetry = local_buffer.dataframe(spark_session, telemetry_schema)

    return telemetry


@pytest.fixture(scope="session")
def statistics_step_df(telemetry_df) -> DataFrame:
    return statistics_step(telemetry_df)


@pytest.fixture(scope="session")
def statistics_cycle_df(statistics_step_df) -> DataFrame:
    return statistics_cycle(statistics_step_df)
