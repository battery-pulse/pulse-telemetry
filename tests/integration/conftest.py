import os
import pytest
import subprocess
from pulse_telemetry.utils.telemetry_generator import telemetry_generator
from pyspark.sql import DataFrame, SparkSession


current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
scripts_dir = os.path.join(current_dir, 'scripts')
manifest_dir = os.path.join(current_dir, 'manifests')


@pytest.fixture(scope="session")
def kubernetes_services():
    """Fixture to deploy Stackable operators and set up MinIO and Hive Metastore services using NodePort."""
    # Run the setup script before tests
    setup_script_path = os.path.join(scripts_dir, "setup.sh")
    subprocess.run([setup_script_path], cwd=manifest_dir, capture_output=True, text=True, check=True)
    
    # Docker Desktop and kind can be accessed from localhost
    yield {"kubernetes_ip": "localhost"}

    # Run the teardown script after the tests
    teardown_script_path = os.path.join(scripts_dir, "teardown.sh")
    subprocess.run([teardown_script_path], capture_output=True, text=True, check=True)


@pytest.fixture(scope="session")
def spark_session(kubernetes_services) -> SparkSession:
    kubernetes_ip = kubernetes_services["kubernetes_ip"]
    return (
        SparkSession.builder.appName("IntegrationTesting")
        # Hive metastore configuration
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive.type", "hive")
        .config("spark.sql.catalog.hive.uri", f"thrift://{kubernetes_ip}:30001")  # Hive Metastore
        .config("spark.sql.catalog.hive.warehouse", "s3a://my-warehouse/")
        .config("spark.sql.catalog.defaultCatalog", "hive")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{kubernetes_ip}:30000")  # MinIO
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Performance tuning
        .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        # Timezone
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
