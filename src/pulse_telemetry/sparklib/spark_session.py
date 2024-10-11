from pyspark.sql import SparkSession


def spark_session(app_name: str, hive_uri: str, warehouse_path: str, catalog_name: str = "hive") -> SparkSession:
    """Initializes and returns a SparkSession configured for Iceberg and Hive catalog.

    Uses the default catalog "hive". Methods reading and writing Iceberg tables need to only
    specify the database name and table name.

    Parameters
    ----------
    app_name : str
        The name of the Spark application. This is used to identify the Spark job.
    hive_uri : str
        The URI of the Hive metastore, typically in the form "thrift://hostname:port".
    warehouse_path : str
        The path to the default warehouse directory where Hive-managed tables are stored.
    catalog_name : str
        The name for the metastore catalog, defaults to "hive".

    Returns
    -------
    SparkSession
        A SparkSession object configured to use Iceberg with Hive catalog.

    Example
    -------
    ```
    spark = spark_session(
        app_name="IcebergApp",
        hive_uri="thrift://localhost:9083",
        warehouse_path="s3a://your-warehouse-path/"
    )
    ```
    """
    return (
        SparkSession.builder.appName(app_name)
        # Hive metastore configuration
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "hive")
        .config(f"spark.sql.catalog.{catalog_name}.uri", hive_uri)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config("spark.sql.catalog.defaultCatalog", f"{catalog_name}")  # Set hive as the default catalog
        # Performance tuning
        .config("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)
        # Timezone
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
