from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


statistics_step_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False),
        T.StructField("test_id", dataType=T.StringType(), nullable=False),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),
        T.StructField("step_number", dataType=T.LongType(), nullable=False),
        T.StructField("step_type", dataType=T.StringType(), nullable=True),
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True),
        # Time
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False),
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False),
        # Current
        T.StructField("start_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_abs_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_abs_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("mean_current__A", dataType=T.DoubleType(), nullable=False),
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("mean_voltage__V", dataType=T.DoubleType(), nullable=False),
        # Power
        T.StructField("start_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_abs_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_abs_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("mean_power__W", dataType=T.DoubleType(), nullable=False),
        # Accumulations (within the step)
        T.StructField("charge_capacity__Ah", dataType=T.DoubleType(), nullable=False),
        T.StructField("discharge_capacity__Ah", dataType=T.DoubleType(), nullable=False),
        T.StructField("charge_energy__Wh", dataType=T.DoubleType(), nullable=False),
        T.StructField("discharge_energy__Wh", dataType=T.DoubleType(), nullable=False),
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
    ]
)


def statistics_step(df: "DataFrame") -> "DataFrame":
    """Returns step-level statistics of the timeseries data.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame with the "timeseries" schema.

    Returns
    -------
    DataFrame
        Aggregated statistics at the step level.

    Notes
    -----
    Can be applied in both batch and streaming contexts, but it is reccomended to always run a
    batch job to finalize statistics for late data. In streaming mode, use a watermark to limit
    in-memory state. Use update_ts to avoid issues with backfill of historical data:

    ```
    df = df.withWatermark("update_ts", "14 days")
    ```
    """
    return df.groupBy(
        "device_id",
        "test_id",
        "cycle_number",
        "step_number",
    ).agg(
        # Identifiers (groupbys are already included)
        F.first("step_type", ignorenulls=True).alias("step_type"),  # Will return null if all null
        F.first("step_id", ignorenulls=True).alias("step_id"),
        # Time
        F.min("timestamp").alias("start_time"),
        F.max("timestamp").alias("end_time"),
        # Current
        F.min_by("current__A", "timestamp").alias("start_current__A"),
        F.max_by("current__A", "timestamp").alias("end_current__A"),
        F.min("current__A").alias("min_current__A"),
        F.min(F.abs("current__A")).alias("min_abs_current__A"),
        F.max("current__A").alias("max_current__A"),
        F.max(F.abs("current__A")).alias("max_abs_current__A"),
        F.avg("current__A").alias("mean_current__A"),
        # Voltage
        F.min_by("voltage__V", "timestamp").alias("start_voltage__V"),
        F.max_by("voltage__V", "timestamp").alias("end_voltage__V"),
        F.min("voltage__V").alias("min_voltage__V"),
        F.max("voltage__V").alias("max_voltage__V"),
        F.avg("voltage__V").alias("mean_voltage__V"),
        # Power
        F.min_by("power__W", "timestamp").alias("start_power__W"),
        F.max_by("power__W", "timestamp").alias("end_power__W"),
        F.min("power__W").alias("min_power__W"),
        F.min(F.abs("power__W")).alias("min_abs_power__W"),
        F.max("power__W").alias("max_power__W"),
        F.max(F.abs("power__W")).alias("max_abs_power__W"),
        F.avg("power__W").alias("mean_power__W"),
        # Accumulations (within the step) - charging is negative current
        F.sum(F.when(F.col("current__A") <= 0, F.col("capacity__Ah")).otherwise(0.0)).alias("charge_capacity__Ah"),
        F.sum(F.when(F.col("current__A") > 0, F.col("capacity__Ah")).otherwise(0.0)).alias("discharge_capacity__Ah"),
        F.sum(F.when(F.col("current__A") <= 0, F.col("energy__Wh")).otherwise(0.0)).alias("charge_energy__Wh"),
        F.sum(F.when(F.col("current__A") > 0, F.col("energy__Wh")).otherwise(0.0)).alias("discharge_energy__Wh"),
        # Metadata
        F.current_timestamp().alias("update_ts"),  # Timestamp in timezone configured in Spark environment
    )
