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
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False),  # Time at the start of the step
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False),  # Time at the end of the step
        T.StructField("duration__s", dataType=T.DoubleType(), nullable=False),  # Time over the step
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False),  # Voltage at the start of the step
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False),  # Voltage at the end of the step
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False),  # Minimum voltage over the step
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False),  # Maximum voltage over the step
        T.StructField("time_averaged_voltage__V", dataType=T.DoubleType(), nullable=False),  # Average voltage over the step
        # Current (signed, but min means smallest and max largest)
        T.StructField("start_current__A", dataType=T.DoubleType(), nullable=False),  # Current at the start of the step
        T.StructField("end_current__A", dataType=T.DoubleType(), nullable=False),  # Current at the end of the step
        T.StructField("min_charge_current__A", dataType=T.DoubleType(), nullable=True),  # Smallest current in charge state
        T.StructField("min_discharge_current__A", dataType=T.DoubleType(), nullable=True),  # Smallest current in discharge state
        T.StructField("max_charge_current__A", dataType=T.DoubleType(), nullable=True),  # Largest current in charge state
        T.StructField("max_discharge_current__A", dataType=T.DoubleType(), nullable=True),  # Largest current in discharge state
        T.StructField("time_averaged_current__A", dataType=T.DoubleType(), nullable=False),  # Average current over the step
        # Power (signed, but min means smallest and max largest)
        T.StructField("start_power__W", dataType=T.DoubleType(), nullable=False),  # Power at the start of the step
        T.StructField("end_power__W", dataType=T.DoubleType(), nullable=False),  # Power at the end of the step
        T.StructField("min_charge_power__W", dataType=T.DoubleType(), nullable=True),  # Smallest power in the charge state
        T.StructField("min_discharge_power__W", dataType=T.DoubleType(), nullable=True),  # Smallest power in the discharge state
        T.StructField("max_charge_power__W", dataType=T.DoubleType(), nullable=True),  # Largest power in the charge state
        T.StructField("max_discharge_power__W", dataType=T.DoubleType(), nullable=True),  # Largest power in the discharge state
        T.StructField("time_averaged_power__W", dataType=T.DoubleType(), nullable=False),  # Average power over the step
        # Accumulations (unsigned)
        T.StructField("charge_capacity__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity charged over the step
        T.StructField("discharge_capacity__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity discharged over the step
        T.StructField("charge_energy__Wh", dataType=T.DoubleType(), nullable=False),  # Unsigned energy charged over the step
        T.StructField("discharge_energy__Wh", dataType=T.DoubleType(), nullable=False),  # Unsigned energy discharged over the step
        # Resolution diagnostics (unsigned)
        T.StructField("max_voltage_delta__V", dataType=T.DoubleType(), nullable=False),  # Largest change in voltage (of a single record) over the step
        T.StructField("max_current_delta__A", dataType=T.DoubleType(), nullable=False),  # Largest change in current (of a single record) over the step
        T.StructField("max_duration__s", dataType=T.DoubleType(), nullable=False),  # Largest change in time (of a single record) over the step
        T.StructField("num_records", dataType=T.LongType(), nullable=False),  # Number of records over the step
        # Auxiliary metrics
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True),  # First non-null auxiliary entry over the step
        T.StructField("metadata", dataType=T.StringType(), nullable=True),  # First non-null metadata entry over the step
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
    ]
)  # fmt: skip


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
    Can be applied in both batch and streaming contexts, but it is recommended to always run a
    batch job to finalize statistics for late data. In streaming mode, use a watermark to limit
    in-memory state. Use update_ts to avoid issues with backfill of historical data:

    ```
    df = df.withWatermark("update_ts", "14 days")
    ```
    """
    # Calculating weighted averages using the duration__s column
    time_weighted_avg = lambda col: (F.sum(F.col(col) * F.col("duration__s")) / F.sum("duration__s"))  # noqa: E731

    return df.groupBy("device_id", "test_id", "cycle_number", "step_number").agg(
        # Identifiers (groupbys are already included)
        F.first("step_type", ignorenulls=True).alias("step_type"),
        F.first("step_id", ignorenulls=True).alias("step_id"),
        # Time
        F.min_by("timestamp", "record_number").alias("start_time"),
        F.max_by("timestamp", "record_number").alias("end_time"),
        (
            F.max_by("timestamp", "record_number").cast("double")
            - F.min_by("timestamp", "record_number").cast("double")
        ).alias("duration__s"),
        # Voltage
        F.min_by("voltage__V", "record_number").alias("start_voltage__V"),
        F.max_by("voltage__V", "record_number").alias("end_voltage__V"),
        F.min("voltage__V").alias("min_voltage__V"),
        F.max("voltage__V").alias("max_voltage__V"),
        time_weighted_avg("voltage__V").alias("time_averaged_voltage__V"),
        # Current (keeping sign but min is "smallest", max is "largest")
        F.min_by("current__A", "record_number").alias("start_current__A"),
        F.max_by("current__A", "record_number").alias("end_current__A"),
        F.min(F.when(F.col("current__A") > 0, F.col("current__A"))).alias("min_charge_current__A"),
        F.max(F.when(F.col("current__A") < 0, F.col("current__A"))).alias("min_discharge_current__A"),
        F.max(F.when(F.col("current__A") > 0, F.col("current__A"))).alias("max_charge_current__A"),
        F.min(F.when(F.col("current__A") < 0, F.col("current__A"))).alias("max_discharge_current__A"),
        time_weighted_avg("current__A").alias("time_averaged_current__A"),
        # Power (keeping sign but min is "smallest", max is "largest")
        F.min_by("power__W", "record_number").alias("start_power__W"),
        F.max_by("power__W", "record_number").alias("end_power__W"),
        F.min(F.when(F.col("power__W") > 0, F.col("power__W"))).alias("min_charge_power__W"),
        F.max(F.when(F.col("power__W") < 0, F.col("power__W"))).alias("min_discharge_power__W"),
        F.max(F.when(F.col("power__W") > 0, F.col("power__W"))).alias("max_charge_power__W"),
        F.min(F.when(F.col("power__W") < 0, F.col("power__W"))).alias("max_discharge_power__W"),
        time_weighted_avg("power__W").alias("time_averaged_power__W"),
        # Accumulations (within the step, and remember step capacity/energy is unsigned)
        F.max_by("step_capacity_charged__Ah", "record_number").alias("charge_capacity__Ah"),
        F.max_by("step_capacity_discharged__Ah", "record_number").alias("discharge_capacity__Ah"),
        F.max_by("step_energy_charged__Wh", "record_number").alias("charge_energy__Wh"),
        F.max_by("step_energy_discharged__Wh", "record_number").alias("discharge_energy__Wh"),
        # Resolution diagnostics
        F.max(F.abs("voltage_delta__V")).alias("max_voltage_delta__V"),
        F.max(F.abs("current_delta__A")).alias("max_current_delta__A"),
        F.max("duration__s").alias("max_duration__s"),
        F.count("*").alias("num_records"),
        # Auxiliary metrics
        F.first("auxiliary", ignorenulls=True).alias("auxiliary"),
        F.first("metadata", ignorenulls=True).alias("metadata"),
        # Metadata
        F.current_timestamp().alias("update_ts"),  # Timestamp in timezone configured in Spark environment
    )
