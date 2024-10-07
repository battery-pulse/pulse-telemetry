from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


statistics_cycle_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False),
        T.StructField("test_id", dataType=T.StringType(), nullable=False),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),
        # Time
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False),  # Time at the start of the cycle
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False),  # Time at the end of the cycle
        T.StructField("duration__s", dataType=T.DoubleType(), nullable=False),  # Time over the cycle
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False),  # Voltage at the start of the cycle
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False),  # Voltage at the end of the cycle
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False),  # Minimum voltage over the cycle
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False),  # Maximum voltage over the cycle
        T.StructField("time_averaged_voltage__V", dataType=T.DoubleType(), nullable=False),  # Average voltage over the cycle
        # Current (signed, but min means smallest and max largest)
        T.StructField("start_current__A", dataType=T.DoubleType(), nullable=False),  # Current at the start of the cycle
        T.StructField("end_current__A", dataType=T.DoubleType(), nullable=False),  # Current at the end of the cycle
        T.StructField("min_charge_current__A", dataType=T.DoubleType(), nullable=True),  # Smallest current in charge state
        T.StructField("min_discharge_current__A", dataType=T.DoubleType(), nullable=True),  # Smallest current in discharge state
        T.StructField("max_charge_current__A", dataType=T.DoubleType(), nullable=True),  # Largest current in charge state
        T.StructField("max_discharge_current__A", dataType=T.DoubleType(), nullable=True),  # Largest current in discharge state
        T.StructField("time_averaged_current__A", dataType=T.DoubleType(), nullable=False),  # Average current over the cycle
        # Power (signed, but min means smallest and max largest)
        T.StructField("start_power__W", dataType=T.DoubleType(), nullable=False),  # Power at the start of the cycle
        T.StructField("end_power__W", dataType=T.DoubleType(), nullable=False),  # Power at the end of the cycle
        T.StructField("min_charge_power__W", dataType=T.DoubleType(), nullable=True),  # Smallest power in the charge state
        T.StructField("min_discharge_power__W", dataType=T.DoubleType(), nullable=True),  # Smallest power in the discharge state
        T.StructField("max_charge_power__W", dataType=T.DoubleType(), nullable=True),  # Largest power in the charge state
        T.StructField("max_discharge_power__W", dataType=T.DoubleType(), nullable=True),  # Largest power in the discharge state
        T.StructField("time_averaged_power__W", dataType=T.DoubleType(), nullable=False),  # Average power over the cycle
        # Accumulations (unsigned)
        T.StructField("charge_capacity__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity charged over the cycle
        T.StructField("discharge_capacity__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity discharged over the cycle
        T.StructField("charge_energy__Wh", dataType=T.DoubleType(), nullable=False),  # Unsigned energy charged over the cycle
        T.StructField("discharge_energy__Wh", dataType=T.DoubleType(), nullable=False),  # Unsigned energy discharged over the cycle
        # Resolution diagnostics (unsigned)
        T.StructField("max_voltage_delta__V", dataType=T.DoubleType(), nullable=False),  # Largest change in voltage (of a single record) over the cycle
        T.StructField("max_current_delta__A", dataType=T.DoubleType(), nullable=False),  # Largest change in current (of a single record) over the cycle
        T.StructField("max_power_delta__W", dataType=T.DoubleType(), nullable=False),  # Largest change in power (of a single record) over the cycle
        T.StructField("max_duration__s", dataType=T.DoubleType(), nullable=False),  # Largest change in time (of a single record) over the cycle
        T.StructField("num_records", dataType=T.LongType(), nullable=False),  # Number of records over the cycle
        # Auxiliary metrics
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True),  # First non-null auxiliary entry over the cycle
        T.StructField("metadata", dataType=T.StringType(), nullable=True),  # First non-null metadata entry over the cycle
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
    ]
)  # fmt: skip


def statistics_cycle(df: "DataFrame") -> "DataFrame":
    """Returns cycle-level statistics of the timeseries data.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame with the "statistics_step" schema.

    Returns
    -------
    DataFrame
        Aggregated statistics at the cycle level.

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

    return df.groupBy("device_id", "test_id", "cycle_number").agg(
        # Time
        F.min_by("start_time", "step_number").alias("start_time"),
        F.max_by("end_time", "step_number").alias("end_time"),
        (
            F.max_by("end_time", "step_number").cast("double") - F.min_by("start_time", "step_number").cast("double")
        ).alias("duration__s"),
        # Voltage
        F.min_by("start_voltage__V", "step_number").alias("start_voltage__V"),
        F.max_by("end_voltage__V", "step_number").alias("end_voltage__V"),
        F.min("min_voltage__V").alias("min_voltage__V"),
        F.max("max_voltage__V").alias("max_voltage__V"),
        time_weighted_avg("time_averaged_voltage__V").alias("time_averaged_voltage__V"),
        # Current (keeping sign but min is "smallest", max is "largest")
        F.min_by("start_current__A", "step_number").alias("start_current__A"),
        F.max_by("end_current__A", "step_number").alias("end_current__A"),
        F.min("min_charge_current__A").alias("min_charge_current__A"),
        F.max("min_discharge_current__A").alias("min_discharge_current__A"),
        F.max("max_charge_current__A").alias("max_charge_current__A"),
        F.min("max_discharge_current__A").alias("max_discharge_current__A"),
        time_weighted_avg("time_averaged_current__A").alias("time_averaged_current__A"),
        # Power (keeping sign but min is "smallest", max is "largest")
        F.min_by("start_power__W", "step_number").alias("start_power__W"),
        F.max_by("end_power__W", "step_number").alias("end_power__W"),
        F.min("min_charge_power__W").alias("min_charge_power__W"),
        F.max("min_discharge_power__W").alias("min_discharge_power__W"),
        F.max("max_charge_power__W").alias("max_charge_power__W"),
        F.min("max_discharge_power__W").alias("max_discharge_power__W"),
        time_weighted_avg("time_averaged_power__W").alias("time_averaged_power__W"),
        # Accumulations (within the step, and remember step capacity/energy is unsigned)
        F.sum("charge_capacity__Ah").alias("charge_capacity__Ah"),
        F.sum("discharge_capacity__Ah").alias("discharge_capacity__Ah"),
        F.sum("charge_energy__Wh").alias("charge_energy__Wh"),
        F.sum("discharge_energy__Wh").alias("discharge_energy__Wh"),
        # Resolution diagnostics
        F.max("max_voltage_delta__V").alias("max_voltage_delta__V"),
        F.max("max_current_delta__A").alias("max_current_delta__A"),
        F.max("max_power_delta__W").alias("max_power_delta__W"),
        F.max("max_duration__s").alias("max_duration__s"),
        F.sum("num_records").alias("num_records"),
        # Auxiliary metrics
        F.first("auxiliary", ignorenulls=True).alias("auxiliary"),
        F.first("metadata", ignorenulls=True).alias("metadata"),
        # Metadata
        F.current_timestamp().alias("update_ts"),  # Timestamp in timezone configured in Spark environment
    )
