from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


statistics_cycle_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False, metadata={"comment": "ID of the measuring device."}),
        T.StructField("test_id", dataType=T.StringType(), nullable=False, metadata={"comment": "ID of the measurement sequence."}),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False, metadata={"comment": "Cycle number within the sequence."}),
        # Time
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp at the start of the cycle."}),
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp at the end of the cycle."}),
        T.StructField("duration__s", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time over the cycle in Seconds."}),
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Voltage at the start of the cycle in Volts."}),
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Voltage at the end of the cycle in Volts."}),
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Minimum voltage over the cycle in Volts."}),
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Maximum voltage over the cycle in Volts."}),
        T.StructField("time_averaged_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time-averaged voltage over the cycle in Volts."}),
        # Current (signed, but min means smallest and max largest)
        T.StructField("start_current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Current at the start of the cycle in Amps."}),
        T.StructField("end_current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Current at the end of the cycle in Amps."}),
        T.StructField("min_charge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest charge current over the cycle in Amps."}),
        T.StructField("min_discharge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest discharge current over the cycle in Amps."}),
        T.StructField("max_charge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest charge current over the cycle in Amps."}),
        T.StructField("max_discharge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest discharge current over the cycle in Amps."}),
        T.StructField("time_averaged_current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time-averaged current over the cycle in Amps."}),
        # Power (signed, but min means smallest and max largest)
        T.StructField("start_power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Power at the start of the cycle in Watts."}),
        T.StructField("end_power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Power at the end of the cycle in Watts."}),
        T.StructField("min_charge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest charge power over the cycle in Watts."}),
        T.StructField("min_discharge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest discharge power over the cycle in Watts."}),
        T.StructField("max_charge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest charge power over the cycle in Watts."}),
        T.StructField("max_discharge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest discharge power over the cycle in Watts."}),
        T.StructField("time_averaged_power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time-averaged power over the cycle in Watts."}),
        # Accumulations (unsigned)
        T.StructField("charge_capacity__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity charged over the cycle in Amp-hours."}),
        T.StructField("discharge_capacity__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity discharged over the cycle in Amp-hours."}),
        T.StructField("charge_energy__Wh", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned energy charged over the cycle in Watt-hours."}),
        T.StructField("discharge_energy__Wh", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned energy discharged over the cycle in Watt-hours."}),
        # Resolution diagnostics (unsigned)
        T.StructField("max_voltage_delta__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in voltage (of a single record) over the cycle in Volts."}),
        T.StructField("max_current_delta__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in current (of a single record) over the cycle in Amps."}),
        T.StructField("max_power_delta__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in power (of a single record) over the cycle in Watts."}),
        T.StructField("max_duration__s", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in time (of a single record) over the cycle in Seconds."}),
        T.StructField("num_records", dataType=T.LongType(), nullable=False, metadata={"comment": "Number of records over the cycle."}),
        # Auxiliary metrics
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True, metadata={"comment": "First auxiliary measurements (e.g. temperature.) over the cycle"}),
        T.StructField("metadata", dataType=T.StringType(), nullable=True, metadata={"comment": "First JSON string for user-specified fields over the cycle."}),
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp when the row was processed by the pulse telemetry application."}),
    ]
)  # fmt: skip
statistics_cycle_comment = "Aggregation of battery telemetry at the cycle level."
statistics_cycle_composite_key = ["device_id", "test_id", "cycle_number"]
statistics_cycle_partitions = ["year(start_time)"]
statistics_cycle_write_order = ["device_id", "test_id", "cycle_number"]


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

    return df.groupBy(*statistics_cycle_composite_key).agg(
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
