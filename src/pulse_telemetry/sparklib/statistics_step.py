from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


statistics_step_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False, metadata={"comment": "ID of the measuring device."}),
        T.StructField("test_id", dataType=T.StringType(), nullable=False, metadata={"comment": "ID of the measurement sequence."}),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False, metadata={"comment": "Cycle number within the sequence."}),
        T.StructField("step_number", dataType=T.LongType(), nullable=False, metadata={"comment": "Step number within the sequence."}),
        T.StructField("step_type", dataType=T.StringType(), nullable=True, metadata={"comment": "Step type as a human-readable string."}),
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True, metadata={"comment": "Unique ID for the step type."}),
        # Time
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp at the start of the step."}),
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp at the end of the step."}),
        T.StructField("duration__s", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time over the step in Seconds."}),
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Voltage at the start of the step in Volts."}),
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Voltage at the end of the step in Volts."}),
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Minimum voltage over the step in Volts."}),
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Maximum voltage over the step in Volts."}),
        T.StructField("time_averaged_voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time-averaged voltage over the step in Volts."}),
        # Current (signed, but min means smallest and max largest)
        T.StructField("start_current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Current at the start of the step in Amps."}),
        T.StructField("end_current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Current at the end of the step in Amps."}),
        T.StructField("min_charge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest charge current over the step in Amps."}),
        T.StructField("min_discharge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest discharge current over the step in Amps."}),
        T.StructField("max_charge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest charge current over the step in Amps."}),
        T.StructField("max_discharge_current__A", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest discharge current over the step in Amps."}),
        T.StructField("time_averaged_current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time-averaged current over the step in Amps."}),
        # Power (signed, but min means smallest and max largest)
        T.StructField("start_power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Power at the start of the step in Watts."}),
        T.StructField("end_power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Power at the end of the step in Watts."}),
        T.StructField("min_charge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest charge power over the step in Watts."}),
        T.StructField("min_discharge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Smallest discharge power over the step in Watts."}),
        T.StructField("max_charge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest charge power over the step in Watts."}),
        T.StructField("max_discharge_power__W", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Largest discharge power over the step in Watts."}),
        T.StructField("time_averaged_power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time-averaged power over the step in Watts."}),
        # Accumulations (unsigned)
        T.StructField("charge_capacity__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity charged over the step in Amp-hours."}),
        T.StructField("discharge_capacity__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity discharged over the step in Amp-hours."}),
        T.StructField("charge_energy__Wh", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned energy charged over the step in Watt-hours."}),
        T.StructField("discharge_energy__Wh", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned energy discharged over the step in Watt-hours."}),
        # Resolution diagnostics (unsigned)
        T.StructField("max_voltage_delta__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in voltage (of a single record) over the step in Volts."}),
        T.StructField("max_current_delta__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in current (of a single record) over the step in Amps."}),
        T.StructField("max_power_delta__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in power (of a single record) over the step in Watts."}),
        T.StructField("max_duration__s", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Largest absolute change in time (of a single record) over the step in Seconds."}),
        T.StructField("num_records", dataType=T.LongType(), nullable=False, metadata={"comment": "Number of records over the step."}),
        # Auxiliary metrics
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True, metadata={"comment": "First auxiliary measurements (e.g. temperature.) over the step"}),
        T.StructField("metadata", dataType=T.StringType(), nullable=True, metadata={"comment": "First JSON string for user-specified fields over the step."}),
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp when the row was processed by the pulse telemetry application."}),
    ]
)  # fmt: skip
statistics_step_comment = "Aggregation of battery telemetry at the charge/discharge step level."
statistics_step_composite_key = ["device_id", "test_id", "cycle_number", "step_number"]
statistics_step_partitions = ["year(start_time)"]
statistics_step_write_order = ["device_id", "test_id", "cycle_number", "step_number"]


def statistics_step(df: "DataFrame") -> "DataFrame":
    """Returns step-level statistics of the telemetry data.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame with the "telemetry" schema.

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

    return df.groupBy(*statistics_step_composite_key).agg(
        # Identifiers (groupby columns are already included)
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
        F.max(F.abs("power_delta__W")).alias("max_power_delta__W"),
        F.max("duration__s").alias("max_duration__s"),
        F.count("*").alias("num_records"),
        # Auxiliary metrics
        F.first("auxiliary", ignorenulls=True).alias("auxiliary"),
        F.first("metadata", ignorenulls=True).alias("metadata"),
        # Metadata
        F.current_timestamp().alias("update_ts"),  # Timestamp in timezone configured in Spark environment
    )
