from itertools import chain
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.window import WindowSpec


def _differential(col: str, window: "WindowSpec", multiplier: float = 1.0) -> "Column":
    return F.col(col) * F.lit(multiplier) - F.lag(col).over(window) * F.lit(multiplier)


def maccor_to_telemetry(raw_df: "DataFrame", schema: T.StructType) -> "DataFrame":
    """Converts Maccor raw schema to telemetry schema. Assumes columns "device_id" and "test_id".
    User should extract these values from the file name with a given pattern.
    """
    cols = raw_df.columns
    var_cols = [F.lit("Maccor").alias("cycler_type")] + [i for i in cols if "var" in i.lower()]  # Maccor variables
    aux_cols = [i for i in cols if "auxiliary" in i.lower()]  # Aux temperatures
    aux_cols_for_map = list(chain(*[(F.lit(c), F.col(c).cast(T.DoubleType())) for c in aux_cols]))
    state_col = next(i for i in cols if "state" in i.lower())
    full_test_window = W.partitionBy("device_id", "test_id").orderBy("Datapoint Number")
    cumsum_window = full_test_window.rowsBetween(W.unboundedPreceding, W.currentRow)
    step_window = W.partitionBy("device_id", "test_id", "step_number").orderBy("record_number")
    renamed = raw_df.repartition("device_id", "test_id").select(
        # Identifiers
        F.col("device_id"),
        F.col("test_id"),
        F.col("Cycle Number").alias("cycle_number"),
        F.col(state_col).alias("step_type"),
        F.sum((F.col("Step Index") != F.lag("Step Index", default=0).over(full_test_window)).cast(T.IntegerType()))
        .over(cumsum_window)
        .alias("step_number"),
        F.col("Step Index").alias("step_id"),
        F.col("Datapoint Number").alias("record_number"),
        # Instantaneous quantities
        F.timestamp_millis(F.col("Timestamp")).alias("timestamp"),
        F.col("Potential (V)").alias("voltage__V"),
        F.col("Current (A)").alias("current__A"),
        F.col("Power (W)").alias("power__W"),
        # Differential Quantities
        F.coalesce(_differential("Timestamp", full_test_window, 0.001), F.lit(0.0)).alias("duration__s"),
        _differential("Potential (V)", full_test_window).alias("voltage_delta__V"),
        _differential("Current (A)", full_test_window).alias("current_delta__A"),
        _differential("Power (W)", full_test_window).alias("power_delta__W"),
        # Note: Using "least" for when value resets to 0 (in that case, the abs delta will be large and we should just
        # take the value from the column itself).
        F.least(F.col("Charge Capacity (Ah)"), F.abs(_differential("Charge Capacity (Ah)", full_test_window))).alias(
            "capacity_charged__Ah"
        ),
        F.least(
            F.col("Discharge Capacity (Ah)"), F.abs(_differential("Discharge Capacity (Ah)", full_test_window))
        ).alias("capacity_discharged__Ah"),
        # Accumulated Quantities
        F.col("Step Time (s)").alias("step_duration__s"),
        # Additional fields
        F.create_map(*aux_cols_for_map).alias("auxiliary"),
        F.to_json(F.struct(*var_cols)).alias("metadata"),
        # Metadata
        F.current_timestamp().alias("update_ts"),
        # Carryover for step calculations
        F.col("Charge Capacity (Ah)"),
        F.col("Charge Energy (Wh)"),
        F.col("Discharge Capacity (Ah)"),
        F.col("Discharge Energy (Wh)"),
    )
    return renamed.withColumns(
        {
            "step_capacity_charged__Ah": F.col("Charge Capacity (Ah)")
            - F.first("Charge Capacity (Ah)").over(step_window),
            "step_capacity_discharged__Ah": F.col("Discharge Capacity (Ah)")
            - F.first("Discharge Capacity (Ah)").over(step_window),
            "step_energy_charged__Wh": F.col("Charge Energy (Wh)") - F.first("Charge Energy (Wh)").over(step_window),
            "step_energy_discharged__Wh": F.col("Discharge Energy (Wh)")
            - F.first("Discharge Energy (Wh)").over(step_window),
            "differential_capacity_charged__Ah_V": F.col("capacity_charged__Ah") / F.col("voltage_delta__V"),
            "differential_capacity_discharged__Ah_V": F.col("capacity_discharged__Ah") / F.col("voltage_delta__V"),
        }
    ).select(*[F.col(i.name).cast(i.dataType) for i in schema])
