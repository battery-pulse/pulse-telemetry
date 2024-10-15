import pyspark.sql.types as T

telemetry_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False, metadata={"comment": "ID of the measuring device."}),
        T.StructField("test_id", dataType=T.StringType(), nullable=False, metadata={"comment": "ID of the measurement sequence."}),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False, metadata={"comment": "Cycle number within the sequence."}),
        T.StructField("step_number", dataType=T.LongType(), nullable=False, metadata={"comment": "Step number within the sequence."}),
        T.StructField("step_type", dataType=T.StringType(), nullable=True, metadata={"comment": "Step type as a human-readable string."}),
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True, metadata={"comment": "Unique ID for the step type."}),
        T.StructField("record_number", dataType=T.LongType(), nullable=False, metadata={"comment": "Record number within the sequence."}),
        # Instantaneous quantities
        T.StructField("timestamp", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp when the record was recorded in UTC."}),
        T.StructField("voltage__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Instantaneous voltage in Volts."}),
        T.StructField("current__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Instantaneous current in Amps. Negative is discharge."}),
        T.StructField("power__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Instantaneous power in Watts. Negative is discharge."}),
        # Differential quantities
        T.StructField("duration__s", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time change from the previous record in Seconds."}),
        T.StructField("voltage_delta__V", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Change in voltage from the previous record in Volts."}),
        T.StructField("current_delta__A", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Change in current from the previous record in Amps."}),
        T.StructField("power_delta__W", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Change in power from the previous record in Watts."}),
        T.StructField("capacity_charged__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity (in Amp-hours) accumulated since the previous record under charge conditions."}),
        T.StructField("capacity_discharged__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity (in Amp-hours) accumulated since the previous record under discharge conditions."}),
        T.StructField("differential_capacity_charged__Ah_V", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Differential capacity (in Amp-hours per Volt) derived from the charged capacity."}),
        T.StructField("differential_capacity_discharged__Ah_V", dataType=T.DoubleType(), nullable=True, metadata={"comment": "Differential capacity (in Amp-hours per Volt) from the discharged capacity."}),
        # Accumulated quantities
        T.StructField("step_duration__s", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Time accumulated up until this point within the step in Seconds."}),
        T.StructField("step_capacity_charged__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity (in Amp-hours) accumulated up until this point within the step under charge conditions."}),
        T.StructField("step_capacity_discharged__Ah", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned capacity (in Amp-hours) accumulated up until this point within the step under discharge conditions."}),
        T.StructField("step_energy_charged__Wh", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned energy (in Watt-hours) accumulated up until this point within the step under charge conditions."}),
        T.StructField("step_energy_discharged__Wh", dataType=T.DoubleType(), nullable=False, metadata={"comment": "Unsigned energy (in Watt-hours) accumulated up until this point within the step under discharge conditions."}),
        # Additional fields
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True, metadata={"comment": "Auxiliary measurements (e.g. temperature.)"}),
        T.StructField("metadata", dataType=T.StringType(), nullable=True, metadata={"comment": "JSON string for user-specified fields."}),
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False, metadata={"comment": "Timestamp when the row was processed by the pulse telemetry application."}),
    ]
)  # fmt: skip
telemetry_comment = "Enriched individual telemetry records from the battery."
telemetry_composite_key = ["device_id", "test_id", "cycle_number", "step_number", "record_number"]
telemetry_partitions = ["device_id", "test_id", "month(timestamp)"]
telemetry_write_order = ["cycle_number", "step_number", "record_number"]
