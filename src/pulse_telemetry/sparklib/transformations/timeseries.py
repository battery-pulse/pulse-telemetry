import pyspark.sql.types as T

timeseries_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False),  # ID of the measuring device
        T.StructField("test_id", dataType=T.StringType(), nullable=False),  # ID of the measured sequence
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),  # Cycle number within the sequence
        T.StructField("step_number", dataType=T.LongType(), nullable=False),  # Step number within the sequence
        T.StructField("step_type", dataType=T.StringType(), nullable=True),  # Step type as a human-readable string
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True),  # Unique ID for the step type
        T.StructField("record_number", dataType=T.LongType(), nullable=False),  # Record number within the sequence
        # Instantanious quantities
        T.StructField("timestamp", dataType=T.TimestampType(), nullable=False),  # Timestamp when the record was recorded
        T.StructField("date", dataType=T.DateType(), nullable=False),  # Date when the data was recorded, for partions
        T.StructField("voltage__V", dataType=T.DoubleType(), nullable=False),  # Instantanious voltage
        T.StructField("current__A", dataType=T.DoubleType(), nullable=False),  # Signed instantanious current, negative is discharge
        T.StructField("power__W", dataType=T.DoubleType(), nullable=False),  # Signed instantanious power
        # Differential quantities
        T.StructField("duration__s", dataType=T.DoubleType(), nullable=False),  # Time change from the previous record
        T.StructField("voltage_delta__V", dataType=T.DoubleType(), nullable=False),  # Change in voltage from the previous record
        T.StructField("current_delta__A", dataType=T.DoubleType(), nullable=False),  # Change in current from the previous record
        T.StructField("capacity_charged__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity accumulated from the previous record under positive current conditions
        T.StructField("capacity_discharged__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity accumulated from the previous record under negative current conditions
        T.StructField("differential_capacity_charged__Ah_V", dataType=T.DoubleType(), nullable=True),  # Signed dQ/dV for charge
        T.StructField("differential_capacity_discharged__Ah_V", dataType=T.DoubleType(), nullable=True),  # Signed dQ/dV for discharge
        # Acumulated quantities
        T.StructField("step_capacity_charged__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity accumulated during the step under positive current conditions
        T.StructField("step_capacity_discharged__Ah", dataType=T.DoubleType(), nullable=False),  # Unsigned capacity accumulated during the step under negative current conditions
        T.StructField("step_energy_charged__Wh", dataType=T.DoubleType(), nullable=False),  # Unsigned energy accumulated during this step under positive current conditions
        T.StructField("step_energy_discharged__Wh", dataType=T.DoubleType(), nullable=False),  # Unsigned energy accumulated during this step under negative current conditions
        # Additional fields
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True),  # Auxiliary measurements (e.g. temperature)
        T.StructField("metadata", dataType=T.StringType(), nullable=True),  # JSON string for user-specified fields
        # Metadata
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),  # Timestamp when this data was processed by the pulse application
    ]
)  # fmt: skip
