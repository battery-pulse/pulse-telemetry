import pyspark.sql.types as T

timeseries_schema = T.StructType(
    [
        # Identifiers
        T.StructField("device_id", dataType=T.StringType(), nullable=False),  # ID of the measuring device
        T.StructField(
            "test_id", dataType=T.StringType(), nullable=False
        ),  # ID of the sequence (i.e. test) being measured by the device
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),  # Cycle number within the sequence
        T.StructField("step_number", dataType=T.LongType(), nullable=False),  # Step number within the sequence
        T.StructField("step_type", dataType=T.StringType(), nullable=True),  # Step type as a human-readable string
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True),  # Unique ID for the step type
        T.StructField("record_number", dataType=T.LongType(), nullable=False),  # Record number within the sequence
        # Data
        T.StructField(
            "timestamp", dataType=T.TimestampType(), nullable=False
        ),  # Timestamp when the record was recorded
        T.StructField("date", dataType=T.DateType(), nullable=False),  # Date when the data was recorded, for partions
        T.StructField("current__A", dataType=T.DoubleType(), nullable=False),  # Instantanious current
        T.StructField("voltage__V", dataType=T.DoubleType(), nullable=False),  # Instantanious voltage
        T.StructField("power__W", dataType=T.DoubleType(), nullable=False),  # Instantanious power
        T.StructField(
            "capacity__Ah", dataType=T.DoubleType(), nullable=False
        ),  # Capacity accumulated during this record
        T.StructField("energy__Wh", dataType=T.DoubleType(), nullable=False),  # Energy accumulated during this record
        T.StructField(
            "auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True
        ),  # Auxiliary measurements (e.g. temperature)
        T.StructField("metadata", dataType=T.StringType(), nullable=True),  # JSON string for user-specified fields
        # Metadata
        T.StructField(
            "update_ts", dataType=T.TimestampType(), nullable=False
        ),  # Timestamp when this data was processed by the pulse application
    ]
)
