import pyspark.sql.types as T

statistics_steps_schema = T.StructType(
    [
        T.StructField("device_id", dataType=T.StringType(), nullable=False),
        T.StructField("test_id", dataType=T.LongType(), nullable=True),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),
        T.StructField("step_type", dataType=T.StringType(), nullable=True),
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True),
        T.StructField("step_number", dataType=T.LongType(), nullable=False),
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False),
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False),
        # Current
        T.StructField("start_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("mean_current__A", dataType=T.DoubleType(), nullable=False),
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False),
        # Power
        T.StructField("start_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("mean_power__W", dataType=T.DoubleType(), nullable=False),
        # Other
        T.StructField("capacity__Ah", dataType=T.DoubleType(), nullable=False),
        T.StructField("energy__Wh", dataType=T.DoubleType(), nullable=False),
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
    ]
)
