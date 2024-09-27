import pyspark.sql.types as T

statistics_cycle_schema = T.StructType(
    [
        T.StructField("device_id", dataType=T.StringType(), nullable=False),
        T.StructField("test_id", dataType=T.StringType(), nullable=False),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),
        T.StructField("start_time", dataType=T.TimestampType(), nullable=False),
        T.StructField("end_time", dataType=T.TimestampType(), nullable=False),
        # Current
        T.StructField("min_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_abs_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_abs_current__A", dataType=T.DoubleType(), nullable=False),
        # Voltage
        T.StructField("start_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("end_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_voltage__V", dataType=T.DoubleType(), nullable=False),
        # Power
        T.StructField("min_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("min_abs_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("max_abs_power__W", dataType=T.DoubleType(), nullable=False),
        # Other
        T.StructField("charge_capacity__Ah", dataType=T.DoubleType(), nullable=False),
        T.StructField("discharge_capacity__Ah", dataType=T.DoubleType(), nullable=False),
        T.StructField("charge_energy__Wh", dataType=T.DoubleType(), nullable=False),
        T.StructField("discharge_energy__Wh", dataType=T.DoubleType(), nullable=False),
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
    ]
)
