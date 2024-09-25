import pyspark.sql.types as T

timeseries_schema = T.StructType(
    [
        T.StructField("device_id", dataType=T.StringType(), nullable=False),
        T.StructField("test_id", dataType=T.LongType(), nullable=True),
        T.StructField("sequence_number", dataType=T.LongType(), nullable=False),
        T.StructField("step_number", dataType=T.LongType(), nullable=False),
        T.StructField("step_id", dataType=T.IntegerType(), nullable=True),
        T.StructField("cycle_number", dataType=T.IntegerType(), nullable=False),
        T.StructField("timestamp", dataType=T.TimestampType(), nullable=False),
        T.StructField("current__A", dataType=T.DoubleType(), nullable=False),
        T.StructField("voltage__V", dataType=T.DoubleType(), nullable=False),
        T.StructField("power__W", dataType=T.DoubleType(), nullable=False),
        T.StructField("capacity__Ah", dataType=T.DoubleType(), nullable=False),
        T.StructField("energy__Wh", dataType=T.DoubleType(), nullable=False),
        T.StructField("auxiliary", dataType=T.MapType(T.StringType(), T.DoubleType()), nullable=True),
        T.StructField("update_ts", dataType=T.TimestampType(), nullable=False),
    ]
)
