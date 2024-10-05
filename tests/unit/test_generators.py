from pulse_telemetry.sparklib.transformations.timeseries import timeseries_schema


def test_timeseries_generator(spark_session, timeseries_df):
    # Schema validation
    parsed_df = spark_session.createDataFrame(timeseries_df.rdd, timeseries_schema)  # fix nullability attributes
    assert parsed_df.schema == timeseries_schema
    assert parsed_df.count() == timeseries_df.count(), "Checking that no rows were dropped enforcing schema."

    # Sequence validation
    timeseries_df = timeseries_df.toPandas()
    timeseries_df.to_csv("ts.csv")
    assert len(timeseries_df) == 150, "Should cover 150 rows = 10Hz x 3s x 5ch"
    assert max(timeseries_df["cycle_number"]) == 2, "Shold cover 2 cycles"
    assert max(timeseries_df["step_number"]) == 6, "Should cover 6 steps"
