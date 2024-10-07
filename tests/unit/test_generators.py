from pulse_telemetry.sparklib.transformations.telemetry import telemetry_schema


def test_telemetry_generator(spark_session, telemetry_df):
    # Schema validation
    parsed_df = spark_session.createDataFrame(telemetry_df.rdd, telemetry_schema)  # fix nullability attributes
    assert parsed_df.schema == telemetry_schema
    assert parsed_df.count() == telemetry_df.count(), "Checking that no rows were dropped enforcing schema."

    # Sequence validation
    telemetry_df = telemetry_df.toPandas()
    assert len(telemetry_df) == 150, "Should cover 150 rows = 10Hz x 3s x 5ch"
    assert max(telemetry_df["cycle_number"]) == 2, "Shold cover 2 cycles"
    assert max(telemetry_df["step_number"]) == 6, "Should cover 6 steps"
