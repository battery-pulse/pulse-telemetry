from json import loads

from pulse_telemetry.sparklib.conversions.maccor_conversion import maccor_to_telemetry
from pulse_telemetry.sparklib.telemetry import telemetry_schema
from pyspark.sql.functions import max, max_by


def test_conversion(maccor_raw_data):
    converted_df = maccor_to_telemetry(maccor_raw_data, telemetry_schema)
    # Schema is enforced in conversion
    assert maccor_raw_data.count() == converted_df.count()
    assert [(x.name, x.dataType) for x in telemetry_schema.fields] == [
        (x.name, x.dataType) for x in converted_df.schema.fields
    ]

    grouped = loads(
        converted_df.groupby("device_id", "test_id")
        .agg(
            max("cycle_number").alias("max_cycle"),
            max("step_number").alias("max_step_num"),
            max_by("step_number", "record_number").alias("last_step_num"),
            max("record_number").alias("max_rec_num"),
        )
        .toJSON()
        .first()
    )
    assert grouped["max_cycle"] == 25
    assert grouped["max_step_num"] == 281
    assert grouped["last_step_num"] == grouped["max_step_num"]
    # Record number 27858 is missing from the CSV
    assert grouped["max_rec_num"] == maccor_raw_data.count() + 1
