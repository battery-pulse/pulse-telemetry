import datetime

from pulse_telemetry.sparklib.statistics_cycle import statistics_cycle, statistics_cycle_schema
from pulse_telemetry.sparklib.statistics_step import statistics_step_schema
from pyspark.sql import Row
from pytest import approx


def test_schema(spark_session, statistics_cycle_df):
    parsed_df = spark_session.createDataFrame(
        statistics_cycle_df.rdd, statistics_cycle_schema
    )  # fix nullability attributes
    assert parsed_df.schema == statistics_cycle_schema
    assert parsed_df.count() == statistics_cycle_df.count(), "Checking that no rows were dropped enforcing schema."


def test_groupby(statistics_cycle_df):
    assert statistics_cycle_df.count() == 10, "Expecting 10 groups = 5ch x 2cycles."
    first_record = statistics_cycle_df.collect()[0]
    assert first_record.cycle_number == 1, "Expecting cycle 1 to be first group."


def test_aggregations(statistics_cycle_df):
    record = statistics_cycle_df.collect()[-1]
    assert (record.end_time - record.start_time).total_seconds() == approx(record.duration__s), "Duration"
    assert record.start_voltage__V == approx(3.0), "Start voltage."  # Voltage
    assert record.end_voltage__V == approx(3.0), "End voltage."
    assert record.min_voltage__V == approx(3.0), "Min voltage."
    assert record.max_voltage__V == approx(4.0), "Max voltage."
    assert record.time_averaged_voltage__V == approx(3.333, abs=0.001), "Mean voltage."
    assert record.start_current__A == approx(0.0), "Start current."  # Current
    assert record.end_current__A == approx(-1.0), "End current."
    assert record.min_charge_current__A == approx(1.0), "Min charge current."
    assert record.min_discharge_current__A == approx(-1.0), "Min discharge current."
    assert record.max_charge_current__A == approx(1.0), "Max charge current."
    assert record.max_discharge_current__A == approx(-1.0), "Max discharge current."
    assert record.time_averaged_current__A == approx(0.0, abs=0.001), "Mean current."
    assert record.start_power__W == approx(0.0), "Start power."  # Power
    assert record.end_power__W == approx(-3.0), "End power."
    assert record.min_charge_power__W == approx(3.2), "Min charge power."
    assert record.min_discharge_power__W == approx(-3.0), "Min discharge power."
    assert record.max_charge_power__W == approx(4.0), "Max charge power."
    assert record.max_discharge_power__W == approx(-3.8), "Max discharge power."
    assert record.time_averaged_power__W == approx(0.066, abs=0.01), "Mean power."
    assert record.charge_capacity__Ah == approx(1.0 * 0.5 / 3600), "Charge capacity."  # Accumulations
    assert record.discharge_capacity__Ah == approx(1.0 * 0.5 / 3600), "Discharge capacity."
    assert record.charge_energy__Wh == approx(3.6 * 0.5 / 3600), "Charge energy."
    assert record.discharge_energy__Wh == approx(3.4 * 0.5 / 3600), "Discharge energy."
    assert record.max_voltage_delta__V == approx(0.2), "Max voltage delta"  # Resolution diagnostics
    assert record.max_current_delta__A == approx(2), "Max current delta"
    assert record.max_power_delta__W == approx(7.8), "Max power delta"
    assert record.max_duration__s == approx(0.1, abs=0.01), "Max duration"
    assert record.num_records == 15, "Num records"
    assert record.auxiliary == {"temperature": 25.0}, "Auxiliary"
    assert record.metadata == '{"experiment": "testing"}', "Metadata"


def test_null_handling(spark_session):
    # Define nullable fields
    nullable_fields = [field.name for field in statistics_cycle_schema.fields if field.nullable]

    # Validates all null incoming rows
    nulled_statistics_step = spark_session.createDataFrame(
        [
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type=None,
                step_id=None,
                start_time=datetime.datetime.now(datetime.timezone.utc),
                end_time=datetime.datetime.now(datetime.timezone.utc),
                duration__s=0.0,
                start_voltage__V=0.0,
                end_voltage__V=0.0,
                min_voltage__V=0.0,
                max_voltage__V=0.0,
                time_averaged_voltage__V=0.0,
                start_current__A=0.0,
                end_current__A=0.0,
                min_charge_current__A=None,
                min_discharge_current__A=None,
                max_charge_current__A=None,
                max_discharge_current__A=None,
                time_averaged_current__A=0.0,
                start_power__W=0.0,
                end_power__W=0.0,
                min_charge_power__W=None,
                min_discharge_power__W=None,
                max_charge_power__W=None,
                max_discharge_power__W=None,
                time_averaged_power__W=0.0,
                charge_capacity__Ah=0.0,
                discharge_capacity__Ah=0.0,
                charge_energy__Wh=0.0,
                discharge_energy__Wh=0.0,
                max_voltage_delta__V=0.0,
                max_current_delta__A=0.0,
                max_power_delta__W=0.0,
                max_duration__s=0.0,
                num_records=0,
                auxiliary=None,
                metadata=None,
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
        ],
        schema=statistics_step_schema,
    )
    nulled_statistics_cycle_df = statistics_cycle(nulled_statistics_step)
    record = nulled_statistics_cycle_df.collect()[0]
    for field in nullable_fields:
        assert getattr(record, field) is None, f"{field} should be null if not present in input."

    # Validates mixed nulls
    nulled_statistics_step = spark_session.createDataFrame(
        [
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type=None,
                step_id=None,
                start_time=datetime.datetime.now(datetime.timezone.utc),
                end_time=datetime.datetime.now(datetime.timezone.utc),
                duration__s=0.0,
                start_voltage__V=0.0,
                end_voltage__V=0.0,
                min_voltage__V=0.0,
                max_voltage__V=0.0,
                time_averaged_voltage__V=0.0,
                start_current__A=0.0,
                end_current__A=0.0,
                min_charge_current__A=None,
                min_discharge_current__A=None,
                max_charge_current__A=None,
                max_discharge_current__A=None,
                time_averaged_current__A=0.0,
                start_power__W=0.0,
                end_power__W=0.0,
                min_charge_power__W=None,
                min_discharge_power__W=None,
                max_charge_power__W=None,
                max_discharge_power__W=None,
                time_averaged_power__W=0.0,
                charge_capacity__Ah=0.0,
                discharge_capacity__Ah=0.0,
                charge_energy__Wh=0.0,
                discharge_energy__Wh=0.0,
                max_voltage_delta__V=0.0,
                max_current_delta__A=0.0,
                max_power_delta__W=0.0,
                max_duration__s=0.0,
                num_records=0,
                auxiliary=None,
                metadata=None,
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=2,
                step_type="Charge",
                step_id=1,
                start_time=datetime.datetime.now(datetime.timezone.utc),
                end_time=datetime.datetime.now(datetime.timezone.utc),
                duration__s=10.0,
                start_voltage__V=3.6,
                end_voltage__V=4.2,
                min_voltage__V=3.6,
                max_voltage__V=4.2,
                time_averaged_voltage__V=3.9,
                start_current__A=0.5,
                end_current__A=0.0,
                min_charge_current__A=0.5,
                min_discharge_current__A=0.0,
                max_charge_current__A=1.0,
                max_discharge_current__A=0.0,
                time_averaged_current__A=0.25,
                start_power__W=1.8,
                end_power__W=0.0,
                min_charge_power__W=1.8,
                min_discharge_power__W=0.0,
                max_charge_power__W=2.0,
                max_discharge_power__W=0.0,
                time_averaged_power__W=1.0,
                charge_capacity__Ah=0.05,
                discharge_capacity__Ah=0.0,
                charge_energy__Wh=0.2,
                discharge_energy__Wh=0.0,
                max_voltage_delta__V=0.1,
                max_current_delta__A=0.02,
                max_power_delta__W=0.02,
                max_duration__s=1.0,
                num_records=100,
                auxiliary={"temperature": 25.0},
                metadata="{'key': 'value'}",
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
        ],
        schema=statistics_step_schema,
    )
    nulled_statistics_cycle_df = statistics_cycle(nulled_statistics_step)
    record = nulled_statistics_cycle_df.collect()[0]
    for field in nullable_fields:
        assert getattr(record, field) is not None, f"{field} should not be null."


def test_empty_df(spark_session):
    empty_statistics_step = spark_session.createDataFrame([], schema=statistics_step_schema)
    empty_statistics_cycle = statistics_cycle(empty_statistics_step)
    assert empty_statistics_cycle.count() == 0, "The output DataFrame should be empty for empty input."
