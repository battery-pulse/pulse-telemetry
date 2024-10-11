import datetime

from pulse_telemetry.sparklib.transformations.statistics_step import statistics_step, statistics_step_schema
from pulse_telemetry.sparklib.transformations.telemetry import telemetry_schema
from pyspark.sql import Row
from pytest import approx


def test_schema(spark_session, statistics_step_df):
    parsed_df = spark_session.createDataFrame(
        statistics_step_df.rdd, statistics_step_schema
    )  # fix nullability attributes
    assert parsed_df.schema == statistics_step_schema
    assert parsed_df.count() == statistics_step_df.count(), "Checking that no rows were dropped enforcing schema."


def test_groupby(statistics_step_df):
    assert statistics_step_df.count() == 30, "Expecting 30 groups = 5ch x 6steps."
    first_record = statistics_step_df.collect()[0]
    assert first_record.cycle_number == 1, "Expecting cycle 1 to be first group."
    assert first_record.step_number == 1, "Expecting step 1 to be first group."
    assert first_record.step_type == "Rest", "Expecting rest to be first group."
    assert first_record.step_id == 0, "Expecting step-id 0 to be first group."


def test_aggregations(statistics_step_df):
    # Validates discharge step
    discharge_record = statistics_step_df.collect()[-1]
    assert (discharge_record.end_time - discharge_record.start_time).total_seconds() == approx(
        discharge_record.duration__s
    ), "Duration - discharge"
    assert discharge_record.start_voltage__V == approx(3.8), "Start voltage - discharge."  # Voltage
    assert discharge_record.end_voltage__V == approx(3.0), "End voltage - discharge."
    assert discharge_record.min_voltage__V == approx(3.0), "Min voltage - discharge."
    assert discharge_record.max_voltage__V == approx(3.8), "Max voltage - discharge."
    assert discharge_record.time_averaged_voltage__V == approx(3.4, abs=0.001), "Mean voltage - discharge."
    assert discharge_record.start_current__A == approx(-1.0), "Start current - discharge."  # Current
    assert discharge_record.end_current__A == approx(-1.0), "End current - discharge."
    assert discharge_record.min_charge_current__A is None, "Min charge current - discharge."
    assert discharge_record.min_discharge_current__A == approx(-1.0), "Min discharge current - discharge."
    assert discharge_record.max_charge_current__A is None, "Max charge current - discharge."
    assert discharge_record.max_discharge_current__A == approx(-1.0), "Max discharge current - discharge."
    assert discharge_record.time_averaged_current__A == approx(-1.0), "Mean current - discharge."
    assert discharge_record.start_power__W == approx(-3.8), "Start power - discharge."  # Power
    assert discharge_record.end_power__W == approx(-3.0), "End power - discharge."
    assert discharge_record.min_charge_power__W is None, "Min charge power - discharge."
    assert discharge_record.min_discharge_power__W == approx(-3.0), "Min discharge power - discharge."
    assert discharge_record.max_charge_power__W is None, "Max charge power - discharge."
    assert discharge_record.max_discharge_power__W == approx(-3.8), "Max discharge power - discharge."
    assert discharge_record.time_averaged_power__W == approx(-3.4, abs=0.001), "Mean power - discharge."
    assert discharge_record.charge_capacity__Ah == approx(
        0.0, abs=0.001
    ), "Charge capacity - discharge."  # Accumulations
    assert discharge_record.discharge_capacity__Ah == approx(1.0 * 0.5 / 3600), "Discharge capacity - discharge."
    assert discharge_record.charge_energy__Wh == approx(0.0, abs=0.001), "Charge energy - discharge."
    assert discharge_record.discharge_energy__Wh == approx(3.4 * 0.5 / 3600), "Discharge energy - discharge."
    assert discharge_record.max_voltage_delta__V == approx(
        0.2
    ), "Max voltage delta - discharge"  # Resolution diagnostics
    assert discharge_record.max_current_delta__A == approx(2), "Max current delta - discharge"
    assert discharge_record.max_power_delta__W == approx(7.8), "Max power delta - discharge"
    assert discharge_record.max_duration__s == approx(0.1, abs=0.01), "Max duration - discharge"
    assert discharge_record.num_records == 5, "Num records - discharge"
    assert discharge_record.auxiliary == {"temperature": 25.0}, "Auxiliary - discharge"
    assert discharge_record.metadata == '{"experiment": "testing"}', "Metadata - discharge"

    # Validates charge step
    charge_record = statistics_step_df.collect()[-2]
    assert (charge_record.end_time - charge_record.start_time).total_seconds() == approx(
        charge_record.duration__s
    ), "Duration - charge"
    assert charge_record.start_voltage__V == approx(3.2), "Start voltage - charge."  # Voltage
    assert charge_record.end_voltage__V == approx(4.0), "End voltage - charge."
    assert charge_record.min_voltage__V == approx(3.2), "Min voltage - charge."
    assert charge_record.max_voltage__V == approx(4.0), "Max voltage - charge."
    assert charge_record.time_averaged_voltage__V == approx(3.6, abs=0.001), "Mean voltage - charge."
    assert charge_record.start_current__A == approx(1.0), "Start current - charge."  # Current
    assert charge_record.end_current__A == approx(1.0), "End current - charge."
    assert charge_record.min_charge_current__A == approx(1.0), "Min charge current - charge."
    assert charge_record.min_discharge_current__A is None, "Min discharge current - charge."
    assert charge_record.max_charge_current__A == approx(1.0), "Max charge current - charge."
    assert charge_record.max_discharge_current__A is None, "Max discharge current - charge."
    assert charge_record.time_averaged_current__A == approx(1.0), "Mean current - charge."
    assert charge_record.start_power__W == approx(3.2), "Start power - charge."  # Power
    assert charge_record.end_power__W == approx(4.0), "End power - charge."
    assert charge_record.min_charge_power__W == approx(3.2), "Min charge power - charge."
    assert charge_record.min_discharge_power__W is None, "Min discharge power - charge."
    assert charge_record.max_charge_power__W == approx(4.0), "Max charge power - charge."
    assert charge_record.max_discharge_power__W is None, "Max discharge power - charge."
    assert charge_record.time_averaged_power__W == approx(3.6, abs=0.001), "Mean power - charge."
    assert charge_record.charge_capacity__Ah == approx(1.0 * 0.5 / 3600), "Charge capacity - charge."  # Accumulations
    assert charge_record.discharge_capacity__Ah == approx(0.0, abs=0.001), "Discharge capacity - charge."
    assert charge_record.charge_energy__Wh == approx(3.6 * 0.5 / 3600), "Charge energy - charge."
    assert charge_record.discharge_energy__Wh == approx(0.0, abs=0.001), "Discharge energy - charge."
    assert charge_record.max_voltage_delta__V == approx(0.2), "Max voltage delta - charge"  # Resolution diagnostics
    assert charge_record.max_current_delta__A == approx(1), "Max current delta - charge"
    assert charge_record.max_power_delta__W == approx(3.2), "Max power delta - charge"
    assert charge_record.max_duration__s == approx(0.1, abs=0.01), "Max duration - charge"
    assert charge_record.num_records == 5, "Num records - charge"
    assert charge_record.auxiliary == {"temperature": 25.0}, "Auxiliary - charge"
    assert charge_record.metadata == '{"experiment": "testing"}', "Metadata - charge"

    # Validates rest step
    rest_record = statistics_step_df.collect()[-3]
    assert (rest_record.end_time - rest_record.start_time).total_seconds() == approx(
        rest_record.duration__s
    ), "Duration - rest"
    assert rest_record.start_voltage__V == approx(3.0), "Start voltage - rest."  # Voltage
    assert rest_record.end_voltage__V == approx(3.0), "End voltage - rest."
    assert rest_record.min_voltage__V == approx(3.0), "Min voltage - rest."
    assert rest_record.max_voltage__V == approx(3.0), "Max voltage - rest."
    assert rest_record.time_averaged_voltage__V == approx(3.0), "Mean voltage - rest."
    assert rest_record.start_current__A == approx(0.0), "Start current - rest."  # Current
    assert rest_record.end_current__A == approx(0.0), "End current - rest."
    assert rest_record.min_charge_current__A is None, "Min charge current - rest."
    assert rest_record.min_discharge_current__A is None, "Min discharge current - rest."
    assert rest_record.max_charge_current__A is None, "Max charge current - rest."
    assert rest_record.max_discharge_current__A is None, "Max discharge current - rest."
    assert rest_record.time_averaged_current__A == 0.0, "Mean current - rest."
    assert rest_record.start_power__W == approx(0.0), "Start power - rest."  # Power
    assert rest_record.end_power__W == approx(0.0), "End power - rest."
    assert rest_record.min_charge_power__W is None, "Min charge power - rest."
    assert rest_record.min_discharge_power__W is None, "Min discharge power - rest."
    assert rest_record.max_charge_power__W is None, "Max charge power - rest."
    assert rest_record.max_discharge_power__W is None, "Max discharge power - rest."
    assert rest_record.time_averaged_power__W == 0.0, "Mean power - rest."
    assert rest_record.charge_capacity__Ah == approx(0.0), "Charge capacity - rest."  # Accumulations
    assert rest_record.discharge_capacity__Ah == approx(0.0), "Discharge capacity - rest."
    assert rest_record.charge_energy__Wh == approx(0.0), "Charge energy - rest."
    assert rest_record.discharge_energy__Wh == approx(0.0), "Discharge energy - rest."
    assert rest_record.max_voltage_delta__V == approx(0.0), "Max voltage delta - rest"  # Resolution diagnostics
    assert rest_record.max_current_delta__A == approx(1), "Max current delta - rest"
    assert rest_record.max_power_delta__W == approx(3.0), "Max power delta - rest"
    assert rest_record.max_duration__s == approx(0.1, abs=0.01), "Max duration - rest"
    assert rest_record.num_records == 5, "Num records - rest"
    assert rest_record.auxiliary == {"temperature": 25.0}, "Auxiliary - rest"
    assert rest_record.metadata == '{"experiment": "testing"}', "Metadata - rest"


def test_null_handling(spark_session):
    # Define nullable fields
    nullable_fields = [field.name for field in statistics_step_schema.fields if field.nullable]
    # Validates all null rows
    nulled_telemetry = spark_session.createDataFrame(
        [
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type=None,
                step_id=None,
                record_number=1,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                voltage__V=0.0,
                current__A=0.0,
                power__W=0.0,
                duration__s=0.0,
                voltage_delta__V=0.0,
                current_delta__A=0.0,
                power_delta__W=0.0,
                capacity_charged__Ah=0.0,
                capacity_discharged__Ah=0.0,
                differential_capacity_charged__Ah_V=None,
                differential_capacity_discharged__Ah_V=None,
                step_duration__s=0.0,
                step_capacity_charged__Ah=0.0,
                step_capacity_discharged__Ah=0.0,
                step_energy_charged__Wh=0.0,
                step_energy_discharged__Wh=0.0,
                auxiliary=None,
                metadata=None,
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
        ],
        schema=telemetry_schema,
    )
    nulled_statistics_step_df = statistics_step(nulled_telemetry)
    record = nulled_statistics_step_df.collect()[0]
    for field in nullable_fields:
        assert getattr(record, field) is None, f"{field} should be null if not present in input."

    # Validates mixed nulls
    nulled_telemetry = spark_session.createDataFrame(
        [
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type=None,
                step_id=None,
                record_number=1,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                voltage__V=0.0,
                current__A=0.0,
                power__W=0.0,
                duration__s=0.0,
                voltage_delta__V=0.0,
                current_delta__A=0.0,
                power_delta__W=0.0,
                capacity_charged__Ah=0.0,
                capacity_discharged__Ah=0.0,
                differential_capacity_charged__Ah_V=None,
                differential_capacity_discharged__Ah_V=None,
                step_duration__s=0.0,
                step_capacity_charged__Ah=0.0,
                step_capacity_discharged__Ah=0.0,
                step_energy_charged__Wh=0.0,
                step_energy_discharged__Wh=0.0,
                auxiliary=None,
                metadata=None,
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type="Charge",
                step_id=1,
                record_number=2,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                voltage__V=0.0,
                current__A=1.0,
                power__W=1.0,
                duration__s=0.0,
                voltage_delta__V=0.0,
                current_delta__A=0.0,
                power_delta__W=0.0,
                capacity_charged__Ah=0.0,
                capacity_discharged__Ah=0.0,
                differential_capacity_charged__Ah_V=1.0,
                differential_capacity_discharged__Ah_V=None,
                step_duration__s=0.0,
                step_capacity_charged__Ah=0.0,
                step_capacity_discharged__Ah=0.0,
                step_energy_charged__Wh=0.0,
                step_energy_discharged__Wh=0.0,
                auxiliary={"Temperature": 25.0},
                metadata='{"cool": "wow"}',
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type="Charge",
                step_id=1,
                record_number=3,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                voltage__V=0.0,
                current__A=-1.0,
                power__W=-1.0,
                duration__s=0.0,
                voltage_delta__V=0.0,
                current_delta__A=0.0,
                power_delta__W=0.0,
                capacity_charged__Ah=0.0,
                capacity_discharged__Ah=0.0,
                differential_capacity_charged__Ah_V=None,
                differential_capacity_discharged__Ah_V=-1.0,
                step_duration__s=0.0,
                step_capacity_charged__Ah=0.0,
                step_capacity_discharged__Ah=0.0,
                step_energy_charged__Wh=0.0,
                step_energy_discharged__Wh=0.0,
                auxiliary={"Temperature": 25.0},
                metadata='{"cool": "wow"}',
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
        ],
        schema=telemetry_schema,
    )
    nulled_statistics_step_df = statistics_step(nulled_telemetry)
    record = nulled_statistics_step_df.collect()[0]
    for field in nullable_fields:
        assert getattr(record, field) is not None, f"{field} should not be null."


def test_empty_df(spark_session):
    empty_telemetry = spark_session.createDataFrame([], schema=telemetry_schema)
    empty_statistics_step = statistics_step(empty_telemetry)
    assert empty_statistics_step.count() == 0, "The output DataFrame should be empty for empty input."
