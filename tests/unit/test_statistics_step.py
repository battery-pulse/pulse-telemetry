import datetime

from pulse_telemetry.sparklib.transformations.statistics_step import statistics_step, statistics_step_schema
from pulse_telemetry.sparklib.transformations.timeseries import timeseries_schema
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
    assert first_record.step_id == 0, "Expecing step-id 0 to be first group."
    assert (first_record.end_time - first_record.start_time).total_seconds() == approx(0.4, abs=0.01)


def test_aggregations(spark_session, statistics_step_df):
    # Validates discharge step
    discharge_record = statistics_step_df.collect()[-1]
    assert discharge_record.start_current__A == approx(1.0), "Start current - discharge."  # Current
    assert discharge_record.end_current__A == approx(1.0), "End current - discharge."
    assert discharge_record.min_current__A == approx(1.0), "Min current - discharge."
    assert discharge_record.min_abs_current__A == approx(1.0), "Min abs current - discharge."
    assert discharge_record.max_current__A == approx(1.0), "Max current - discharge."
    assert discharge_record.max_abs_current__A == approx(1.0), "Max abs current - discharge."
    assert discharge_record.mean_current__A == approx(1.0), "Mean current - discharge."
    assert discharge_record.start_voltage__V == approx(3.8), "Start voltage - discharge."  # Voltage
    assert discharge_record.end_voltage__V == approx(3.0), "End voltage - discharge."
    assert discharge_record.min_voltage__V == approx(3.0), "Min voltage - discharge."
    assert discharge_record.max_voltage__V == approx(3.8), "Max voltage - discharge."
    assert discharge_record.mean_voltage__V == approx(3.4), "Mean voltage - discharge."
    assert discharge_record.start_power__W == approx(3.8), "Start power - discharge."  # Power
    assert discharge_record.end_power__W == approx(3.0), "End power - discharge."
    assert discharge_record.min_power__W == approx(3.0), "Min power - discharge."
    assert discharge_record.min_abs_power__W == approx(3.0), "Min abs power - discharge."
    assert discharge_record.max_power__W == approx(3.8), "Max power - discharge."
    assert discharge_record.max_abs_power__W == approx(3.8), "Max abs power - discharge."
    assert discharge_record.mean_power__W == approx(3.4), "Mean power - discharge."
    assert discharge_record.charge_capacity__Ah == approx(0.0), "Charge capacity - discharge."  # Accumulations
    assert discharge_record.discharge_capacity__Ah == approx(1.0 * 0.5 / 3600), "Discharge capacity - discharge."
    assert discharge_record.charge_energy__Wh == approx(0.0), "Charge energy - discharge."
    assert discharge_record.discharge_energy__Wh == approx(3.4 * 0.5 / 3600), "Discharge energy - discharge."

    # Validates charge step
    charge_record = statistics_step_df.collect()[-2]  # The second last step is a charge
    assert charge_record.start_current__A == approx(-1.0), "Start current - charge."  # Current
    assert charge_record.end_current__A == approx(-1.0), "End current - charge."
    assert charge_record.min_current__A == approx(-1.0), "Min current - charge."
    assert charge_record.min_abs_current__A == approx(1.0), "Min abs current - charge."
    assert charge_record.max_current__A == approx(-1.0), "Max current - charge."
    assert charge_record.max_abs_current__A == approx(1.0), "Max abs current - charge."
    assert charge_record.mean_current__A == approx(-1.0), "Mean current - charge."
    assert charge_record.start_voltage__V == approx(3.2), "Start voltage - charge."  # Voltage
    assert charge_record.end_voltage__V == approx(4.0), "End voltage - charge."
    assert charge_record.min_voltage__V == approx(3.2), "Min voltage - charge."
    assert charge_record.max_voltage__V == approx(4.0), "Max voltage - charge."
    assert charge_record.mean_voltage__V == approx(3.6), "Mean voltage - charge."
    assert charge_record.start_power__W == approx(-3.2), "Start power - charge."  # Power
    assert charge_record.end_power__W == approx(-4.0), "End power - charge."
    assert charge_record.min_power__W == approx(-4.0), "Min power - charge."
    assert charge_record.min_abs_power__W == approx(3.2), "Min abs power - charge."
    assert charge_record.max_power__W == approx(-3.2), "Max power - charge."
    assert charge_record.max_abs_power__W == approx(4.0), "Max abs power - charge."
    assert charge_record.mean_power__W == approx(-3.6), "Mean power - charge."
    assert charge_record.charge_capacity__Ah == approx(-1.0 * 0.5 / 3600), "Charge capacity - charge."  # Accumulations
    assert charge_record.discharge_capacity__Ah == approx(0.0), "Discharge capacity - charge."
    assert charge_record.charge_energy__Wh == approx(-3.6 * 0.5 / 3600), "Charge energy - charge."
    assert charge_record.discharge_energy__Wh == approx(0.0), "Discharge energy - charge."


def test_null_handling(spark_session):
    # Validates all null rows
    nulled_timeseries = spark_session.createDataFrame(
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
                date=datetime.datetime.now(datetime.timezone.utc).date(),
                current__A=0.5,
                voltage__V=3.7,
                power__W=1.85,
                capacity__Ah=0.1,
                energy__Wh=0.2,
                auxiliary={"key": 0.1},
                metadata="{'key': 'value'}",
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
        ],
        schema=timeseries_schema,
    )
    nulled_statistics_step_df = statistics_step(nulled_timeseries)
    record = nulled_statistics_step_df.collect()[0]
    assert record.step_type is None, "step_type should be null if not present in input."
    assert record.step_id is None, "step_id should be null if not present in input."

    # Validates mixed nulls
    nulled_timeseries = spark_session.createDataFrame(
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
                date=datetime.datetime.now(datetime.timezone.utc).date(),
                current__A=0.5,
                voltage__V=3.7,
                power__W=1.85,
                capacity__Ah=0.1,
                energy__Wh=0.2,
                auxiliary=None,
                metadata=None,
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
            Row(
                device_id="A",
                test_id="test1",
                cycle_number=1,
                step_number=1,
                step_type="Rest",
                step_id=1,
                record_number=1,
                timestamp=datetime.datetime.now(datetime.timezone.utc),
                date=datetime.datetime.now(datetime.timezone.utc).date(),
                current__A=0.5,
                voltage__V=3.7,
                power__W=1.85,
                capacity__Ah=0.1,
                energy__Wh=0.2,
                auxiliary=None,
                metadata=None,
                update_ts=datetime.datetime.now(datetime.timezone.utc),
            ),
        ],
        schema=timeseries_schema,
    )
    nulled_statistics_step_df = statistics_step(nulled_timeseries)
    record = nulled_statistics_step_df.collect()[0]
    assert record.step_type == "Rest", "step_type should be first non-null value."
    assert record.step_id == 1, "step_id should be first non-null value."


def test_empty_df(spark_session):
    empty_timeseries = spark_session.createDataFrame([], schema=timeseries_schema)
    empty_statistics_step = statistics_step(empty_timeseries)
    assert empty_statistics_step.count() == 0, "The output DataFrame should be empty for empty input."
