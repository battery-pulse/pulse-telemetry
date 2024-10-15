import pytest
from pulse_telemetry.sparklib.statistics_cycle import statistics_cycle
from pulse_telemetry.sparklib.statistics_step import statistics_step
from pulse_telemetry.sparklib.telemetry import telemetry_schema
from pulse_telemetry.utils import channel
from pulse_telemetry.utils.telemetry_generator import telemetry_generator
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return SparkSession.builder.appName("UnitTesting").config("spark.sql.session.timeZone", "UTC").getOrCreate()


@pytest.fixture(scope="session")
def telemetry_df(spark_session) -> DataFrame:
    # Runs the generator against a local buffer
    local_buffer = channel.LocalBuffer()
    channel.run_with_timeout(
        source=telemetry_generator,
        sink=local_buffer,
        topic="telemetry",
        num_channels=5,
        timeout_seconds=3,
        acquisition_frequency=10,
        points_per_step=5,
        lower_voltage_limit=3,  # V
        upper_voltage_limit=4,  # V
        current=1.0,  # A
    )

    # Collects the results in a pyspark dataframe
    telemetry = local_buffer.dataframe(spark_session, telemetry_schema)

    return telemetry


@pytest.fixture(scope="session")
def statistics_step_df(telemetry_df) -> DataFrame:
    return statistics_step(telemetry_df)


@pytest.fixture(scope="session")
def statistics_cycle_df(statistics_step_df) -> DataFrame:
    return statistics_cycle(statistics_step_df)
