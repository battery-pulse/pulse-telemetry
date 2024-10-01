import pytest
from pulse_telemetry.sparklib.transformations.statistics_step import statistics_step
from pulse_telemetry.sparklib.transformations.timeseries import timeseries_schema
from pulse_telemetry.utils import channel
from pulse_telemetry.utils.timeseries_generator import timeseries_generator
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return SparkSession.builder.appName("UnitTesting").getOrCreate()


@pytest.fixture(scope="session")
def timeseries_df(spark_session) -> DataFrame:
    # Runs the generator against a local buffer
    local_buffer = channel.LocalBuffer()
    channel.run_with_timeout(
        source=timeseries_generator,
        sink=local_buffer,
        topic="timeseries",
        num_channels=5,
        timeout_seconds=3,
        aquisition_frequency=10,
        points_per_step=5,
        lower_voltage_limit=3,  # V
        upper_voltage_limit=4,  # V
        current=1.0,  # A
    )

    # Collects the results in a pyspark dataframe
    timeseries = local_buffer.dataframe(spark_session, timeseries_schema)

    return timeseries


@pytest.fixture(scope="session")
def statistics_step_df(timeseries_df) -> DataFrame:
    return statistics_step(timeseries_df)
