import asyncio
import json
from collections.abc import AsyncGenerator, Callable
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import col, from_json

# Abstraction for generator functions
StateGenerator = Callable[..., AsyncGenerator[dict, None]]


# Kafka producer adopts this interface (see LocalBuffer below for unit testing)
class Publisher(Protocol):
    def send(self, topic: str, value: str):
        pass


async def channel(source: StateGenerator, sink: Publisher, topic: str, **kwargs):
    async for state in source(**kwargs):
        message = json.dumps(state)
        sink.send(topic=topic, value=message)


async def concurrent_channels(source: StateGenerator, sink: Publisher, topic: str, num_channels: int, **kwargs):
    tasks = [asyncio.create_task(channel(source=source, sink=sink, topic=topic, **kwargs)) for _ in range(num_channels)]
    await asyncio.gather(*tasks)


def run_with_timeout(
    source: StateGenerator, sink: Publisher, topic: str, num_channels: int, timeout_seconds: float, **kwargs
):
    try:
        asyncio.run(
            asyncio.wait_for(
                fut=concurrent_channels(source=source, sink=sink, topic=topic, num_channels=num_channels, **kwargs),
                timeout=timeout_seconds,
            )
        )
    except asyncio.TimeoutError:
        pass


class LocalBuffer(Publisher):  # For unit testing
    def __init__(self):
        self.messages = []

    def send(self, topic: str, value: str):
        self.messages.append(value)

    def dataframe(self, spark: SparkSession, schema: T.StructType) -> DataFrame:
        json_rdd = spark.sparkContext.parallelize([(x,) for x in self.messages])
        return (
            spark.createDataFrame(json_rdd, T.StructType([T.StructField("json_value", T.StringType())]))
            .select(from_json(col("json_value"), schema).alias("data"))
            .select("data.*")
        )
