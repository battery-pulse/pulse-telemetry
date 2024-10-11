import datetime

import pulse_telemetry.sparklib.transformations.processing_incremental as processing_incramental
import pyspark.sql.types as T
from pyspark.sql import Row


def test_adjusted_watermark(spark_session):

    schema = T.StructType([T.StructField("watermark", dataType=T.TimestampType(), nullable=False)])
    
    # Empty sink
    sink = spark_session.createDataFrame([], schema=schema)
    watermark = processing_incramental._adjusted_watermark(
        sink=sink,
        watermark_column="watermark",
        watermark_buffer=datetime.timedelta(days=1),
        watermark_default=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    )
    assert watermark == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), "For empty sink."

    # Sink with data
    sink = spark_session.createDataFrame(
        [
            Row(watermark=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)),
            Row(watermark=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)),
        ], schema=schema
    )
    watermark = processing_incramental._adjusted_watermark(
        sink=sink,
        watermark_column="watermark",
        watermark_buffer=datetime.timedelta(days=1),
        watermark_default=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    )
    assert watermark == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), "For sink with data."
