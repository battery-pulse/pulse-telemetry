import datetime
from typing import List, Callable, TYPE_CHECKING

import pyspark.sql.functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


# Telem - device id, test id, month
# Steps - device id, year
# Cycle - device id, year


def adjusted_watermark(
    sink: "DataFrame",
    timestamp_column: str = "update_ts",
    watermark_buffer: datetime.timedelta = datetime.timedelta(minutes=60),
    default_watermark: datetime = datetime.datetime(1970, 1, 1)
) -> datetime:
    """Retrieves the maximum update timestamp from the sink DataFrame and subtracts a buffer.

    The buffer should be longer than the duration of the job that builds the sink dataset.

    Parameters
    ----------
    sink : DataFrame
        The sink table.
    timestamp_column : str
        The name of the "updated at" timestamp column in the sink table.
    watermark_buffer : datetime.timedelta
        The time delta to subtract from the max timestamp as a buffer.
    default_watermark : datetime
        The default timestamp to use if the sink table is empty.

    Returns
    -------
    datetime
        The adjusted maximum update timestamp as a datetime object.
    """
    if sink.head(1):  # not empty
        max_timestamp = sink.agg(F.max(timestamp_column)).collect()[0][0] or default_watermark
    else:
        max_timestamp = default_watermark

    # Adjust the timestamp with buffer
    return max_timestamp - watermark_buffer


def updated_groups_in_source(
    source: "DataFrame",
    adjusted_watermark: datetime.datetime,
    sink_group_by_cols: List[str],
    timestamp_col: str = "update_ts"
) -> "DataFrame":
    """Identifies groups in the source DataFrame that have new data since the watermark.

    The watermark represents the last time that data in the sink table was updated.

    Parameters
    ----------
    source : DataFrame
        The source table.
    adjusted_watermark : datetime
        The watermark, which is used to filter for updated rows in the source.
    sink_group_by_cols : List[str]
        The columns that define a group in the sink.
    timestamp_col : str
        The name of the "updated at" timestamp column in the source table.

    Returns
    -------
    DataFrame
        A DataFrame containing the distinct groups with new data.
    """
    return source.filter(
        F.col(timestamp_col) > adjusted_watermark
    ).select(*sink_group_by_cols).distinct()


def source_records_for_updated_groups(
    source: "DataFrame",
    updated_groups: "DataFrame",
    sink_group_by_cols: List[str],
    broadcast_threshold: int = 10000  # Adjust based on your environment
) -> "DataFrame":
    """Fetches all records from the source DataFrame for the specified groups.

    Parameters
    ----------
    source : DataFrame
        The source table.
    updated_groups : DataFrame
        DataFrame containing the sink groups to fetch data for.
    sink_group_by_cols : List[str]
        The columns that define a group in the sink, used in a join.
    broadcast_threshold : int
        The maximum number of groups to broadcast. If the number of groups exceeds this threshold,
        a semi-join is used instead of broadcasting.

    Returns
    -------
    DataFrame
        A DataFrame containing all source records for the specified groups.
    """
    # Cache the updated_groups to avoid recomputation (used twice below)
    updated_groups = updated_groups.cache()
    
    # Efficiently check the number of groups without counting the entire DataFrame
    sampled_count = updated_groups.limit(broadcast_threshold + 1).count()
    
    if sampled_count <= broadcast_threshold:
        all_records = source.join(F.broadcast(updated_groups), on=sink_group_by_cols, how="inner")
    else:
        all_records = source.join(updated_groups, on=sink_group_by_cols, how="left_semi")

    updated_groups.unpersist()  # Free updated groups
    return all_records


def incramental_processing(
    source: "DataFrame",
    sink: "DataFrame",
    sink_group_by_cols: List[str],
    timestamp_col: str,
    buffer_minutes: int,
    aggregation_function: Callable[["DataFrame"], "DataFrame"],
    default_timestamp: datetime.datetime = datetime.datetime(1970, 1, 1),
    broadcast_threshold: int = 10000  # Adjust based on your environment
) -> "DataFrame":
    """Main incramental processing job of the source data.

    Parameters
    ----------
    source : DataFrame
        The source table.
    sink : DataFrame
        The sink table.
    sink_group_by_cols : List[str]
        Columns that define the groups in the sink.
    timestamp_col : str
        The name of the timestamp column used for incremental reading.
    buffer_minutes : int
        Number of minutes to subtract from the last processed timestamp to handle late-arriving data.
    aggregation_function : Callable[[DataFrame], DataFrame]
        The aggregation function to apply to the data.
    default_timestamp : datetime.datetime, optional
        The default timestamp to use if the sink DataFrame is empty.
    broadcast_threshold : int, optional
        The maximum number of groups to broadcast in joins.

    Returns
    -------
    DataFrame
        The DataFrame resulting from applying the aggregation function to the filtered source records.
    """

    # Get the adjusted last processed timestamp from the sink DataFrame
    adjusted_timestamp = adjusted_watermark(
        sink,
        timestamp_column=timestamp_col,
        watermark_buffer=datetime.timedelta(minutes=buffer_minutes),
        default_watermark=default_timestamp
    )

    # Identify updated groups in the source DataFrame
    updated_groups = updated_groups_in_source(
        source,
        adjusted_timestamp,
        sink_group_by_cols,
        timestamp_col=timestamp_col
    )
    if not updated_groups.head(1):
        return source.sql_ctx.createDataFrame([], source.schema)  # Return an empty DataFrame if no updates

    # Fetch all records for updated groups
    all_records = source_records_for_updated_groups(
        source,
        updated_groups,
        sink_group_by_cols,
        broadcast_threshold=broadcast_threshold
    )

    # Perform aggregations and return the result
    return aggregation_function(all_records)
