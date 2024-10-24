import datetime
from typing import TYPE_CHECKING

import pyspark.sql.functions as F

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame

    Aggregation = Callable[[DataFrame], DataFrame]


def processing_incremental(
    source: "DataFrame",
    sink: "DataFrame",
    aggregation_function: "Aggregation",
    group_by_columns: list[str],
    partition_cutoff: datetime.datetime,
    partition_column: str,
    watermark_column: str = "update_ts",
    watermark_buffer: datetime.timedelta = datetime.timedelta(minutes=60),
    watermark_default: datetime.datetime = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),
) -> "DataFrame":
    """Incremental processing for aggregation transformations.

    Applicable for telemetry -> statistics_step and statistics_step -> statistics_cycle.

    Parameters
    ----------
    source: DataFrame
        The source table.
    sink: DataFrame
        The sink table.
    aggregation_function: Callable[[DataFrame], DataFrame]
        The aggregation function to apply to the source data, returning the sink data.
    group_by_columns: List[str]
        The columns that define a group in the source and a record in the sink.
    partition_cutoff: datetime.datetime
        The timestamp cutoff for partition pruning.
    partition_column: str
        The name of the timestamp column used for partitioning in the source table.
    watermark_column: str
        The name of the timestamp column representing when records were processed by the system.
    watermark_buffer: datetime.timedelta
        The time delta to subtract from the last processed timestamp as a buffer.
    watermark_default: datetime.datetime, optional
        The default timestamp to use if the sink table is empty.

    Returns
    -------
    DataFrame
        The DataFrame resulting from applying the aggregation function to the filtered source records.

    """
    # Get the adjusted last processed timestamp from the sink DataFrame
    watermark = _adjusted_watermark(
        sink=sink,
        watermark_column=watermark_column,
        watermark_buffer=watermark_buffer,
        watermark_default=watermark_default,
    )

    # Identify updated groups in the source DataFrame
    updated_groups = _updated_groups_in_source(
        source=source,
        group_by_columns=group_by_columns,
        partition_cutoff=partition_cutoff,
        partition_column=partition_column,
        watermark=watermark,
        watermark_column=watermark_column,
    )

    # Fetch all records for updated groups
    all_records = _source_records_for_updated_groups(
        source=source,
        updated_groups=updated_groups,
        partition_cutoff=partition_cutoff,
        partition_column=partition_column,
    )

    # Perform aggregations on the updated groups
    return aggregation_function(all_records)


def _adjusted_watermark(
    sink: "DataFrame",
    watermark_column: str,  # In our case, update_ts
    watermark_buffer: datetime.timedelta,  # Some records might have come in during processing
    watermark_default: datetime.datetime,  # Default if there is not sink records
) -> datetime.datetime:
    if sink.head(1):  # Not empty
        max_timestamp = sink.agg(F.max(watermark_column)).collect()[0][0] or watermark_default
    else:
        max_timestamp = watermark_default
    return (max_timestamp - watermark_buffer).astimezone(datetime.UTC)


def _updated_groups_in_source(
    source: "DataFrame",
    group_by_columns: list[str],  # Whatever groups in the source identifies a record in the sink
    partition_cutoff: datetime.datetime,  # Used for partition pruning
    partition_column: str,
    watermark: datetime.datetime,  # Filter for records that have come in since last batch
    watermark_column: str,
) -> "DataFrame":
    return (
        source.filter(
            (F.col(partition_column) >= partition_cutoff)  # Used for partition pruning
            & (F.col(watermark_column) > watermark)  # Records not processed in last batch
        )
        .select(*group_by_columns)
        .distinct()
    )


def _source_records_for_updated_groups(
    source: "DataFrame",
    updated_groups: "DataFrame",  # The distinct groups that have new data
    partition_cutoff: datetime.datetime,  # Used for partition pruning
    partition_column: str,
) -> "DataFrame":
    # Cache the updated_groups to avoid recomputation
    updated_groups = updated_groups.cache()

    # Efficiently check the number of groups without counting the entire DataFrame
    sampled_count = updated_groups.limit(1).count()

    # Filters source records using a join
    if sampled_count == 0:
        # No updated records
        all_records = source.limit(0)
    else:
        # See spark session for `spark.sql.autoBroadcastJoinThreshold`
        all_records = source.filter(
            F.col(partition_column) >= partition_cutoff  # Partition pruning
        ).join(updated_groups, on=updated_groups.columns, how="left_semi")

    updated_groups.unpersist()  # Free up the cached DataFrame
    return all_records
