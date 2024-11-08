import hashlib
import warnings

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    Backoff,
    DataVersion,
    ExperimentalWarning,
    Jitter,
    MultiToSingleDimensionPartitionMapping,
    Output,
    RetryPolicy,
    asset,
)
from pandas.util import hash_pandas_object

from dagster_pyiceberg_example.assets.utils import (
    get_air_quality_data_for_partition_key,
)
from dagster_pyiceberg_example.IO import LuchtMeetNetResource
from dagster_pyiceberg_example.partitions import (
    daily_partition,
    daily_station_partition,
)

warnings.filterwarnings("ignore", category=ExperimentalWarning)


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="iceberg",
    io_manager_key="io_manager",
    partitions_def=daily_station_partition,
    retry_policy=RetryPolicy(
        max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
    ),
    code_version="v1",
    group_name="measurements",
    metadata={
        "partition_expr": {
            "daily": "measurement_date",
            "stations": "station_number",
        }
    },
)
def air_quality_data(
    context: AssetExecutionContext,
    luchtmeetnet_api: LuchtMeetNetResource,
) -> Output[pd.DataFrame]:
    df = get_air_quality_data_for_partition_key(
        context.partition_key, context, luchtmeetnet_api
    )
    df_hash = hashlib.sha256(hash_pandas_object(df, index=True).values).hexdigest()
    return Output(df, data_version=DataVersion(df_hash))


@asset(
    description="Copy air quality data from ingestion to bronze",
    compute_kind="iceberg",
    io_manager_key="io_manager",
    partitions_def=daily_partition,
    ins={
        "ingested_data": AssetIn(
            "air_quality_data",
            # NB: need this to control which downstream asset partitions are materialized
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="daily"
            ),
            input_manager_key="io_manager",
        )
    },
    code_version="v1",
    group_name="measurements",
    metadata={
        "partition_expr": "measurement_date",
    },
)
def daily_air_quality_data(
    context: AssetExecutionContext, ingested_data: pd.DataFrame
) -> pd.DataFrame:
    context.log.info(f"Copying data for partition {context.partition_key}")
    context.log.info(ingested_data.head())
    context.log.info(ingested_data.shape)
    return ingested_data
