import hashlib
import warnings
from pathlib import Path
from typing import Any, Dict, List

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
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
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

luchtmeetnet_models_project = DbtProject(
    project_dir=Path(__file__).parent.joinpath("dbt").resolve(),
    packaged_project_dir=Path(__file__).parent.joinpath("dbt-project").resolve(),
)
luchtmeetnet_models_project.prepare_if_dev()


@asset(
    description="Air quality data from the Luchtmeetnet API",
    compute_kind="python",
    io_manager_key="landing_zone_io_manager",
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
) -> Output[List[Dict[str, Any]]]:
    df = get_air_quality_data_for_partition_key(
        context.partition_key, context, luchtmeetnet_api
    )
    df_hash = hashlib.sha256(hash_pandas_object(df, index=True).values).hexdigest()
    return Output(df.to_dict(orient="records"), data_version=DataVersion(df_hash))


@asset(
    description="Copy air quality data to iceberg table",
    compute_kind="iceberg",
    io_manager_key="warehouse_io_manager",
    partitions_def=daily_partition,
    ins={
        "ingested_data": AssetIn(
            "air_quality_data",
            # NB: need this to control which downstream asset partitions are materialized
            partition_mapping=MultiToSingleDimensionPartitionMapping(
                partition_dimension_name="daily"
            ),
            input_manager_key="landing_zone_io_manager",
            # NB: Some partitions can fail because of 500 errors from API
            #  So we need to allow missing partitions
            metadata={"allow_missing_partitions": True},
        )
    },
    code_version="v1",
    group_name="measurements",
    metadata={
        "partition_expr": "measurement_date",
    },
)
def daily_air_quality_data(
    context: AssetExecutionContext, ingested_data: Dict[str, List[Dict[str, Any]]]
) -> pd.DataFrame:
    context.log.info(f"Copying data for partition {context.partition_key}")
    return pd.concat(
        [
            pd.DataFrame.from_dict(data_partition)
            for data_partition in ingested_data.values()
        ]
    )


@dbt_assets(
    manifest=luchtmeetnet_models_project.manifest_path,
)
def luchtmeetnet_models_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
