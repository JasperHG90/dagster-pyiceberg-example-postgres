import os

from dagster import Definitions, EnvVar
from dagster_aws.s3 import S3PickleIOManager, S3Resource
from dagster_dbt import DbtCliResource
from dagster_pyiceberg.config import IcebergCatalogConfig
from dagster_pyiceberg.io_manager.polars import IcebergPolarsIOManager

from dagster_pyiceberg_example.assets import (
    air_quality_data,
    daily_air_quality_data,
    luchtmeetnet_models_dbt_assets,
    luchtmeetnet_models_project,
)
from dagster_pyiceberg_example.IO import (
    LuchtMeetNetResource,
    RateLimiterResource,
    RedisResource,
)

resources = {
    "landing_zone_io_manager": S3PickleIOManager(
        s3_resource=S3Resource(
            endpoint_url=os.environ["DAGSTER_SECRET_S3_ENDPOINT"],
            aws_access_key_id=os.environ["DAGSTER_SECRET_S3_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["DAGSTER_SECRET_S3_SECRET_ACCESS_KEY"],
            aws_session_token=None,
            verify=False,
        ),
        s3_bucket="landingzone",
    ),
    "warehouse_io_manager": IcebergPolarsIOManager(
        name="dagster_example_catalog",
        config=IcebergCatalogConfig(
            properties={
                "uri": os.environ["DAGSTER_SECRET_PYICEBERG_CATALOG_URI"],
                "s3.endpoint": os.environ["DAGSTER_SECRET_S3_ENDPOINT"],
                "s3.access-key-id": os.environ["DAGSTER_SECRET_S3_ACCESS_KEY_ID"],
                "s3.secret-access-key": os.environ[
                    "DAGSTER_SECRET_S3_SECRET_ACCESS_KEY"
                ],
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "warehouse": os.environ["DAGSTER_SECRET_S3_WAREHOUSE"],
            },
        ),
        schema="air_quality",
        partition_spec_update_mode="update",
        schema_update_mode="update",
        db_io_manager="custom",
    ),
    "luchtmeetnet_api": LuchtMeetNetResource(
        rate_limiter=RateLimiterResource(  # See https://api-docs.luchtmeetnet.nl/ for rate limits
            rate_calls=100,
            rate_minutes=5,
            bucket_key="luchtmeetnet_api",
            redis=RedisResource(
                host=EnvVar("DAGSTER_SECRET_REDIS_HOST"),
                port=16564,
                password=EnvVar("DAGSTER_SECRET_REDIS_PASSWORD"),
                username=EnvVar("DAGSTER_SECRET_REDIS_USERNAME"),
            ),
        )
    ),
    "dbt": DbtCliResource(
        project_dir=luchtmeetnet_models_project,
    ),
}


definition = Definitions(
    assets=[air_quality_data, daily_air_quality_data, luchtmeetnet_models_dbt_assets],
    resources=resources,
)
