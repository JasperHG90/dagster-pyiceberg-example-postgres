from dagster import materialize
from dagster_pyiceberg import IcebergSqlCatalogConfig
from dagster_pyiceberg_pandas import IcebergPandasIOManager

from dagster_pyiceberg_example.assets import air_quality_data, daily_air_quality_data
from dagster_pyiceberg_example.IO import (
    LuchtMeetNetResource,
    RateLimiterResource,
    RedisResource,
)

resources = {
    "io_manager": IcebergPandasIOManager(
        name="dagster_example_catalog",
        config=IcebergSqlCatalogConfig(
            properties={
                "uri": "postgresql+psycopg2://pyiceberg:pyiceberg@postgres/catalog",
                "s3.endpoint": "http://minio:9000",
                "s3.access-key-id": "pyiceberg",
                "s3.secret-access-key": "pyiceberg",
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "warehouse": "s3://warehouse",
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
                host="redis",
                port=16564,
                username="default",
                password="dagster",
            ),
        )
    ),
}


def test_asset():
    res = materialize(
        assets=[air_quality_data, daily_air_quality_data],
        resources=resources,
        partition_key="2024-11-07",
        selection=[daily_air_quality_data],
    )
    assert res.success
