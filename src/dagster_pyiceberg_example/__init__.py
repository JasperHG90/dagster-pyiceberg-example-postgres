from dagster import Definitions, EnvVar

from dagster_pyiceberg_example.assets import air_quality_data, daily_air_quality_data
from dagster_pyiceberg_example.IO import (
    LuchtMeetNetResource,
    RateLimiterResource,
    RedisResource,
)

resources = {
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
}


definition = Definitions(
    assets=[
        air_quality_data,
        daily_air_quality_data,
    ],
    resources=resources,
)
