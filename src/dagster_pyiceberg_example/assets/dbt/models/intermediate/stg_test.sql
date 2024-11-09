{{
    config(
        materialized='external',
        location="{{ env_var('DAGSTER_SECRET_S3_WAREHOUSE') }}/luchtmeetnet/test.parquet"
    )
}}

SELECT * FROM {{ source('dagster_example_catalog', 'daily_air_quality_data' ) }}
