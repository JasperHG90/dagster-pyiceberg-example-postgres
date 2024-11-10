{{
    config(
        materialized='external',
        location='/home/vscode/workspace/.tmp/data/stg_test.parquet',
        plugin="custom_iceberg"
    )
}}

SELECT * FROM {{ source('dagster_example_catalog', 'daily_air_quality_data' ) }}
