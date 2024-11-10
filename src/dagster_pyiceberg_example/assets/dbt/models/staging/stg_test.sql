{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/stg_test.parquet",
        plugin="custom_iceberg"
    )
}}

SELECT * FROM {{ source('dagster_example_catalog', 'daily_air_quality_data' ) }}
