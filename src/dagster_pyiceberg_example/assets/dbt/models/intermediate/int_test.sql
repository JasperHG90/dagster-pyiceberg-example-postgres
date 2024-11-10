{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/int_test.parquet",
        plugin="custom_iceberg"
    )
}}

SELECT * FROM {{ ref('stg_test') }}
