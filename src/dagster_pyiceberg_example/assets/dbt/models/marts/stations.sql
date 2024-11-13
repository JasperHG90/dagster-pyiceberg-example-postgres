{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/stations.parquet",
        plugin="custom_iceberg"
    )
}}

select * from {{ ref('int_measurements_aggregate_by_measurement_date') }}
