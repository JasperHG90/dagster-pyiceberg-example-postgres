{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/stg_test.parquet",
        plugin="custom_iceberg"
    )
}}

with source as (

    select * from {{ source('dagster_example_catalog', 'daily_air_quality_data' ) }}

),

renamed as (

    select
        station_number,
        timestamp_measured,
        measurement_date,
        formula,
        value
    from source
)

select * from renamed
