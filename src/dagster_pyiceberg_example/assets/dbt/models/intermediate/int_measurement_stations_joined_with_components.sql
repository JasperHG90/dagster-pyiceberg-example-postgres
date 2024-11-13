{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/int_measurement_stations_joined_with_components.parquet",
        plugin="custom_iceberg"
    )
}}

with measurements as (

    select * from {{ ref('stg_luchtmeetnet__measurements') }}

),

components as (

    select * from {{ ref('raw_components') }}

),

components_by_station as (

    select
        measurements.station_number,
        measurements.formula,
        components.name_english as formula_name
    from measurements
    left join components on measurements.formula = components.formula

)

select * from components_by_station
