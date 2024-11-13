{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/int_measurements_aggregate_by_measurement_date.parquet",
        plugin="custom_iceberg"
    )
}}

with measurements as (

    select * from {{ ref('stg_luchtmeetnet__measurements') }}

),

stations as (

    select * from {{ ref('raw_stations') }}

),

measurements_summary_statistics as (

    select
        measurements.station_number,
        stations.location as station_location,
        measurements.measurement_date,
        formula,
        avg(value) as mean_value,
        median(value) as median_value,
        min(value) as min_value,
        max(value) as max_value,
        stddev(value) as stddev_value,

    from measurements

    left join stations on measurements.station_number = stations.number

    group by 1, 2, 3, 4

)

select * from measurements_summary_statistics
