{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/int_measurements_summary_statistics_by_measurement_date.parquet",
        plugin="custom_iceberg"
    )
}}

with measurements as (

    select * from {{ ref('stg_measurements') }}

),

final as (

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
        {% for formula in formulas -%}

            sum(
                case
                    when formula = '{{ formula }}'
                    then value
                    else 0
                end
            ) as {{ formula }}_value,

        {% endfor -%}

    from measurements

    left join {{ ref('stations') }} as stations on measurements.station_number = stations.number

    group by 1, 2, 3

)

select * from final
