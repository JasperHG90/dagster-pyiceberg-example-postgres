{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/int_measurements_pivoted_to_station_and_date.parquet",
        plugin="custom_iceberg"
    )
}}

{%- set formulas = ['C10H8', 'C6H6', 'C7H8', 'C8H10', 'CO', 'H2S', 'NH3', 'NO', 'NO2', 'PM10', 'PM25', 'PS', 'SO2'] -%}

with measurements as (

    select * from {{ ref('stg_luchtmeetnet__measurements') }}

),

final as (

    select
        measurements.station_number,
        stations.location as station_location,
        measurements.timestamp_measured,
        first(measurements.measurement_date),
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
