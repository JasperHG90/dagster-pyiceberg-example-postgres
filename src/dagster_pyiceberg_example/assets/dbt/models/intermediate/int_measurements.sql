{{
    config(
        materialized="external",
        location="{{ env_var('DBT_DUCKDB_TEMP_DATA_DIR') }}/int_test.parquet",
        plugin="custom_iceberg"
    )
}}

{%- set formulas = ['C10H8', 'C6H6', 'C7H8', 'C8H10', 'CO', 'H2S', 'NH3', 'NO', 'NO2', 'PM10', 'PM25', 'PS', 'SO2'] -%}

with measurements as (

    select * from {{ ref('stg_measurements') }}

),

final as (

    select
        station_number,
        b.location as station_location,
        a.measurement_date,
        {% for formula in formulas -%}

            sum(
                case
                    when formula = '{{ formula }}'
                    then value
                    else 0
                end
            ) as {{ formula }}_value,

        {% endfor -%}

    from measurements as a

    left join {{ ref('stations') }} as b on a.station_number = b.number

    group by 1, 2, 3

)

select * from final
