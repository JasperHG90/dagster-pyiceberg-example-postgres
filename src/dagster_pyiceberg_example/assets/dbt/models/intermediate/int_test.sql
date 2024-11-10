{{
    config(
        materialized='external',
        location='/home/vscode/workspace/.tmp/data/int_test.parquet',
        plugin="custom_iceberg"
    )
}}

SELECT * FROM {{ ref('stg_test') }}
