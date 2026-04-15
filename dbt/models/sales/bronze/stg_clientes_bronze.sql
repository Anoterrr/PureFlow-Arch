{{ config(
    materialized='external',
    location="s3://bronze/crm_clientes/dt=" ~ var('execution_date', modules.datetime.datetime.now().strftime('%Y-%m-%d')) ~ "/clientes.parquet",
    format='parquet'
) }}

SELECT
    *,
    now() as _ingested_at,
    'customers' as _domain,
    'clientes.json' as _source_file
FROM {{ source('landing', 'raw_clientes') }}
