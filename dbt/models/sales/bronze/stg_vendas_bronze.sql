{{ config(
    materialized='external',
    location="s3://bronze/erp_vendas/dt=" ~ var('execution_date', modules.datetime.datetime.now().strftime('%Y-%m-%d')) ~ "/vendas.parquet",
    format='parquet'
) }}

SELECT
    *,
    now() as _ingested_at,
    'sales' as _domain,
    'vendas.csv' as _source_file
FROM {{ source('landing', 'raw_vendas') }}
