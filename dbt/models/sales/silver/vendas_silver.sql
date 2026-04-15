{{ config(
    materialized='external',
    location="s3://silver/erp_vendas/dt=" ~ var('execution_date', modules.datetime.datetime.now().strftime('%Y-%m-%d')) ~ "/vendas.parquet",
    format='parquet'
) }}

SELECT
    id,
    customer_id,
    CAST(sale_date AS DATE) as sale_date,
    CAST(sale_value AS DECIMAL(18,2)) as sale_value,
    _ingested_at,
    _source_file,
    now() as _processed_at
FROM {{ ref('stg_vendas_bronze') }}
WHERE sale_value > 0  -- Business Rule: only positive sales
  AND customer_id IS NOT NULL -- Business Rule: customer must be identified
