{{ config(
    materialized='external',
    location="s3://silver/crm_clientes/dt=" ~ var('execution_date', modules.datetime.datetime.now().strftime('%Y-%m-%d')) ~ "/clientes.parquet",
    format='parquet'
) }}

SELECT
    customer_id,
    name,
    email,
    phone,
    region,
    CAST(created_at AS DATE) as created_at,
    _ingested_at,
    _source_file,
    now() as _processed_at
FROM {{ ref('stg_clientes_bronze') }}
WHERE customer_id IS NOT NULL
  AND email LIKE '%@%' -- Business Rule: valid email format
