{{ config(materialized='external', format='parquet') }}

{% set base_date = var('execution_date', '2026-04-15') %}

WITH silver_vendas AS (
    -- Consumindo da camada Silver via Parquet Scan com data dinâmica
    SELECT * FROM read_parquet('s3://silver/erp_vendas/dt=' ~ base_date ~ '/vendas.parquet')
),

silver_clientes AS (
    -- Consumindo da camada Silver via Parquet Scan com data dinâmica
    SELECT * FROM read_parquet('s3://silver/crm_clientes/dt=' ~ base_date ~ '/clientes.parquet')
),

enriched_sales AS (
    SELECT 
        v.id,
        v.sale_value,
        v.sale_date,
        c.name as customer_name,
        c.region,
        date_trunc('month', v.sale_date::DATE) as sale_month
    FROM silver_vendas v
    LEFT JOIN silver_clientes c ON v.customer_id = c.customer_id
)

SELECT 
    region,
    sale_month,
    sum(sale_value) as total_revenue,
    count(id) as total_orders,
    round(avg(sale_value), 2) as avg_ticket
FROM enriched_sales
GROUP BY region, sale_month
ORDER BY total_revenue DESC
