{{ config(materialized='table') }}

{% set base_date = var('execution_date', '2024-03-29') %}

WITH silver_vendas AS (
    -- Consumindo da camada Silver via Delta Scan com data dinâmica
    SELECT * FROM delta_scan('s3://silver/erp_vendas/dt=' ~ base_date ~ '/')
),

silver_clientes AS (
    SELECT * FROM delta_scan('s3://silver/crm_clientes/dt=' ~ base_date ~ '/')
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
