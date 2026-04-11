{{ config(materialized='table') }}

WITH silver_vendas AS (
    -- Direct Delta scan from Silver (Task 3.1)
    -- dbt-duckdb supports delta via external_location if configured, 
    -- but direct scan is more reliable for custom Delta/S3 paths.
    SELECT * FROM delta_scan('s3://silver/erp_vendas/dt=2024-03-29/')
),

silver_clientes AS (
    SELECT * FROM delta_scan('s3://silver/crm_clientes/dt=2024-03-29/')
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
