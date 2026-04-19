{{ config(
    materialized='external',
    location="s3://gold/sales_summary/dt=" ~ var('execution_date', '2026-04-18') ~ "/sales_summary.parquet",
    format='parquet'
) }}

WITH silver_sales AS (
    SELECT * FROM {{ source('silver', 'sales_silver') }}
),

silver_customers AS (
    SELECT * FROM {{ source('silver', 'customers_silver') }}
),

enriched_sales AS (
    SELECT 
        v.id,
        v.price as sale_value,
        v.sale_date,
        c.name as customer_name,
        c.uf as region,
        date_trunc('month', v.sale_date::DATE) as sale_month
    FROM silver_sales v
    LEFT JOIN silver_customers c ON v.customer_id = c.customer_id
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
