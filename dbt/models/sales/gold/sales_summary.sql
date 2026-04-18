{{ config(
    materialized='external',
    location="s3://gold/sales_summary/dt=" ~ var('execution_date', '2026-04-17') ~ "/sales_summary.parquet",
    format='parquet'
) }}

WITH silver_vendas AS (
    SELECT * FROM {{ source('silver', 'vendas_silver') }}
),

silver_clientes AS (
    SELECT * FROM {{ source('silver', 'clientes_silver') }}
),

enriched_sales AS (
    SELECT 
        v.id,
        v.preco as sale_value,
        v.data_venda as sale_date,
        c.nome as customer_name,
        c.uf as region,
        date_trunc('month', v.data_venda::DATE) as sale_month
    FROM silver_vendas v
    LEFT JOIN silver_clientes c ON v.cliente_id = c.cliente_id
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
