-- Transformation from Bronze to Silver for Sales
SELECT 
    *,
    (preco * 0.9) as preco_com_desconto,
    CURRENT_TIMESTAMP as silver_processed_at
FROM source_data
WHERE preco > 0
