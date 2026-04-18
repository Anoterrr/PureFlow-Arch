-- Bronze transformation for Sales (Raw to Bronze)
SELECT 
    id, 
    cliente_id, 
    produto, 
    CAST(preco AS DOUBLE) as preco, 
    CAST(data AS DATE) as data_venda 
FROM source_data
