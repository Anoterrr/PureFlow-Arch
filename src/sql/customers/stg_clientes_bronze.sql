-- Bronze transformation for Customers (Raw to Bronze)
SELECT 
    id as cliente_id, 
    nome, 
    email, 
    cidade,
    estado 
FROM source_data
