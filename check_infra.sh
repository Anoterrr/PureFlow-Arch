#!/bin/bash
echo "--- Verificando Containers ---"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n--- Testando Rede Interna (DNS) ---"
docker exec pureflow_app ping minio -c 2 || echo "ERRO: App não enxerga o MinIO"

echo -e "\n--- Testando Permissões de Escrita (Data Lake) ---"
docker exec pureflow_app touch /app/data/write_test && echo "Escrita: OK" || echo "ERRO: Sem permissão em /app/data"

echo -e "\n--- Testando dbt + DuckDB ---"
docker exec pureflow_app dbt debug