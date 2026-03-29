#!/bin/bash
echo "--- Checking Containers ---"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n--- Testing Internal Network (DNS) ---"
docker exec pureflow_app ping minio -c 2 || echo "ERROR: App cannot see MinIO"

echo -e "\n--- Testing Write Permissions (Data Lake) ---"
docker exec pureflow_app touch /app/data/write_test && echo "Write: OK" || echo "ERROR: No permission in /app/data"

echo -e "\n--- Testing dbt + DuckDB ---"
docker exec pureflow_app dbt debug
