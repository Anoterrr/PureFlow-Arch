#!/bin/bash
# scripts/setup_minio.sh: Automates the creation of required buckets in MinIO.

# Load environment variables from .env if it exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Configuration (using defaults from .env or connection.py)
STORAGE_USER=${STORAGE_USER:-admin}
STORAGE_PASSWORD=${STORAGE_PASSWORD:-strongpassword123}
S3_ENDPOINT=${S3_ENDPOINT:-http://localhost:9000}

# Buckets to create
BUCKETS=(
    "${S3_BUCKET_LANDING:-landing-zone}"
    "${S3_BUCKET_BRONZE:-bronze}"
    "${S3_BUCKET_SILVER:-silver}"
    "${S3_BUCKET_GOLD:-gold}"
    "${S3_BUCKET_QUARANTINE:-quarantine}"
)

echo "🌊 Initializing MinIO buckets at $S3_ENDPOINT..."

# Use the minio/mc docker image to avoid local installation dependency
# We use the host network to reach localhost:9000 if running from host
# or we assume 'minio' hostname if running inside the docker network.
# For simplicity, we'll try to detect if we're inside docker or use localhost.

MC_COMMAND="docker run --rm --network host minio/mc"

# 1. Configure MC alias
$MC_COMMAND alias set pureflow "$S3_ENDPOINT" "$STORAGE_USER" "$STORAGE_PASSWORD"

# 2. Create Buckets
for BUCKET in "${BUCKETS[@]}"; do
    echo "🏗️ Checking bucket: $BUCKET"
    if ! $MC_COMMAND ls "pureflow/$BUCKET" > /dev/null 2>&1; then
        echo "   ✨ Creating bucket: $BUCKET"
        $MC_COMMAND mb "pureflow/$BUCKET"
    else
        echo "   ✅ Bucket already exists: $BUCKET"
    fi
done

echo "✅ MinIO initialization complete."
