#!/bin/bash
# Script to generate clean data and upload it to MinIO bucket automatically

# 1. Run the clean data generation
echo "🚀 Running generate_clean_data.py..."
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
poetry run python src/utils/generate_clean_data.py

# 2. Upload to MinIO
echo "📤 Uploading to MinIO bucket 'landing-zone'..."

# Use a temporary mc container connected to the project network
# Load variables from .env if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

docker run --rm --network pureflow-arch_pureflow_net \
  --entrypoint /bin/sh \
  -e STORAGE_USER=${STORAGE_USER} \
  -e STORAGE_PASSWORD=${STORAGE_PASSWORD} \
  -v "$(pwd)/data/minio_data/landing-zone:/data/minio_data/landing-zone" \
  minio/mc \
  -c "
  mc config host add myminio http://minio:9000 \$STORAGE_USER \$STORAGE_PASSWORD;
  mc cp --recursive /data/minio_data/landing-zone/ myminio/landing-zone/;
  echo '✅ Upload complete!'
  "

echo "✨ Done! Clean data generated and uploaded."
