#!/bin/bash
# Script to generate clean data and upload it to MinIO bucket automatically

# 1. Run the clean data generation
echo "🚀 Running generate_clean_data.py..."
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
poetry run python src/utils/generate_clean_data.py

# 2. Upload to MinIO
echo "📤 Uploading to MinIO bucket 'landing-zone'..."

# Use a temporary mc container connected to the project network
docker run --rm --network pureflow-arch_pureflow_net \
  -v "$(pwd)/data/minio_data/landing-zone:/data/minio_data/landing-zone" \
  minio/mc \
  /bin/sh -c "
  /usr/bin/mc config host add myminio http://minio:9000 admin strongpassword123;
  /usr/bin/mc cp --recursive /data/minio_data/landing-zone/ myminio/landing-zone/;
  echo '✅ Upload complete!'
  "

echo "✨ Done! Clean data generated and uploaded."
