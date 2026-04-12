"""
Official Ingestion Script for MinIO Landing Zone.
Handles secure uploads of local raw files to the object storage using S3 protocols.
"""
import os
import boto3
from botocore.exceptions import ClientError
from core.logger import logger
from dotenv import load_dotenv

# Load .env but we will be careful with its values
load_dotenv()

def get_config():
    """Dynamically determines the correct endpoint based on the environment."""
    
    # 1. Check if we are inside a container first
    is_container = os.path.exists("/.dockerenv") or os.getenv("AIRFLOW_HOME") is not None
    
    env_endpoint = os.getenv("S3_ENDPOINT")
    
    # If we are in a container and S3_ENDPOINT is either unset or set to localhost,
    # we MUST use the container hostname 'minio'.
    if is_container:
        if not env_endpoint or "localhost" in env_endpoint or "127.0.0.1" in env_endpoint:
            logger.info("📦 Container detected: Routing S3 traffic to 'http://minio:9000'")
            return "http://minio:9000"
        return env_endpoint
    
    # 2. Outside container: Use .env or default to localhost
    return env_endpoint if env_endpoint else "http://localhost:9000"

S3_ENDPOINT = get_config()
S3_ACCESS_KEY = os.getenv("STORAGE_USER", "admin")
S3_SECRET_KEY = os.getenv("STORAGE_PASSWORD", "strongpassword123")
S3_BUCKET = "landing-zone"
USER_CONTEXT = "analyst"

def get_s3_client():
    """Returns a boto3 client configured for the MinIO instance."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        use_ssl=False,
        config=boto3.session.Config(
            signature_version="s3v4",
            s3={'addressing_style': 'path'}
        )
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Checks if bucket exists, creates it if it doesn't."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        try:
            logger.info("🪣 Bucket '%s' not found. Creating...", bucket_name)
            s3_client.create_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.warning("Could not create bucket (it might already exist): %s", str(e))

def upload_to_landing(local_file_path: str, target_key: str):
    """
    Uploads a file to the landing zone bucket with metadata for traceability.
    """
    s3_client = get_s3_client()
    
    if not os.path.isfile(local_file_path):
        logger.error("Local file not found: %s", local_file_path)
        return False

    try:
        ensure_bucket_exists(s3_client, S3_BUCKET)

        logger.info("🚀 Uploading: %s -> s3://%s/%s", 
                    local_file_path, S3_BUCKET, target_key)
        
        s3_client.upload_file(
            local_file_path,
            S3_BUCKET,
            target_key,
            ExtraArgs={
                "Metadata": {
                    "uploaded_by": USER_CONTEXT,
                    "ingestion_method": "official_upload_script"
                }
            }
        )
        logger.info("✔️ Success! File visible in MinIO UI.")
        return True

    except Exception as e:
        logger.error("❌ MinIO Upload Error (Endpoint: %s): %s", S3_ENDPOINT, str(e))
        raise  # Re-raise the exception to fail the calling task

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    upload_to_landing("data/raw/vendas.csv", "erp-sales/vendas.csv")
