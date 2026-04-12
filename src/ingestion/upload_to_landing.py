"""
Official Ingestion Script for MinIO Landing Zone.
Handles secure uploads of local raw files to the object storage using S3 protocols.
"""
import os
import boto3
from botocore.exceptions import ClientError
from core.logger import logger
from core.connection import ConnectionFactory
from dotenv import load_dotenv

load_dotenv()

# Configuration - Credentials from environment variables as requested
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("STORAGE_USER", "admin")
S3_SECRET_KEY = os.getenv("STORAGE_PASSWORD", "password123")
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
        config=boto3.session.Config(signature_version="s3v4")
    )

def upload_to_landing(local_file_path: str, target_key: str):
    """
    Uploads a file to the landing zone bucket with metadata for traceability.
    
    Args:
        local_file_path: Absolute or relative path to the local source file.
        target_key: The destination path (key) within the 'landing-zone' bucket.
    """
    s3_client = get_s3_client()
    
    # Production-standard: Check local existence before attempting upload
    if not os.path.isfile(local_file_path):
        logger.error("Local file not found: %s", local_file_path)
        return False

    try:
        logger.info("🚀 Initiating upload: %s -> s3://%s/%s", 
                    local_file_path, S3_BUCKET, target_key)
        
        # Upload with analyst context metadata to satisfy Task 1 requirement
        s3_client.upload_file(
            local_file_path,
            S3_BUCKET,
            target_key,
            ExtraArgs={
                "Metadata": {
                    "uploaded_by": USER_CONTEXT,
                    "ingestion_method": "official_upload_script",
                    "source_system": "local_raw_ingestion"
                }
            }
        )
        logger.info("✔️ Upload successful! File indexed in MinIO metadata layer.")
        return True

    except ClientError as e:
        logger.error("❌ S3 Client Error during upload: %s", e)
        return False
    except Exception as e:
        logger.error("❌ Unexpected error: %s", e)
        return False

if __name__ == "__main__":
    # Ensure local directory exists for testing/operation
    os.makedirs("data/raw", exist_ok=True)
    
    # Example execution based on Task 1 requirements
    sample_source = "data/raw/vendas.csv"
    sample_target = "erp-sales/vendas.csv"
    
    if os.path.exists(sample_source):
        upload_to_landing(sample_source, sample_target)
    else:
        logger.warning("No file found at %s. Please place data there to ingest.", sample_source)
