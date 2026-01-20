import os
import hashlib
import json
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("big-data-pipeline")

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# Database configuration
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/database/analytics.db")

# Prefect configuration
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://localhost:4200/api")

# Buckets
BUCKET_SOURCES = "sources"
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"
BUCKET_QUARANTINE = "quarantine"
BUCKET_METADATA = "metadata"

# Expected schemas for validation
SCHEMAS = {
    "clients": {
        "required_columns": ["id_client", "nom", "email", "date_inscription", "pays"],
        "types": {
            "id_client": "int64",
            "nom": "object",
            "email": "object",
            "date_inscription": "object",
            "pays": "object"
        }
    },
    "achats": {
        "required_columns": ["id_achat", "id_client", "date_achat", "montant", "produit"],
        "types": {
            "id_achat": "int64",
            "id_client": "int64",
            "date_achat": "object",
            "montant": "float64",
            "produit": "object"
        }
    }
}

# Validation rules
VALIDATION_RULES = {
    "achats": {
        "montant_min": 0,
        "montant_max": 10000
    }
}


def get_minio_client() -> Minio:
    """Initialize and return a MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Create bucket if it doesn't exist."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")


def calculate_file_hash(file_path: str) -> str:
    """Calculate MD5 hash of a file for idempotency checks."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def calculate_data_hash(data: bytes) -> str:
    """Calculate MD5 hash of bytes data."""
    return hashlib.md5(data).hexdigest()


def get_processing_metadata(client: Minio, bucket: str, object_name: str) -> Optional[dict]:
    """Retrieve processing metadata for an object."""
    metadata_key = f"{object_name}.metadata.json"
    try:
        response = client.get_object(BUCKET_METADATA, metadata_key)
        data = response.read()
        response.close()
        response.release_conn()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return None


def save_processing_metadata(
    client: Minio,
    object_name: str,
    source_hash: str,
    row_count: int,
    status: str = "processed",
    extra: Optional[dict] = None
) -> None:
    """Save processing metadata for tracking."""
    ensure_bucket_exists(client, BUCKET_METADATA)

    metadata = {
        "object_name": object_name,
        "source_hash": source_hash,
        "row_count": row_count,
        "status": status,
        "processed_at": datetime.now().isoformat(),
        "pipeline_version": "2.0"
    }
    if extra:
        metadata.update(extra)

    metadata_json = json.dumps(metadata, indent=2).encode("utf-8")
    metadata_key = f"{object_name}.metadata.json"

    client.put_object(
        BUCKET_METADATA,
        metadata_key,
        BytesIO(metadata_json),
        length=len(metadata_json),
        content_type="application/json"
    )
    logger.info(f"Saved metadata for {object_name}")


def move_to_quarantine(
    client: Minio,
    source_bucket: str,
    object_name: str,
    reason: str
) -> str:
    """Move invalid file to quarantine bucket with reason."""
    ensure_bucket_exists(client, BUCKET_QUARANTINE)

    # Get the object data
    response = client.get_object(source_bucket, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    # Create quarantine path with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    quarantine_name = f"{timestamp}/{object_name}"

    # Upload to quarantine
    client.put_object(
        BUCKET_QUARANTINE,
        quarantine_name,
        BytesIO(data),
        length=len(data)
    )

    # Save quarantine metadata
    quarantine_metadata = {
        "original_bucket": source_bucket,
        "original_name": object_name,
        "reason": reason,
        "quarantined_at": datetime.now().isoformat()
    }
    metadata_json = json.dumps(quarantine_metadata, indent=2).encode("utf-8")
    client.put_object(
        BUCKET_QUARANTINE,
        f"{quarantine_name}.reason.json",
        BytesIO(metadata_json),
        length=len(metadata_json),
        content_type="application/json"
    )

    logger.warning(f"Moved {object_name} to quarantine: {reason}")
    return quarantine_name


def configure_prefect() -> None:
    os.environ["PREFECT_API_URL"] = PREFECT_API_URL


if __name__ == "__main__":
    client = get_minio_client()
    print("MinIO client configured:", client)
    print(client.list_buckets())

    configure_prefect()
    print(os.getenv("PREFECT_API_URL"))