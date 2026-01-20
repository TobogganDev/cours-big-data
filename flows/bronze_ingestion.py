from io import BytesIO
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

from config import (
    BUCKET_BRONZE,
    BUCKET_SOURCES,
    SCHEMAS,
    get_minio_client,
    ensure_bucket_exists,
    calculate_file_hash,
    get_processing_metadata,
    save_processing_metadata,
    move_to_quarantine,
)


@task(name="Discover Source Files", retries=1)
def discover_source_files(data_dir: str, patterns: Optional[list[str]] = None) -> list[dict]:
    """
    Discover all source files in the data directory.
    Supports dynamic file discovery instead of hardcoded filenames.

    Args:
        data_dir: Path to the data directory.
        patterns: List of glob patterns to match (default: ["*.csv"]).

    Returns:
        List of file info dictionaries with path, name, and hash.
    """
    prefect_logger = get_run_logger()
    data_path = Path(data_dir)

    if patterns is None:
        patterns = ["*.csv"]

    files = []
    for pattern in patterns:
        for file_path in data_path.glob(pattern):
            if file_path.is_file():
                file_hash = calculate_file_hash(str(file_path))
                files.append({
                    "path": str(file_path),
                    "name": file_path.name,
                    "hash": file_hash,
                    "size": file_path.stat().st_size
                })
                prefect_logger.info(f"Discovered file: {file_path.name} (hash: {file_hash[:8]}...)")

    prefect_logger.info(f"Total files discovered: {len(files)}")
    return files


@task(name="Check Idempotency", retries=1)
def check_idempotency(file_info: dict, force: bool = False) -> dict:
    """
    Check if file has already been processed with the same hash.

    Args:
        file_info: File information dictionary.
        force: Force reprocessing even if already processed.

    Returns:
        Updated file info with should_process flag.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    if force:
        file_info["should_process"] = True
        file_info["reason"] = "force_reprocess"
        prefect_logger.info(f"{file_info['name']}: Force reprocessing enabled")
        return file_info

    # Check existing metadata
    existing_metadata = get_processing_metadata(client, BUCKET_BRONZE, file_info["name"])

    if existing_metadata:
        if existing_metadata.get("source_hash") == file_info["hash"]:
            file_info["should_process"] = False
            file_info["reason"] = "already_processed_same_hash"
            prefect_logger.info(f"{file_info['name']}: Already processed with same hash, skipping")
        else:
            file_info["should_process"] = True
            file_info["reason"] = "hash_changed"
            prefect_logger.info(f"{file_info['name']}: Hash changed, will reprocess")
    else:
        file_info["should_process"] = True
        file_info["reason"] = "new_file"
        prefect_logger.info(f"{file_info['name']}: New file, will process")

    return file_info


@task(name="Infer Schema", retries=1)
def infer_schema(file_path: str) -> dict:
    """
    Infer schema from CSV file and compare with expected schema.

    Args:
        file_path: Path to the CSV file.

    Returns:
        Schema information with validation status.
    """
    prefect_logger = get_run_logger()

    # Read a sample of the file to infer schema
    df_sample = pd.read_csv(file_path, nrows=100)

    inferred_schema = {
        "columns": list(df_sample.columns),
        "dtypes": {col: str(dtype) for col, dtype in df_sample.dtypes.items()},
        "row_count_sample": len(df_sample)
    }

    # Determine entity type from filename
    file_name = Path(file_path).stem
    entity_type = None
    for key in SCHEMAS.keys():
        if key in file_name.lower():
            entity_type = key
            break

    inferred_schema["entity_type"] = entity_type

    # Validate against expected schema if entity type is known
    if entity_type and entity_type in SCHEMAS:
        expected = SCHEMAS[entity_type]
        missing_cols = set(expected["required_columns"]) - set(inferred_schema["columns"])
        extra_cols = set(inferred_schema["columns"]) - set(expected["required_columns"])

        inferred_schema["schema_valid"] = len(missing_cols) == 0
        inferred_schema["missing_columns"] = list(missing_cols)
        inferred_schema["extra_columns"] = list(extra_cols)

        if missing_cols:
            prefect_logger.warning(f"Missing required columns: {missing_cols}")
        if extra_cols:
            prefect_logger.info(f"Extra columns detected: {extra_cols}")
    else:
        # Unknown entity type - accept but flag
        inferred_schema["schema_valid"] = True
        inferred_schema["missing_columns"] = []
        inferred_schema["extra_columns"] = []
        prefect_logger.warning(f"Unknown entity type for {file_name}, accepting schema as-is")

    prefect_logger.info(f"Schema inferred: {len(inferred_schema['columns'])} columns, valid={inferred_schema['schema_valid']}")
    return inferred_schema


@task(name="Validate File Content", retries=1)
def validate_file_content(file_path: str, schema_info: dict) -> dict:
    """
    Validate file content beyond schema: check for corruption, encoding, etc.

    Args:
        file_path: Path to the CSV file.
        schema_info: Schema information from infer_schema.

    Returns:
        Validation result with status and errors.
    """
    prefect_logger = get_run_logger()
    validation = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "row_count": 0
    }

    try:
        # Try to read the entire file
        df = pd.read_csv(file_path)
        validation["row_count"] = len(df)

        # Check for empty file
        if len(df) == 0:
            validation["valid"] = False
            validation["errors"].append("File is empty")
            return validation

        # Check for completely null columns
        null_columns = df.columns[df.isnull().all()].tolist()
        if null_columns:
            validation["warnings"].append(f"Completely null columns: {null_columns}")

        # Check for high null rate in required columns
        if schema_info.get("entity_type") in SCHEMAS:
            required_cols = SCHEMAS[schema_info["entity_type"]]["required_columns"]
            for col in required_cols:
                if col in df.columns:
                    null_rate = df[col].isnull().sum() / len(df)
                    if null_rate > 0.5:
                        validation["warnings"].append(f"Column {col} has {null_rate:.1%} null values")

        # Check for duplicate primary keys
        entity_type = schema_info.get("entity_type")
        pk_columns = {
            "clients": "id_client",
            "achats": "id_achat"
        }
        if entity_type in pk_columns and pk_columns[entity_type] in df.columns:
            pk_col = pk_columns[entity_type]
            dup_count = df[pk_col].duplicated().sum()
            if dup_count > 0:
                validation["warnings"].append(f"{dup_count} duplicate values in {pk_col}")

        prefect_logger.info(f"Validation complete: valid={validation['valid']}, rows={validation['row_count']}")

    except Exception as e:
        validation["valid"] = False
        validation["errors"].append(f"Failed to read file: {str(e)}")
        prefect_logger.error(f"Validation failed: {e}")

    return validation


@task(name="Upload to Sources", retries=2)
def upload_to_sources(file_info: dict) -> str:
    """
    Upload local file to MinIO sources bucket.

    Args:
        file_info: File information dictionary.

    Returns:
        Object name in the sources bucket.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    ensure_bucket_exists(client, BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, file_info["name"], file_info["path"])
    prefect_logger.info(f"Uploaded {file_info['name']} to {BUCKET_SOURCES}")

    return file_info["name"]


@task(name="Copy to Bronze Layer", retries=2)
def copy_to_bronze_layer(
    object_name: str,
    file_info: dict,
    schema_info: dict,
    validation: dict
) -> Optional[str]:
    """
    Copy file from sources to bronze bucket with metadata.
    Quarantine invalid files.

    Args:
        object_name: Name of the object in sources.
        file_info: File information dictionary.
        schema_info: Schema information.
        validation: Validation results.

    Returns:
        Object name in bronze bucket, or None if quarantined.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    # Check if file should be quarantined
    if not schema_info.get("schema_valid", True):
        reason = f"Schema validation failed: missing columns {schema_info.get('missing_columns', [])}"
        move_to_quarantine(client, BUCKET_SOURCES, object_name, reason)
        return None

    if not validation.get("valid", True):
        reason = f"Content validation failed: {validation.get('errors', [])}"
        move_to_quarantine(client, BUCKET_SOURCES, object_name, reason)
        return None

    # Copy to bronze
    ensure_bucket_exists(client, BUCKET_BRONZE)

    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    client.put_object(BUCKET_BRONZE, object_name, BytesIO(data), length=len(data))

    # Save processing metadata
    save_processing_metadata(
        client=client,
        object_name=object_name,
        source_hash=file_info["hash"],
        row_count=validation.get("row_count", 0),
        status="ingested_to_bronze",
        extra={
            "schema": schema_info,
            "validation_warnings": validation.get("warnings", []),
            "layer": "bronze"
        }
    )

    prefect_logger.info(f"Copied {object_name} to {BUCKET_BRONZE}")
    return object_name


@flow(name="Bronze Ingestion Flow", retries=1)
def bronze_ingestion_flow(
    data_dir: str = "./data/sources",
    force: bool = False,
    patterns: Optional[list[str]] = None
) -> dict:
    """
    Robust flow to ingest data into the bronze layer.

    Features:
    - Dynamic file discovery
    - Schema inference and validation
    - Idempotency (skip already processed files)
    - Quarantine for invalid files
    - Processing metadata tracking

    Args:
        data_dir: Directory containing source files.
        force: Force reprocessing of all files.
        patterns: File patterns to match (default: ["*.csv"]).

    Returns:
        Processing results dictionary.
    """
    prefect_logger = get_run_logger()
    prefect_logger.info(f"Starting Bronze Ingestion Flow (force={force})")

    # Discover all source files
    files = discover_source_files(data_dir, patterns)

    if not files:
        prefect_logger.warning("No source files found!")
        return {"processed": [], "skipped": [], "quarantined": [], "errors": []}

    results = {
        "processed": [],
        "skipped": [],
        "quarantined": [],
        "errors": []
    }

    for file_info in files:
        try:
            # Check idempotency
            file_info = check_idempotency(file_info, force=force)

            if not file_info.get("should_process", True):
                results["skipped"].append({
                    "name": file_info["name"],
                    "reason": file_info.get("reason", "unknown")
                })
                continue

            # Infer and validate schema
            schema_info = infer_schema(file_info["path"])

            # Validate file content
            validation = validate_file_content(file_info["path"], schema_info)

            # Upload to sources
            object_name = upload_to_sources(file_info)

            # Copy to bronze (or quarantine)
            bronze_name = copy_to_bronze_layer(object_name, file_info, schema_info, validation)

            if bronze_name:
                results["processed"].append({
                    "name": bronze_name,
                    "rows": validation.get("row_count", 0),
                    "schema": schema_info.get("entity_type", "unknown"),
                    "warnings": validation.get("warnings", [])
                })
            else:
                results["quarantined"].append({
                    "name": file_info["name"],
                    "reason": "validation_failed"
                })

        except Exception as e:
            prefect_logger.error(f"Error processing {file_info['name']}: {e}")
            results["errors"].append({
                "name": file_info["name"],
                "error": str(e)
            })

    # Summary
    prefect_logger.info(f"Bronze Ingestion Complete:")
    prefect_logger.info(f"  Processed: {len(results['processed'])}")
    prefect_logger.info(f"  Skipped: {len(results['skipped'])}")
    prefect_logger.info(f"  Quarantined: {len(results['quarantined'])}")
    prefect_logger.info(f"  Errors: {len(results['errors'])}")

    return results


if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print("\nBronze ingestion completed:")
    print(f"  Processed: {result['processed']}")
    print(f"  Skipped: {result['skipped']}")
    print(f"  Quarantined: {result['quarantined']}")
    print(f"  Errors: {result['errors']}")
