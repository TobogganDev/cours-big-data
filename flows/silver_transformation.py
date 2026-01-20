from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

from config import (
    BUCKET_BRONZE,
    BUCKET_SILVER,
    SCHEMAS,
    VALIDATION_RULES,
    get_minio_client,
    ensure_bucket_exists,
    calculate_data_hash,
    get_processing_metadata,
    save_processing_metadata,
)


@task(name="List Bronze Objects", retries=1)
def list_bronze_objects() -> list[str]:
    """
    List all objects in the bronze bucket.

    Returns:
        List of object names in the bronze bucket.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    objects = []
    for obj in client.list_objects(BUCKET_BRONZE):
        if obj.object_name.endswith(".csv"):
            objects.append(obj.object_name)

    prefect_logger.info(f"Found {len(objects)} CSV files in bronze bucket")
    return objects


@task(name="Check Silver Freshness", retries=1)
def check_silver_freshness(bronze_object: str, force: bool = False) -> dict:
    """
    Check if silver data needs to be refreshed based on bronze data changes.

    Args:
        bronze_object: Name of the bronze object.
        force: Force reprocessing.

    Returns:
        Freshness check result with should_process flag.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    silver_object = bronze_object.replace(".csv", ".parquet")

    result = {
        "bronze_object": bronze_object,
        "silver_object": silver_object,
        "should_process": True,
        "reason": "new_data"
    }

    if force:
        result["reason"] = "force_refresh"
        prefect_logger.info(f"{bronze_object}: Force refresh enabled")
        return result

    # Get bronze metadata
    bronze_metadata = get_processing_metadata(client, BUCKET_BRONZE, bronze_object)

    # Get silver metadata
    silver_metadata = get_processing_metadata(client, BUCKET_SILVER, silver_object)

    if not bronze_metadata:
        result["reason"] = "no_bronze_metadata"
        prefect_logger.info(f"{bronze_object}: No bronze metadata, will process")
        return result

    if not silver_metadata:
        result["reason"] = "no_silver_data"
        prefect_logger.info(f"{bronze_object}: No silver data exists, will process")
        return result

    # Compare source hashes
    bronze_hash = bronze_metadata.get("source_hash")
    silver_source_hash = silver_metadata.get("source_hash")

    if bronze_hash != silver_source_hash:
        result["reason"] = "source_changed"
        prefect_logger.info(f"{bronze_object}: Source data changed, will reprocess")
        return result

    # Data is fresh
    result["should_process"] = False
    result["reason"] = "data_is_fresh"
    prefect_logger.info(f"{bronze_object}: Silver data is up to date, skipping")

    return result


@task(name="Read Bronze Data", retries=2)
def read_bronze_data(object_name: str) -> tuple[pd.DataFrame, str]:
    """
    Read CSV data from the bronze bucket.

    Args:
        object_name: Name of the object in the bronze bucket.

    Returns:
        Tuple of (DataFrame, data_hash).
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    data_hash = calculate_data_hash(data)
    df = pd.read_csv(BytesIO(data))

    prefect_logger.info(f"Read {len(df)} rows from {BUCKET_BRONZE}/{object_name}")
    return df, data_hash


@task(name="Validate Schema")
def validate_schema(df: pd.DataFrame, entity_type: str) -> dict:
    """
    Validate DataFrame against expected schema.

    Args:
        df: DataFrame to validate.
        entity_type: Type of entity (clients, achats).

    Returns:
        Validation result.
    """
    prefect_logger = get_run_logger()

    result = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "columns_found": list(df.columns),
        "row_count": len(df)
    }

    if entity_type not in SCHEMAS:
        result["warnings"].append(f"Unknown entity type: {entity_type}")
        prefect_logger.warning(f"No schema defined for {entity_type}")
        return result

    schema = SCHEMAS[entity_type]

    # Check required columns
    missing_cols = set(schema["required_columns"]) - set(df.columns)
    if missing_cols:
        result["valid"] = False
        result["errors"].append(f"Missing required columns: {list(missing_cols)}")
        prefect_logger.error(f"Missing columns: {missing_cols}")

    # Check for extra columns (warning only)
    extra_cols = set(df.columns) - set(schema["required_columns"])
    if extra_cols:
        result["warnings"].append(f"Extra columns found: {list(extra_cols)}")

    prefect_logger.info(f"Schema validation: valid={result['valid']}")
    return result


@task(name="Clean Clients")
def clean_clients(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Clean client data with detailed quality metrics.

    Returns:
        Tuple of (cleaned DataFrame, quality metrics).
    """
    prefect_logger = get_run_logger()
    initial_count = len(df)

    quality_metrics = {
        "initial_count": initial_count,
        "duplicates_removed": 0,
        "nulls_removed": 0,
        "invalid_emails_removed": 0,
        "final_count": 0
    }

    # Remove duplicates on id_client
    before = len(df)
    df = df.drop_duplicates(subset=["id_client"], keep="first")
    quality_metrics["duplicates_removed"] = before - len(df)

    # Remove rows with null values in critical columns
    before = len(df)
    critical_columns = ["id_client", "nom", "email", "pays"]
    df = df.dropna(subset=critical_columns)
    quality_metrics["nulls_removed"] = before - len(df)

    # Validate email format (basic check)
    before = len(df)
    df = df[df["email"].str.contains("@", na=False)]
    quality_metrics["invalid_emails_removed"] = before - len(df)

    # Standardize country names (trim whitespace, title case)
    df["pays"] = df["pays"].str.strip().str.title()

    # Normalize types
    df["id_client"] = df["id_client"].astype(int)

    quality_metrics["final_count"] = len(df)
    quality_metrics["total_removed"] = initial_count - len(df)
    quality_metrics["removal_rate"] = round(
        (initial_count - len(df)) / initial_count * 100, 2
    ) if initial_count > 0 else 0

    prefect_logger.info(
        f"Clients cleaned: {initial_count} -> {len(df)} "
        f"({quality_metrics['removal_rate']}% removed)"
    )

    return df, quality_metrics


@task(name="Clean Achats")
def clean_achats(df: pd.DataFrame, valid_client_ids: set) -> tuple[pd.DataFrame, dict]:
    """
    Clean purchase data with detailed quality metrics.

    Returns:
        Tuple of (cleaned DataFrame, quality metrics).
    """
    prefect_logger = get_run_logger()
    initial_count = len(df)

    quality_metrics = {
        "initial_count": initial_count,
        "duplicates_removed": 0,
        "nulls_removed": 0,
        "invalid_amounts_removed": 0,
        "future_dates_removed": 0,
        "orphan_records_removed": 0,
        "final_count": 0
    }

    # Get validation rules
    rules = VALIDATION_RULES.get("achats", {})
    montant_min = rules.get("montant_min", 0)
    montant_max = rules.get("montant_max", 10000)

    # Remove duplicates on id_achat
    before = len(df)
    df = df.drop_duplicates(subset=["id_achat"], keep="first")
    quality_metrics["duplicates_removed"] = before - len(df)

    # Remove rows with null values
    before = len(df)
    df = df.dropna()
    quality_metrics["nulls_removed"] = before - len(df)

    # Normalize types
    df["id_achat"] = df["id_achat"].astype(int)
    df["id_client"] = df["id_client"].astype(int)
    df["montant"] = df["montant"].astype(float)

    # Filter out invalid amounts
    before = len(df)
    df = df[(df["montant"] > montant_min) & (df["montant"] <= montant_max)]
    quality_metrics["invalid_amounts_removed"] = before - len(df)

    # Convert and validate dates
    df["date_achat"] = pd.to_datetime(df["date_achat"], errors="coerce")
    df = df.dropna(subset=["date_achat"])

    # Filter out future dates
    before = len(df)
    today = pd.Timestamp(datetime.now().date())
    df = df[df["date_achat"] <= today]
    quality_metrics["future_dates_removed"] = before - len(df)

    # Validate foreign key
    before = len(df)
    df = df[df["id_client"].isin(valid_client_ids)]
    quality_metrics["orphan_records_removed"] = before - len(df)

    # Standardize product names
    df["produit"] = df["produit"].str.strip().str.title()

    quality_metrics["final_count"] = len(df)
    quality_metrics["total_removed"] = initial_count - len(df)
    quality_metrics["removal_rate"] = round(
        (initial_count - len(df)) / initial_count * 100, 2
    ) if initial_count > 0 else 0

    prefect_logger.info(
        f"Achats cleaned: {initial_count} -> {len(df)} "
        f"({quality_metrics['removal_rate']}% removed)"
    )

    return df, quality_metrics


@task(name="Standardize Dates")
def standardize_dates(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """
    Standardize date column to datetime format.
    """
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df.dropna(subset=[date_column])
    return df


@task(name="Save to Silver")
def save_to_silver(
    df: pd.DataFrame,
    object_name: str,
    source_hash: str,
    quality_metrics: dict
) -> str:
    """
    Save DataFrame to the silver bucket as Parquet with metadata.

    Args:
        df: DataFrame to save.
        object_name: Name of the object.
        source_hash: Hash of the source data.
        quality_metrics: Quality metrics from cleaning.

    Returns:
        Object name in the silver bucket.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    ensure_bucket_exists(client, BUCKET_SILVER)

    # Convert to Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to MinIO
    parquet_name = object_name.replace(".csv", ".parquet")
    client.put_object(
        BUCKET_SILVER,
        parquet_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    # Save processing metadata
    save_processing_metadata(
        client=client,
        object_name=parquet_name,
        source_hash=source_hash,
        row_count=len(df),
        status="transformed_to_silver",
        extra={
            "quality_metrics": quality_metrics,
            "layer": "silver",
            "format": "parquet"
        }
    )

    prefect_logger.info(f"Saved {len(df)} rows to {BUCKET_SILVER}/{parquet_name}")
    return parquet_name


@task(name="Generate Quality Report")
def generate_quality_report(
    clients_metrics: dict,
    achats_metrics: dict
) -> dict:
    """
    Generate a comprehensive data quality report.
    """
    prefect_logger = get_run_logger()

    report = {
        "timestamp": datetime.now().isoformat(),
        "pipeline_version": "2.0",
        "clients": clients_metrics,
        "achats": achats_metrics,
        "summary": {
            "total_input_records": (
                clients_metrics.get("initial_count", 0) +
                achats_metrics.get("initial_count", 0)
            ),
            "total_output_records": (
                clients_metrics.get("final_count", 0) +
                achats_metrics.get("final_count", 0)
            ),
            "overall_removal_rate": 0
        }
    }

    total_input = report["summary"]["total_input_records"]
    total_output = report["summary"]["total_output_records"]
    if total_input > 0:
        report["summary"]["overall_removal_rate"] = round(
            (total_input - total_output) / total_input * 100, 2
        )

    prefect_logger.info("\n=== Data Quality Report ===")
    prefect_logger.info(f"Clients: {clients_metrics.get('final_count', 0)}/{clients_metrics.get('initial_count', 0)} kept")
    prefect_logger.info(f"  - Duplicates removed: {clients_metrics.get('duplicates_removed', 0)}")
    prefect_logger.info(f"  - Nulls removed: {clients_metrics.get('nulls_removed', 0)}")
    prefect_logger.info(f"  - Invalid emails: {clients_metrics.get('invalid_emails_removed', 0)}")
    prefect_logger.info(f"Achats: {achats_metrics.get('final_count', 0)}/{achats_metrics.get('initial_count', 0)} kept")
    prefect_logger.info(f"  - Duplicates removed: {achats_metrics.get('duplicates_removed', 0)}")
    prefect_logger.info(f"  - Invalid amounts: {achats_metrics.get('invalid_amounts_removed', 0)}")
    prefect_logger.info(f"  - Orphan records: {achats_metrics.get('orphan_records_removed', 0)}")

    return report


@flow(name="Silver Transformation Flow", retries=1)
def silver_transformation_flow(force: bool = False) -> dict:
    """
    Robust flow to transform bronze data into silver layer.

    Features:
    - Incremental processing (skip unchanged data)
    - Schema validation
    - Detailed quality metrics
    - Processing metadata tracking

    Args:
        force: Force reprocessing of all data.

    Returns:
        Processing results dictionary.
    """
    prefect_logger = get_run_logger()
    prefect_logger.info(f"Starting Silver Transformation Flow (force={force})")

    results = {
        "processed": [],
        "skipped": [],
        "errors": [],
        "quality_report": None
    }

    # Check freshness for clients
    clients_freshness = check_silver_freshness("clients.csv", force=force)
    achats_freshness = check_silver_freshness("achats.csv", force=force)

    # If both are fresh, skip processing
    if not clients_freshness["should_process"] and not achats_freshness["should_process"]:
        prefect_logger.info("All silver data is up to date, nothing to process")
        results["skipped"] = ["clients.csv", "achats.csv"]
        return results

    try:
        # Read bronze data
        clients_bronze, clients_hash = read_bronze_data("clients.csv")
        achats_bronze, achats_hash = read_bronze_data("achats.csv")

        # Validate schemas
        clients_schema_result = validate_schema(clients_bronze, "clients")
        achats_schema_result = validate_schema(achats_bronze, "achats")

        if not clients_schema_result["valid"]:
            raise ValueError(f"Clients schema validation failed: {clients_schema_result['errors']}")
        if not achats_schema_result["valid"]:
            raise ValueError(f"Achats schema validation failed: {achats_schema_result['errors']}")

        # Clean clients
        clients_clean, clients_metrics = clean_clients(clients_bronze)
        clients_clean = standardize_dates(clients_clean, "date_inscription")

        # Get valid client IDs for FK validation
        valid_client_ids = set(clients_clean["id_client"].tolist())

        # Clean achats
        achats_clean, achats_metrics = clean_achats(achats_bronze, valid_client_ids)

        # Save to silver
        silver_clients = save_to_silver(
            clients_clean, "clients.csv", clients_hash, clients_metrics
        )
        silver_achats = save_to_silver(
            achats_clean, "achats.csv", achats_hash, achats_metrics
        )

        # Generate quality report
        quality_report = generate_quality_report(clients_metrics, achats_metrics)

        results["processed"] = [
            {"name": silver_clients, "rows": len(clients_clean)},
            {"name": silver_achats, "rows": len(achats_clean)}
        ]
        results["quality_report"] = quality_report

    except Exception as e:
        prefect_logger.error(f"Silver transformation failed: {e}")
        results["errors"].append(str(e))
        raise

    prefect_logger.info("Silver Transformation Complete")
    return results


if __name__ == "__main__":
    result = silver_transformation_flow()
    print("\nSilver transformation completed:")
    print(f"  Processed: {result['processed']}")
    print(f"  Skipped: {result['skipped']}")
    if result['quality_report']:
        print(f"  Quality Report: {result['quality_report']['summary']}")
