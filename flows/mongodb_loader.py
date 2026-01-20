from io import BytesIO
from datetime import datetime
from typing import Optional

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger
from pymongo import UpdateOne

from config import (
    BUCKET_GOLD,
    get_minio_client,
    get_mongo_database,
    get_processing_metadata,
    save_processing_metadata,
    calculate_data_hash,
)


# MongoDB collection names mapping
GOLD_TO_MONGO_MAPPING = {
    # Dimensions
    "dim_clients.parquet": "dim_clients",
    "dim_produits.parquet": "dim_produits",
    "dim_temps.parquet": "dim_temps",
    # Facts
    "fact_ventes.parquet": "fact_ventes",
    # KPIs
    "kpi_ca_par_jour.parquet": "kpi_ca_par_jour",
    "kpi_ca_par_mois.parquet": "kpi_ca_par_mois",
    "kpi_ca_par_pays.parquet": "kpi_ca_par_pays",
    "kpi_volume_par_produit.parquet": "kpi_volume_par_produit",
    "kpi_top_clients.parquet": "kpi_top_clients",
    "kpi_stats_distribution.parquet": "kpi_stats_distribution",
}

# Primary keys for upsert operations
PRIMARY_KEYS = {
    "dim_clients": "id_client",
    "dim_produits": "id_produit",
    "dim_temps": "id_date",
    "fact_ventes": "id_achat",
    "kpi_ca_par_jour": "date",
    "kpi_ca_par_mois": "mois",
    "kpi_ca_par_pays": "pays",
    "kpi_volume_par_produit": "produit",
    "kpi_top_clients": "id_client",
    "kpi_stats_distribution": "produit",
}


@task(name="List Gold Objects", retries=1)
def list_gold_objects() -> list[str]:
    """
    List all parquet files in the gold bucket.

    Returns:
        List of object names.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    objects = []
    for obj in client.list_objects(BUCKET_GOLD):
        if obj.object_name.endswith(".parquet"):
            objects.append(obj.object_name)

    prefect_logger.info(f"Found {len(objects)} parquet files in gold bucket")
    return objects


@task(name="Check MongoDB Freshness", retries=1)
def check_mongodb_freshness(gold_object: str, force: bool = False) -> dict:
    """
    Check if MongoDB data needs to be refreshed based on gold data changes.

    Args:
        gold_object: Name of the gold object.
        force: Force reload.

    Returns:
        Freshness check result.
    """
    prefect_logger = get_run_logger()
    minio_client = get_minio_client()
    db = get_mongo_database()

    collection_name = GOLD_TO_MONGO_MAPPING.get(gold_object)
    if not collection_name:
        return {
            "gold_object": gold_object,
            "should_process": False,
            "reason": "unknown_mapping"
        }

    result = {
        "gold_object": gold_object,
        "collection_name": collection_name,
        "should_process": True,
        "reason": "new_data"
    }

    if force:
        result["reason"] = "force_refresh"
        prefect_logger.info(f"{gold_object}: Force refresh enabled")
        return result

    # Get gold metadata
    gold_metadata = get_processing_metadata(minio_client, BUCKET_GOLD, gold_object)
    if not gold_metadata:
        result["reason"] = "no_gold_metadata"
        prefect_logger.info(f"{gold_object}: No gold metadata, will load")
        return result

    gold_hash = gold_metadata.get("source_hash")

    # Check MongoDB sync metadata
    sync_collection = db["_sync_metadata"]
    sync_doc = sync_collection.find_one({"collection_name": collection_name})

    if not sync_doc:
        result["reason"] = "no_mongo_data"
        prefect_logger.info(f"{gold_object}: No MongoDB data exists, will load")
        return result

    mongo_hash = sync_doc.get("source_hash")

    if gold_hash != mongo_hash:
        result["reason"] = "source_changed"
        prefect_logger.info(f"{gold_object}: Gold data changed, will reload")
        return result

    # Data is fresh
    result["should_process"] = False
    result["reason"] = "data_is_fresh"
    prefect_logger.info(f"{gold_object}: MongoDB data is up to date, skipping")

    return result


@task(name="Read Gold Data", retries=2)
def read_gold_data(object_name: str) -> tuple[pd.DataFrame, str]:
    """
    Read Parquet data from the gold bucket.

    Args:
        object_name: Name of the object in the gold bucket.

    Returns:
        Tuple of (DataFrame, data_hash).
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    response = client.get_object(BUCKET_GOLD, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    data_hash = calculate_data_hash(data)
    df = pd.read_parquet(BytesIO(data))

    prefect_logger.info(f"Read {len(df)} rows from {BUCKET_GOLD}/{object_name}")
    return df, data_hash


@task(name="Convert DataFrame to Documents", retries=1)
def convert_to_documents(df: pd.DataFrame, collection_name: str) -> list[dict]:
    """
    Convert pandas DataFrame to MongoDB documents.
    Handles datetime conversion and NaN values.

    Args:
        df: DataFrame to convert.
        collection_name: Name of the target collection.

    Returns:
        List of documents ready for MongoDB.
    """
    prefect_logger = get_run_logger()

    # Convert datetime columns to Python datetime
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.to_pydatetime()
        elif df[col].dtype == "object":
            # Try to convert string dates
            try:
                df[col] = pd.to_datetime(df[col]).dt.to_pydatetime()
            except (ValueError, TypeError):
                pass

    # Convert to records, replacing NaN with None
    documents = df.where(pd.notnull(df), None).to_dict(orient="records")

    prefect_logger.info(f"Converted {len(documents)} documents for {collection_name}")
    return documents


@task(name="Upsert to MongoDB", retries=2)
def upsert_to_mongodb(
    documents: list[dict],
    collection_name: str,
    primary_key: str
) -> dict:
    """
    Upsert documents to MongoDB collection.

    Args:
        documents: List of documents to upsert.
        collection_name: Target collection name.
        primary_key: Field to use as primary key for upsert.

    Returns:
        Upsert statistics.
    """
    prefect_logger = get_run_logger()
    db = get_mongo_database()

    collection = db[collection_name]

    # Create index on primary key if not exists
    collection.create_index(primary_key, unique=True, background=True)

    # Prepare bulk operations
    operations = []
    for doc in documents:
        pk_value = doc.get(primary_key)
        if pk_value is not None:
            operations.append(
                UpdateOne(
                    {primary_key: pk_value},
                    {"$set": doc},
                    upsert=True
                )
            )

    stats = {
        "collection": collection_name,
        "total_documents": len(documents),
        "operations_prepared": len(operations),
        "inserted": 0,
        "modified": 0,
        "upserted": 0
    }

    if operations:
        result = collection.bulk_write(operations, ordered=False)
        stats["inserted"] = result.inserted_count
        stats["modified"] = result.modified_count
        stats["upserted"] = result.upserted_count

    prefect_logger.info(
        f"Upserted to {collection_name}: "
        f"{stats['upserted']} inserted, {stats['modified']} modified"
    )

    return stats


@task(name="Save Sync Metadata", retries=1)
def save_sync_metadata(
    collection_name: str,
    gold_object: str,
    source_hash: str,
    row_count: int,
    stats: dict
) -> None:
    """
    Save synchronization metadata to MongoDB.

    Args:
        collection_name: Name of the synced collection.
        gold_object: Source gold object name.
        source_hash: Hash of the source data.
        row_count: Number of rows synced.
        stats: Upsert statistics.
    """
    prefect_logger = get_run_logger()
    db = get_mongo_database()

    sync_collection = db["_sync_metadata"]

    sync_doc = {
        "collection_name": collection_name,
        "gold_object": gold_object,
        "source_hash": source_hash,
        "row_count": row_count,
        "last_sync": datetime.now(),
        "stats": stats,
        "pipeline_version": "2.0"
    }

    sync_collection.update_one(
        {"collection_name": collection_name},
        {"$set": sync_doc},
        upsert=True
    )

    prefect_logger.info(f"Saved sync metadata for {collection_name}")


@task(name="Calculate Refresh Time", retries=1)
def calculate_refresh_time(gold_object: str) -> Optional[dict]:
    """
    Calculate the time between Gold layer update and MongoDB sync.

    Args:
        gold_object: Name of the gold object.

    Returns:
        Refresh time information or None.
    """
    prefect_logger = get_run_logger()
    minio_client = get_minio_client()
    db = get_mongo_database()

    # Get gold processing time
    gold_metadata = get_processing_metadata(minio_client, BUCKET_GOLD, gold_object)
    if not gold_metadata:
        return None

    gold_processed_at = gold_metadata.get("processed_at")
    if not gold_processed_at:
        return None

    gold_time = datetime.fromisoformat(gold_processed_at)

    # Get MongoDB sync time
    collection_name = GOLD_TO_MONGO_MAPPING.get(gold_object)
    if not collection_name:
        return None

    sync_collection = db["_sync_metadata"]
    sync_doc = sync_collection.find_one({"collection_name": collection_name})

    if not sync_doc:
        return None

    mongo_time = sync_doc.get("last_sync")
    if not mongo_time:
        return None

    # Calculate refresh time
    refresh_delta = mongo_time - gold_time
    refresh_seconds = refresh_delta.total_seconds()

    result = {
        "gold_object": gold_object,
        "collection_name": collection_name,
        "gold_processed_at": gold_time.isoformat(),
        "mongo_synced_at": mongo_time.isoformat(),
        "refresh_time_seconds": round(refresh_seconds, 2),
        "refresh_time_human": str(refresh_delta)
    }

    prefect_logger.info(
        f"Refresh time for {collection_name}: {result['refresh_time_human']}"
    )

    return result


@flow(name="MongoDB Loader Flow", retries=1)
def mongodb_loader_flow(
    force: bool = False,
    collections: Optional[list[str]] = None
) -> dict:
    """
    Flow to load gold layer data into MongoDB.

    Features:
    - Incremental loading (skip unchanged data)
    - Upsert logic based on primary keys
    - Sync metadata tracking
    - Refresh time calculation

    Args:
        force: Force reload of all data.
        collections: Specific collections to load (None = all).

    Returns:
        Processing results dictionary.
    """
    prefect_logger = get_run_logger()
    prefect_logger.info(f"Starting MongoDB Loader Flow (force={force})")

    results = {
        "loaded": [],
        "skipped": [],
        "errors": [],
        "refresh_times": []
    }

    # Get gold objects to process
    gold_objects = list_gold_objects()

    # Filter if specific collections requested
    if collections:
        gold_objects = [
            obj for obj in gold_objects
            if GOLD_TO_MONGO_MAPPING.get(obj) in collections
        ]

    for gold_object in gold_objects:
        try:
            # Check freshness
            freshness = check_mongodb_freshness(gold_object, force=force)

            if not freshness.get("should_process", False):
                results["skipped"].append({
                    "object": gold_object,
                    "reason": freshness.get("reason", "unknown")
                })
                continue

            collection_name = freshness["collection_name"]
            primary_key = PRIMARY_KEYS.get(collection_name)

            if not primary_key:
                prefect_logger.warning(
                    f"No primary key defined for {collection_name}, skipping"
                )
                results["skipped"].append({
                    "object": gold_object,
                    "reason": "no_primary_key"
                })
                continue

            # Read gold data
            df, data_hash = read_gold_data(gold_object)

            # Convert to documents
            documents = convert_to_documents(df, collection_name)

            # Upsert to MongoDB
            stats = upsert_to_mongodb(documents, collection_name, primary_key)

            # Save sync metadata
            save_sync_metadata(
                collection_name=collection_name,
                gold_object=gold_object,
                source_hash=data_hash,
                row_count=len(df),
                stats=stats
            )

            # Calculate refresh time
            refresh_info = calculate_refresh_time(gold_object)
            if refresh_info:
                results["refresh_times"].append(refresh_info)

            results["loaded"].append({
                "object": gold_object,
                "collection": collection_name,
                "rows": len(df),
                "stats": stats
            })

        except Exception as e:
            prefect_logger.error(f"Error loading {gold_object}: {e}")
            results["errors"].append({
                "object": gold_object,
                "error": str(e)
            })

    # Summary
    prefect_logger.info("MongoDB Loader Complete:")
    prefect_logger.info(f"  Loaded: {len(results['loaded'])} collections")
    prefect_logger.info(f"  Skipped: {len(results['skipped'])} collections")
    prefect_logger.info(f"  Errors: {len(results['errors'])}")

    if results["refresh_times"]:
        avg_refresh = sum(
            r["refresh_time_seconds"] for r in results["refresh_times"]
        ) / len(results["refresh_times"])
        prefect_logger.info(f"  Average refresh time: {avg_refresh:.2f}s")

    return results


if __name__ == "__main__":
    result = mongodb_loader_flow()
    print("\nMongoDB loader completed:")
    print(f"  Loaded: {len(result['loaded'])} collections")
    for loaded in result["loaded"]:
        print(f"    - {loaded['collection']}: {loaded['rows']} rows")
    print(f"  Skipped: {len(result['skipped'])} collections")
    print(f"  Errors: {len(result['errors'])}")

    if result["refresh_times"]:
        print("\nRefresh times:")
        for rt in result["refresh_times"]:
            print(f"  {rt['collection_name']}: {rt['refresh_time_human']}")
