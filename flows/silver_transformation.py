from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="Read Bronze Data", retries=2)
def read_bronze_data(object_name: str) -> pd.DataFrame:
    """
    Read CSV data from the bronze bucket.

    Args:
        object_name: Name of the object in the bronze bucket.

    Returns:
        DataFrame with the raw data.
    """
    client = get_minio_client()
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_csv(BytesIO(data))
    print(f"Read {len(df)} rows from {BUCKET_BRONZE}/{object_name}")
    return df


@task(name="Clean Clients")
def clean_clients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean client data:
    - Remove duplicates on id_client
    - Remove rows with null values in critical columns
    - Validate email format
    - Standardize country names
    """
    initial_count = len(df)

    # Remove duplicates on id_client
    df = df.drop_duplicates(subset=["id_client"], keep="first")

    # Remove rows with null values in critical columns
    critical_columns = ["id_client", "nom", "email", "pays"]
    df = df.dropna(subset=critical_columns)

    # Validate email format (basic check)
    df = df[df["email"].str.contains("@", na=False)]

    # Standardize country names (trim whitespace, title case)
    df["pays"] = df["pays"].str.strip().str.title()

    # Normalize types
    df["id_client"] = df["id_client"].astype(int)

    final_count = len(df)
    print(f"Clients cleaned: {initial_count} -> {final_count} ({initial_count - final_count} removed)")

    return df


@task(name="Clean Achats")
def clean_achats(df: pd.DataFrame, valid_client_ids: set) -> pd.DataFrame:
    """
    Clean purchase data:
    - Remove duplicates on id_achat
    - Remove rows with null values
    - Filter out invalid amounts (negative or > 10000)
    - Filter out future dates
    - Validate foreign key (id_client must exist)
    """
    initial_count = len(df)

    # Remove duplicates on id_achat
    df = df.drop_duplicates(subset=["id_achat"], keep="first")

    # Remove rows with null values
    df = df.dropna()

    # Normalize types
    df["id_achat"] = df["id_achat"].astype(int)
    df["id_client"] = df["id_client"].astype(int)
    df["montant"] = df["montant"].astype(float)

    # Filter out invalid amounts
    df = df[(df["montant"] > 0) & (df["montant"] <= 10000)]

    # Convert and validate dates
    df["date_achat"] = pd.to_datetime(df["date_achat"], errors="coerce")
    df = df.dropna(subset=["date_achat"])

    # Filter out future dates
    today = pd.Timestamp(datetime.now().date())
    df = df[df["date_achat"] <= today]

    # Validate foreign key
    df = df[df["id_client"].isin(valid_client_ids)]

    # Standardize product names
    df["produit"] = df["produit"].str.strip().str.title()

    final_count = len(df)
    print(f"Achats cleaned: {initial_count} -> {final_count} ({initial_count - final_count} removed)")

    return df


@task(name="Standardize Dates")
def standardize_dates(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """
    Standardize date column to datetime format.
    """
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df = df.dropna(subset=[date_column])
    return df


@task(name="Save to Silver")
def save_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Save DataFrame to the silver bucket as Parquet.

    Args:
        df: DataFrame to save.
        object_name: Name of the object in the silver bucket.

    Returns:
        Object name in the silver bucket.
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

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

    print(f"Saved {len(df)} rows to {BUCKET_SILVER}/{parquet_name}")
    return parquet_name


@task(name="Generate Quality Report")
def generate_quality_report(
    clients_initial: int,
    clients_final: int,
    achats_initial: int,
    achats_final: int
) -> dict:
    """
    Generate a data quality report.
    """
    report = {
        "timestamp": datetime.now().isoformat(),
        "clients": {
            "initial_count": clients_initial,
            "final_count": clients_final,
            "removed": clients_initial - clients_final,
            "removal_rate": round((clients_initial - clients_final) / clients_initial * 100, 2) if clients_initial > 0 else 0
        },
        "achats": {
            "initial_count": achats_initial,
            "final_count": achats_final,
            "removed": achats_initial - achats_final,
            "removal_rate": round((achats_initial - achats_final) / achats_initial * 100, 2) if achats_initial > 0 else 0
        }
    }

    print("\n=== Data Quality Report ===")
    print(f"Clients: {clients_final}/{clients_initial} kept ({report['clients']['removal_rate']}% removed)")
    print(f"Achats: {achats_final}/{achats_initial} kept ({report['achats']['removal_rate']}% removed)")

    return report


@flow(name="Silver Transformation Flow", retries=1)
def silver_transformation_flow() -> dict:
    """
    Flow to transform bronze data into silver layer.
    - Clean and validate data
    - Standardize formats
    - Remove duplicates
    - Save as Parquet
    """
    # Read bronze data
    clients_bronze = read_bronze_data("clients.csv")
    achats_bronze = read_bronze_data("achats.csv")

    clients_initial = len(clients_bronze)
    achats_initial = len(achats_bronze)

    # Clean clients
    clients_clean = clean_clients(clients_bronze)
    clients_clean = standardize_dates(clients_clean, "date_inscription")

    # Get valid client IDs for FK validation
    valid_client_ids = set(clients_clean["id_client"].tolist())

    # Clean achats
    achats_clean = clean_achats(achats_bronze, valid_client_ids)

    # Save to silver
    silver_clients = save_to_silver(clients_clean, "clients.csv")
    silver_achats = save_to_silver(achats_clean, "achats.csv")

    # Generate quality report
    report = generate_quality_report(
        clients_initial, len(clients_clean),
        achats_initial, len(achats_clean)
    )

    return {
        "clients": silver_clients,
        "achats": silver_achats,
        "quality_report": report
    }


if __name__ == "__main__":
    result = silver_transformation_flow()
    print("\nSilver transformation completed:", result)
