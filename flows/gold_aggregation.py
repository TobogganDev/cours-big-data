from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

from config import (
    BUCKET_SILVER,
    BUCKET_GOLD,
    get_minio_client,
    ensure_bucket_exists,
    calculate_data_hash,
    get_processing_metadata,
    save_processing_metadata,
)


@task(name="Check Gold Freshness", retries=1)
def check_gold_freshness(force: bool = False) -> dict:
    """
    Check if gold data needs to be refreshed based on silver data changes.

    Args:
        force: Force reprocessing.

    Returns:
        Freshness check result with should_process flag and source hashes.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    result = {
        "should_process": True,
        "reason": "new_data",
        "silver_hashes": {}
    }

    if force:
        result["reason"] = "force_refresh"
        prefect_logger.info("Gold refresh: Force refresh enabled")
        return result

    # Get silver metadata for both files
    clients_silver_meta = get_processing_metadata(client, BUCKET_SILVER, "clients.parquet")
    achats_silver_meta = get_processing_metadata(client, BUCKET_SILVER, "achats.parquet")

    if not clients_silver_meta or not achats_silver_meta:
        result["reason"] = "missing_silver_metadata"
        prefect_logger.info("Gold refresh: Missing silver metadata, will process")
        return result

    # Get gold metadata (using dim_clients as reference)
    gold_meta = get_processing_metadata(client, BUCKET_GOLD, "dim_clients.parquet")

    if not gold_meta:
        result["reason"] = "no_gold_data"
        prefect_logger.info("Gold refresh: No gold data exists, will process")
        return result

    # Compare silver hashes
    current_silver_hashes = {
        "clients": clients_silver_meta.get("source_hash"),
        "achats": achats_silver_meta.get("source_hash")
    }
    result["silver_hashes"] = current_silver_hashes

    gold_silver_hashes = gold_meta.get("silver_hashes", {})

    if current_silver_hashes != gold_silver_hashes:
        result["reason"] = "silver_data_changed"
        prefect_logger.info("Gold refresh: Silver data changed, will reprocess")
        return result

    # Data is fresh
    result["should_process"] = False
    result["reason"] = "data_is_fresh"
    prefect_logger.info("Gold refresh: Gold data is up to date, skipping")

    return result


@task(name="Read Silver Data", retries=2)
def read_silver_data(object_name: str) -> tuple[pd.DataFrame, str]:
    """
    Read Parquet data from the silver bucket.

    Args:
        object_name: Name of the object in the silver bucket.

    Returns:
        Tuple of (DataFrame, data_hash).
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    data_hash = calculate_data_hash(data)
    df = pd.read_parquet(BytesIO(data))

    prefect_logger.info(f"Read {len(df)} rows from {BUCKET_SILVER}/{object_name}")
    return df, data_hash


@task(name="Create Dim Clients")
def create_dim_clients(clients: pd.DataFrame, achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create client dimension with enriched data.
    """
    prefect_logger = get_run_logger()

    # Aggregate purchase data per client
    client_stats = achats.groupby("id_client").agg(
        total_ca=("montant", "sum"),
        nb_achats=("id_achat", "count"),
        premiere_commande=("date_achat", "min"),
        derniere_commande=("date_achat", "max")
    ).reset_index()

    # Merge with client data
    dim_clients = clients.merge(client_stats, on="id_client", how="left")

    # Fill NaN for clients without purchases
    dim_clients["total_ca"] = dim_clients["total_ca"].fillna(0)
    dim_clients["nb_achats"] = dim_clients["nb_achats"].fillna(0).astype(int)

    # Create client segments based on total CA
    def get_segment(ca):
        if ca >= 5000:
            return "Platinum"
        elif ca >= 2000:
            return "Gold"
        elif ca >= 500:
            return "Silver"
        else:
            return "Bronze"

    dim_clients["segment"] = dim_clients["total_ca"].apply(get_segment)
    dim_clients["total_ca"] = dim_clients["total_ca"].round(2)

    prefect_logger.info(f"Created dim_clients with {len(dim_clients)} rows")
    prefect_logger.info(f"Segments distribution:\n{dim_clients['segment'].value_counts().to_dict()}")

    return dim_clients


@task(name="Create Dim Produits")
def create_dim_produits(achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create product dimension with aggregated stats.
    """
    prefect_logger = get_run_logger()

    dim_produits = achats.groupby("produit").agg(
        nb_ventes=("id_achat", "count"),
        ca_total=("montant", "sum"),
        prix_moyen=("montant", "mean"),
        prix_min=("montant", "min"),
        prix_max=("montant", "max")
    ).reset_index()

    # Add product ID
    dim_produits["id_produit"] = range(1, len(dim_produits) + 1)

    # Round values
    dim_produits["ca_total"] = dim_produits["ca_total"].round(2)
    dim_produits["prix_moyen"] = dim_produits["prix_moyen"].round(2)

    # Reorder columns
    dim_produits = dim_produits[[
        "id_produit", "produit", "nb_ventes", "ca_total",
        "prix_moyen", "prix_min", "prix_max"
    ]]

    prefect_logger.info(f"Created dim_produits with {len(dim_produits)} products")

    return dim_produits


@task(name="Create Dim Temps")
def create_dim_temps(achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create time dimension from purchase dates.
    """
    prefect_logger = get_run_logger()

    # Get unique dates
    dates = achats["date_achat"].dt.date.unique()
    dates = pd.to_datetime(dates)

    dim_temps = pd.DataFrame({"date": dates})
    dim_temps["id_date"] = range(1, len(dim_temps) + 1)
    dim_temps["jour"] = dim_temps["date"].dt.day
    dim_temps["mois"] = dim_temps["date"].dt.month
    dim_temps["annee"] = dim_temps["date"].dt.year
    dim_temps["trimestre"] = dim_temps["date"].dt.quarter
    dim_temps["semaine"] = dim_temps["date"].dt.isocalendar().week.astype(int)
    dim_temps["jour_semaine"] = dim_temps["date"].dt.dayofweek
    dim_temps["nom_jour"] = dim_temps["date"].dt.day_name()
    dim_temps["nom_mois"] = dim_temps["date"].dt.month_name()
    dim_temps["est_weekend"] = dim_temps["jour_semaine"].isin([5, 6])

    # Reorder columns
    dim_temps = dim_temps[[
        "id_date", "date", "jour", "mois", "annee", "trimestre",
        "semaine", "jour_semaine", "nom_jour", "nom_mois", "est_weekend"
    ]]

    prefect_logger.info(f"Created dim_temps with {len(dim_temps)} dates")

    return dim_temps


@task(name="Create Fact Ventes")
def create_fact_ventes(
    achats: pd.DataFrame,
    dim_clients: pd.DataFrame,
    dim_produits: pd.DataFrame
) -> pd.DataFrame:
    """
    Create fact table for sales with foreign keys to dimensions.
    """
    prefect_logger = get_run_logger()

    # Create product ID mapping
    produit_mapping = dim_produits.set_index("produit")["id_produit"].to_dict()

    fact_ventes = achats.copy()
    fact_ventes["id_produit"] = fact_ventes["produit"].map(produit_mapping)

    # Add client segment for denormalization
    segment_mapping = dim_clients.set_index("id_client")["segment"].to_dict()
    fact_ventes["segment_client"] = fact_ventes["id_client"].map(segment_mapping)

    # Add time components
    fact_ventes["date"] = fact_ventes["date_achat"].dt.date
    fact_ventes["mois"] = fact_ventes["date_achat"].dt.to_period("M").astype(str)
    fact_ventes["annee"] = fact_ventes["date_achat"].dt.year

    # Select and reorder columns
    fact_ventes = fact_ventes[[
        "id_achat", "id_client", "id_produit", "date_achat", "date", "mois", "annee",
        "produit", "montant", "segment_client"
    ]]

    prefect_logger.info(f"Created fact_ventes with {len(fact_ventes)} rows")

    return fact_ventes


@task(name="Calculate CA par Jour")
def calculate_ca_par_jour(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily revenue.
    """
    prefect_logger = get_run_logger()

    ca_jour = fact_ventes.groupby("date").agg(
        ca_total=("montant", "sum"),
        nb_transactions=("id_achat", "count"),
        panier_moyen=("montant", "mean")
    ).reset_index()

    ca_jour["ca_total"] = ca_jour["ca_total"].round(2)
    ca_jour["panier_moyen"] = ca_jour["panier_moyen"].round(2)

    prefect_logger.info(f"Calculated CA par jour: {len(ca_jour)} days")

    return ca_jour


@task(name="Calculate CA par Mois")
def calculate_ca_par_mois(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly revenue with growth rate.
    """
    prefect_logger = get_run_logger()

    ca_mois = fact_ventes.groupby("mois").agg(
        ca_total=("montant", "sum"),
        nb_transactions=("id_achat", "count"),
        nb_clients_uniques=("id_client", "nunique"),
        panier_moyen=("montant", "mean")
    ).reset_index()

    ca_mois = ca_mois.sort_values("mois")

    # Calculate month-over-month growth
    ca_mois["ca_precedent"] = ca_mois["ca_total"].shift(1)
    ca_mois["taux_croissance"] = (
        (ca_mois["ca_total"] - ca_mois["ca_precedent"]) /
        ca_mois["ca_precedent"] * 100
    ).round(2)

    ca_mois["ca_total"] = ca_mois["ca_total"].round(2)
    ca_mois["panier_moyen"] = ca_mois["panier_moyen"].round(2)

    # Drop helper column
    ca_mois = ca_mois.drop(columns=["ca_precedent"])

    prefect_logger.info(f"Calculated CA par mois: {len(ca_mois)} months")

    return ca_mois


@task(name="Calculate CA par Pays")
def calculate_ca_par_pays(
    fact_ventes: pd.DataFrame,
    dim_clients: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate revenue by country.
    """
    prefect_logger = get_run_logger()

    # Merge with client data to get country
    ventes_pays = fact_ventes.merge(
        dim_clients[["id_client", "pays"]],
        on="id_client",
        how="left"
    )

    ca_pays = ventes_pays.groupby("pays").agg(
        ca_total=("montant", "sum"),
        nb_transactions=("id_achat", "count"),
        nb_clients=("id_client", "nunique"),
        panier_moyen=("montant", "mean")
    ).reset_index()

    ca_pays["ca_total"] = ca_pays["ca_total"].round(2)
    ca_pays["panier_moyen"] = ca_pays["panier_moyen"].round(2)

    # Sort by CA
    ca_pays = ca_pays.sort_values("ca_total", ascending=False)

    # Add percentage of total
    total_ca = ca_pays["ca_total"].sum()
    ca_pays["part_ca"] = (ca_pays["ca_total"] / total_ca * 100).round(2)

    prefect_logger.info(f"Calculated CA par pays: {len(ca_pays)} countries")

    return ca_pays


@task(name="Calculate Volume par Produit")
def calculate_volume_par_produit(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate sales volume by product.
    """
    prefect_logger = get_run_logger()

    volume_produit = fact_ventes.groupby("produit").agg(
        nb_ventes=("id_achat", "count"),
        ca_total=("montant", "sum"),
        prix_moyen=("montant", "mean"),
        nb_clients=("id_client", "nunique")
    ).reset_index()

    volume_produit["ca_total"] = volume_produit["ca_total"].round(2)
    volume_produit["prix_moyen"] = volume_produit["prix_moyen"].round(2)

    # Sort by volume
    volume_produit = volume_produit.sort_values("nb_ventes", ascending=False)

    # Add ranking
    volume_produit["rang"] = range(1, len(volume_produit) + 1)

    prefect_logger.info(f"Calculated volume par produit: {len(volume_produit)} products")

    return volume_produit


@task(name="Calculate Top Clients")
def calculate_top_clients(dim_clients: pd.DataFrame, top_n: int = 100) -> pd.DataFrame:
    """
    Get top clients by total CA.
    """
    prefect_logger = get_run_logger()

    top_clients = dim_clients.nlargest(top_n, "total_ca")[[
        "id_client", "nom", "pays", "segment", "total_ca",
        "nb_achats", "premiere_commande", "derniere_commande"
    ]].copy()

    top_clients["rang"] = range(1, len(top_clients) + 1)

    prefect_logger.info(f"Calculated top {len(top_clients)} clients")

    return top_clients


@task(name="Calculate Stats Distribution")
def calculate_stats_distribution(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate statistical distribution of sales amounts by product.
    """
    prefect_logger = get_run_logger()

    stats = fact_ventes.groupby("produit")["montant"].describe().reset_index()
    stats.columns = ["produit", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]

    # Round values
    for col in ["mean", "std", "min", "25%", "50%", "75%", "max"]:
        stats[col] = stats[col].round(2)

    prefect_logger.info(f"Stats distribution calculated for {len(stats)} products")

    return stats


@task(name="Save to Gold")
def save_to_gold(
    df: pd.DataFrame,
    object_name: str,
    silver_hashes: dict,
    is_dimension: bool = False
) -> str:
    """
    Save DataFrame to the gold bucket as Parquet with metadata.

    Args:
        df: DataFrame to save.
        object_name: Name of the output file.
        silver_hashes: Hashes of source silver data for tracking.
        is_dimension: Whether this is a dimension table.

    Returns:
        Object name in the gold bucket.
    """
    prefect_logger = get_run_logger()
    client = get_minio_client()

    ensure_bucket_exists(client, BUCKET_GOLD)

    # Convert to Parquet
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to MinIO
    client.put_object(
        BUCKET_GOLD,
        object_name,
        buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    # Save processing metadata
    table_type = "dimension" if is_dimension else "fact" if "fact_" in object_name else "kpi"
    save_processing_metadata(
        client=client,
        object_name=object_name,
        source_hash=calculate_data_hash(buffer.getvalue()),
        row_count=len(df),
        status="aggregated_to_gold",
        extra={
            "silver_hashes": silver_hashes,
            "layer": "gold",
            "table_type": table_type,
            "format": "parquet"
        }
    )

    prefect_logger.info(f"Saved {len(df)} rows to {BUCKET_GOLD}/{object_name}")
    return object_name


@flow(name="Gold Aggregation Flow", retries=1)
def gold_aggregation_flow(force: bool = False) -> dict:
    """
    Robust flow to create gold layer with dimensions, facts, and KPIs.

    Features:
    - Incremental processing (skip if silver unchanged)
    - Upsert logic via full refresh with hash tracking
    - Processing metadata for lineage
    - Comprehensive KPI calculations

    Args:
        force: Force reprocessing of all data.

    Returns:
        Processing results dictionary.
    """
    prefect_logger = get_run_logger()
    prefect_logger.info(f"Starting Gold Aggregation Flow (force={force})")

    results = {
        "processed": [],
        "skipped": False,
        "errors": [],
        "tables_created": {
            "dimensions": [],
            "facts": [],
            "kpis": []
        }
    }

    # Check freshness
    freshness = check_gold_freshness(force=force)

    if not freshness["should_process"]:
        prefect_logger.info("Gold data is up to date, nothing to process")
        results["skipped"] = True
        return results

    try:
        # Read silver data
        clients_silver, clients_hash = read_silver_data("clients.parquet")
        achats_silver, achats_hash = read_silver_data("achats.parquet")

        silver_hashes = {
            "clients": clients_hash,
            "achats": achats_hash
        }

        # Create dimensions
        dim_clients = create_dim_clients(clients_silver, achats_silver)
        dim_produits = create_dim_produits(achats_silver)
        dim_temps = create_dim_temps(achats_silver)

        # Create fact table
        fact_ventes = create_fact_ventes(achats_silver, dim_clients, dim_produits)

        # Calculate KPIs
        ca_jour = calculate_ca_par_jour(fact_ventes)
        ca_mois = calculate_ca_par_mois(fact_ventes)
        ca_pays = calculate_ca_par_pays(fact_ventes, dim_clients)
        volume_produit = calculate_volume_par_produit(fact_ventes)
        top_clients = calculate_top_clients(dim_clients)
        stats_distribution = calculate_stats_distribution(fact_ventes)

        # Save dimensions
        save_to_gold(dim_clients, "dim_clients.parquet", silver_hashes, is_dimension=True)
        save_to_gold(dim_produits, "dim_produits.parquet", silver_hashes, is_dimension=True)
        save_to_gold(dim_temps, "dim_temps.parquet", silver_hashes, is_dimension=True)
        results["tables_created"]["dimensions"] = ["dim_clients", "dim_produits", "dim_temps"]

        # Save fact table
        save_to_gold(fact_ventes, "fact_ventes.parquet", silver_hashes)
        results["tables_created"]["facts"] = ["fact_ventes"]

        # Save KPIs
        save_to_gold(ca_jour, "kpi_ca_par_jour.parquet", silver_hashes)
        save_to_gold(ca_mois, "kpi_ca_par_mois.parquet", silver_hashes)
        save_to_gold(ca_pays, "kpi_ca_par_pays.parquet", silver_hashes)
        save_to_gold(volume_produit, "kpi_volume_par_produit.parquet", silver_hashes)
        save_to_gold(top_clients, "kpi_top_clients.parquet", silver_hashes)
        save_to_gold(stats_distribution, "kpi_stats_distribution.parquet", silver_hashes)
        results["tables_created"]["kpis"] = [
            "kpi_ca_par_jour", "kpi_ca_par_mois", "kpi_ca_par_pays",
            "kpi_volume_par_produit", "kpi_top_clients", "kpi_stats_distribution"
        ]

        results["processed"] = [
            {"layer": "dimensions", "count": 3},
            {"layer": "facts", "count": 1},
            {"layer": "kpis", "count": 6}
        ]

    except Exception as e:
        prefect_logger.error(f"Gold aggregation failed: {e}")
        results["errors"].append(str(e))
        raise

    prefect_logger.info("Gold Aggregation Complete")
    prefect_logger.info(f"  Dimensions: {results['tables_created']['dimensions']}")
    prefect_logger.info(f"  Facts: {results['tables_created']['facts']}")
    prefect_logger.info(f"  KPIs: {results['tables_created']['kpis']}")

    return results


if __name__ == "__main__":
    result = gold_aggregation_flow()
    print("\nGold aggregation completed:")
    if result["skipped"]:
        print("  Skipped: Gold data was already up to date")
    else:
        print(f"  Dimensions: {result['tables_created']['dimensions']}")
        print(f"  Facts: {result['tables_created']['facts']}")
        print(f"  KPIs: {result['tables_created']['kpis']}")
