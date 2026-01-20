from io import BytesIO
from datetime import datetime

import pandas as pd
from prefect import flow, task

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client


@task(name="Read Silver Data", retries=2)
def read_silver_data(object_name: str) -> pd.DataFrame:
    """
    Read Parquet data from the silver bucket.

    Args:
        object_name: Name of the object in the silver bucket.

    Returns:
        DataFrame with the clean data.
    """
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_parquet(BytesIO(data))
    print(f"Read {len(df)} rows from {BUCKET_SILVER}/{object_name}")
    return df


@task(name="Create Dim Clients")
def create_dim_clients(clients: pd.DataFrame, achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create client dimension with enriched data:
    - Total CA per client
    - Number of purchases
    - Client segment (Bronze/Silver/Gold/Platinum)
    - First and last purchase dates
    """
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

    # Round total_ca
    dim_clients["total_ca"] = dim_clients["total_ca"].round(2)

    print(f"Created dim_clients with {len(dim_clients)} rows")
    print(f"Segments distribution:\n{dim_clients['segment'].value_counts()}")

    return dim_clients


@task(name="Create Dim Produits")
def create_dim_produits(achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create product dimension with aggregated stats.
    """
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
    dim_produits = dim_produits[["id_produit", "produit", "nb_ventes", "ca_total", "prix_moyen", "prix_min", "prix_max"]]

    print(f"Created dim_produits with {len(dim_produits)} products")

    return dim_produits


@task(name="Create Dim Temps")
def create_dim_temps(achats: pd.DataFrame) -> pd.DataFrame:
    """
    Create time dimension from purchase dates.
    """
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
    dim_temps["jour_semaine"] = dim_temps["date"].dt.dayofweek  # 0=Monday
    dim_temps["nom_jour"] = dim_temps["date"].dt.day_name()
    dim_temps["nom_mois"] = dim_temps["date"].dt.month_name()
    dim_temps["est_weekend"] = dim_temps["jour_semaine"].isin([5, 6])

    # Reorder columns
    dim_temps = dim_temps[["id_date", "date", "jour", "mois", "annee", "trimestre", "semaine", "jour_semaine", "nom_jour", "nom_mois", "est_weekend"]]

    print(f"Created dim_temps with {len(dim_temps)} dates")

    return dim_temps


@task(name="Create Fact Ventes")
def create_fact_ventes(achats: pd.DataFrame, dim_clients: pd.DataFrame, dim_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table for sales with foreign keys to dimensions.
    """
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

    print(f"Created fact_ventes with {len(fact_ventes)} rows")

    return fact_ventes


@task(name="Calculate CA par Jour")
def calculate_ca_par_jour(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily revenue.
    """
    ca_jour = fact_ventes.groupby("date").agg(
        ca_total=("montant", "sum"),
        nb_transactions=("id_achat", "count"),
        panier_moyen=("montant", "mean")
    ).reset_index()

    ca_jour["ca_total"] = ca_jour["ca_total"].round(2)
    ca_jour["panier_moyen"] = ca_jour["panier_moyen"].round(2)

    print(f"Calculated CA par jour: {len(ca_jour)} days")

    return ca_jour


@task(name="Calculate CA par Mois")
def calculate_ca_par_mois(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly revenue with growth rate.
    """
    ca_mois = fact_ventes.groupby("mois").agg(
        ca_total=("montant", "sum"),
        nb_transactions=("id_achat", "count"),
        nb_clients_uniques=("id_client", "nunique"),
        panier_moyen=("montant", "mean")
    ).reset_index()

    ca_mois = ca_mois.sort_values("mois")

    # Calculate month-over-month growth
    ca_mois["ca_precedent"] = ca_mois["ca_total"].shift(1)
    ca_mois["taux_croissance"] = ((ca_mois["ca_total"] - ca_mois["ca_precedent"]) / ca_mois["ca_precedent"] * 100).round(2)

    ca_mois["ca_total"] = ca_mois["ca_total"].round(2)
    ca_mois["panier_moyen"] = ca_mois["panier_moyen"].round(2)

    # Drop helper column
    ca_mois = ca_mois.drop(columns=["ca_precedent"])

    print(f"Calculated CA par mois: {len(ca_mois)} months")

    return ca_mois


@task(name="Calculate CA par Pays")
def calculate_ca_par_pays(fact_ventes: pd.DataFrame, dim_clients: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate revenue by country.
    """
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

    print(f"Calculated CA par pays:\n{ca_pays[['pays', 'ca_total', 'part_ca']].to_string()}")

    return ca_pays


@task(name="Calculate Volume par Produit")
def calculate_volume_par_produit(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate sales volume by product.
    """
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

    print(f"Top 5 products by volume:\n{volume_produit.head()[['rang', 'produit', 'nb_ventes', 'ca_total']].to_string()}")

    return volume_produit


@task(name="Calculate Top Clients")
def calculate_top_clients(dim_clients: pd.DataFrame, top_n: int = 100) -> pd.DataFrame:
    """
    Get top clients by total CA.
    """
    top_clients = dim_clients.nlargest(top_n, "total_ca")[
        ["id_client", "nom", "pays", "segment", "total_ca", "nb_achats", "premiere_commande", "derniere_commande"]
    ].copy()

    top_clients["rang"] = range(1, len(top_clients) + 1)

    print(f"Top 10 clients:\n{top_clients.head(10)[['rang', 'nom', 'segment', 'total_ca']].to_string()}")

    return top_clients


@task(name="Calculate Stats Distribution")
def calculate_stats_distribution(fact_ventes: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate statistical distribution of sales amounts by product.
    """
    stats = fact_ventes.groupby("produit")["montant"].describe().reset_index()
    stats.columns = ["produit", "count", "mean", "std", "min", "25%", "50%", "75%", "max"]

    # Round values
    for col in ["mean", "std", "min", "25%", "50%", "75%", "max"]:
        stats[col] = stats[col].round(2)

    print(f"Stats distribution calculated for {len(stats)} products")

    return stats


@task(name="Save to Gold")
def save_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Save DataFrame to the gold bucket as Parquet.
    """
    client = get_minio_client()

    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)

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

    print(f"Saved {len(df)} rows to {BUCKET_GOLD}/{object_name}")
    return object_name


@flow(name="Gold Aggregation Flow", retries=1)
def gold_aggregation_flow() -> dict:
    """
    Flow to create gold layer with dimensions, facts, and KPIs.
    """
    # Read silver data
    clients_silver = read_silver_data("clients.parquet")
    achats_silver = read_silver_data("achats.parquet")

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
    save_to_gold(dim_clients, "dim_clients.parquet")
    save_to_gold(dim_produits, "dim_produits.parquet")
    save_to_gold(dim_temps, "dim_temps.parquet")

    # Save fact table
    save_to_gold(fact_ventes, "fact_ventes.parquet")

    # Save KPIs
    save_to_gold(ca_jour, "kpi_ca_par_jour.parquet")
    save_to_gold(ca_mois, "kpi_ca_par_mois.parquet")
    save_to_gold(ca_pays, "kpi_ca_par_pays.parquet")
    save_to_gold(volume_produit, "kpi_volume_par_produit.parquet")
    save_to_gold(top_clients, "kpi_top_clients.parquet")
    save_to_gold(stats_distribution, "kpi_stats_distribution.parquet")

    return {
        "dimensions": ["dim_clients", "dim_produits", "dim_temps"],
        "facts": ["fact_ventes"],
        "kpis": [
            "kpi_ca_par_jour",
            "kpi_ca_par_mois",
            "kpi_ca_par_pays",
            "kpi_volume_par_produit",
            "kpi_top_clients",
            "kpi_stats_distribution"
        ]
    }


if __name__ == "__main__":
    result = gold_aggregation_flow()
    print("\nGold aggregation completed:")
    print(f"  Dimensions: {result['dimensions']}")
    print(f"  Facts: {result['facts']}")
    print(f"  KPIs: {result['kpis']}")
