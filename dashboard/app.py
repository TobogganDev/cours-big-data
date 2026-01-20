"""
Streamlit Dashboard for Big Data Analytics.
Connects to FastAPI to display KPIs and visualizations.
"""
import requests
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Configuration
API_BASE_URL = "http://localhost:8000"

st.set_page_config(
    page_title="Big Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============== API Client ==============

@st.cache_data(ttl=60)
def fetch_api(endpoint: str) -> dict:
    """Fetch data from API with caching."""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur API: {e}")
        return None


def check_api_health() -> bool:
    """Check if API is available."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False


# ============== Sidebar ==============

with st.sidebar:
    st.title("📊 Analytics Dashboard")
    st.markdown("---")

    # API Status
    api_healthy = check_api_health()
    if api_healthy:
        st.success("✅ API connectée")
    else:
        st.error("❌ API non disponible")
        st.info("Lancez l'API avec:\n```\ncd api && python main.py\n```")

    st.markdown("---")

    # Navigation
    page = st.radio(
        "Navigation",
        ["🏠 Vue d'ensemble", "📈 Chiffre d'affaires", "👥 Clients", "📦 Produits", "⚙️ Pipeline"]
    )

    st.markdown("---")

    # Refresh button
    if st.button("🔄 Rafraîchir les données"):
        st.cache_data.clear()
        st.rerun()


# ============== Helper Functions ==============

def format_currency(value: float) -> str:
    """Format value as currency."""
    return f"{value:,.2f} €"


def create_metric_card(col, title: str, value: str, delta: str = None):
    """Create a metric card."""
    col.metric(title, value, delta)


# ============== Pages ==============

if not api_healthy:
    st.warning("Veuillez démarrer l'API pour accéder au dashboard.")
    st.stop()


# ---------- Vue d'ensemble ----------
if page == "🏠 Vue d'ensemble":
    st.title("🏠 Vue d'ensemble")

    # KPIs row
    col1, col2, col3, col4 = st.columns(4)

    # CA Total
    ca_data = fetch_api("/api/v1/aggregations/ca-total")
    if ca_data:
        col1.metric("💰 CA Total", format_currency(ca_data["ca_total"]))
        col2.metric("🛒 Nombre de ventes", f"{ca_data['nb_ventes']:,}")

    # Nombre de clients
    clients_data = fetch_api("/api/v1/clients?limit=1")
    if clients_data:
        col3.metric("👥 Clients", f"{clients_data['total']:,}")

    # Nombre de produits
    produits_data = fetch_api("/api/v1/produits?limit=1")
    if produits_data:
        col4.metric("📦 Produits", f"{produits_data['total']:,}")

    st.markdown("---")

    # Charts row
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 CA par Mois")
        ca_mois = fetch_api("/api/v1/kpis/ca-par-mois?limit=12")
        if ca_mois and ca_mois["data"]:
            df = pd.DataFrame(ca_mois["data"])
            df = df.sort_values("mois")
            fig = px.bar(
                df, x="mois", y="ca_total",
                labels={"mois": "Mois", "ca_total": "CA (€)"},
                color_discrete_sequence=["#1f77b4"]
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données disponibles")

    with col2:
        st.subheader("🌍 CA par Pays")
        ca_pays = fetch_api("/api/v1/kpis/ca-par-pays")
        if ca_pays and ca_pays["data"]:
            df = pd.DataFrame(ca_pays["data"])
            fig = px.pie(
                df, values="ca_total", names="pays",
                hole=0.4
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données disponibles")

    # Second row
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🏆 Top 10 Clients")
        top_clients = fetch_api("/api/v1/kpis/top-clients?limit=10")
        if top_clients and top_clients["data"]:
            df = pd.DataFrame(top_clients["data"])
            fig = px.bar(
                df, x="total_ca", y="nom", orientation="h",
                labels={"total_ca": "CA (€)", "nom": "Client"},
                color_discrete_sequence=["#2ca02c"]
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données disponibles")

    with col2:
        st.subheader("📦 Volume par Produit")
        volume_produit = fetch_api("/api/v1/kpis/volume-par-produit")
        if volume_produit and volume_produit["data"]:
            df = pd.DataFrame(volume_produit["data"])
            fig = px.bar(
                df.head(10), x="nb_ventes", y="produit", orientation="h",
                labels={"nb_ventes": "Nb Ventes", "produit": "Produit"},
                color_discrete_sequence=["#ff7f0e"]
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas de données disponibles")


# ---------- Chiffre d'affaires ----------
elif page == "📈 Chiffre d'affaires":
    st.title("📈 Analyse du Chiffre d'Affaires")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        granularite = st.selectbox("Granularité", ["jour", "mois", "annee"])

    # CA par période
    st.subheader(f"CA par {granularite}")
    ca_periode = fetch_api(f"/api/v1/aggregations/ca-par-periode?periode={granularite}")
    if ca_periode and ca_periode["data"]:
        df = pd.DataFrame(ca_periode["data"])
        df = df.sort_values("periode")

        fig = px.line(
            df, x="periode", y="ca",
            labels={"periode": "Période", "ca": "CA (€)"},
            markers=True
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # Stats
        col1, col2, col3 = st.columns(3)
        col1.metric("CA Moyen", format_currency(df["ca"].mean()))
        col2.metric("CA Max", format_currency(df["ca"].max()))
        col3.metric("CA Min", format_currency(df["ca"].min()))

        # Table
        with st.expander("📋 Voir les données"):
            st.dataframe(df, use_container_width=True)
    else:
        st.info("Pas de données disponibles")

    st.markdown("---")

    # CA par jour détaillé
    st.subheader("📅 CA Journalier (détail)")
    ca_jour = fetch_api("/api/v1/kpis/ca-par-jour?limit=30")
    if ca_jour and ca_jour["data"]:
        df = pd.DataFrame(ca_jour["data"])

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["date"],
            y=df["ca_total"],
            mode='lines+markers',
            name='CA',
            fill='tozeroy'
        ))
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="CA (€)",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas de données disponibles")


# ---------- Clients ----------
elif page == "👥 Clients":
    st.title("👥 Analyse des Clients")

    # Top clients
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🏆 Meilleurs Clients")
        n_clients = st.slider("Nombre de clients", 5, 50, 10)
        top_clients = fetch_api(f"/api/v1/kpis/top-clients?limit={n_clients}")
        if top_clients and top_clients["data"]:
            df = pd.DataFrame(top_clients["data"])

            fig = px.bar(
                df, x="nom", y="total_ca",
                labels={"nom": "Client", "total_ca": "CA (€)"},
                color="total_ca",
                color_continuous_scale="Blues"
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("📊 Stats")
        if top_clients and top_clients["data"]:
            df = pd.DataFrame(top_clients["data"])
            st.metric("CA Total Top Clients", format_currency(df["total_ca"].sum()))
            st.metric("CA Moyen", format_currency(df["total_ca"].mean()))
            st.metric("Nb Achats Total", f"{df['nb_achats'].sum():,}")

    st.markdown("---")

    # Liste des clients
    st.subheader("📋 Liste des Clients")

    col1, col2 = st.columns(2)
    with col1:
        # Get unique countries
        ca_pays = fetch_api("/api/v1/kpis/ca-par-pays")
        pays_list = ["Tous"] + [p["pays"] for p in ca_pays["data"]] if ca_pays and ca_pays["data"] else ["Tous"]
        pays_filter = st.selectbox("Filtrer par pays", pays_list)

    with col2:
        limit = st.number_input("Nombre de résultats", 10, 500, 50)

    endpoint = f"/api/v1/clients?limit={limit}"
    if pays_filter != "Tous":
        endpoint += f"&pays={pays_filter}"

    clients = fetch_api(endpoint)
    if clients and clients["data"]:
        df = pd.DataFrame(clients["data"])
        st.dataframe(df, use_container_width=True)
        st.caption(f"Affichage de {len(df)} sur {clients['total']} clients")
    else:
        st.info("Pas de clients disponibles")


# ---------- Produits ----------
elif page == "📦 Produits":
    st.title("📦 Analyse des Produits")

    # Volume par produit
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Volume par Produit")
        volume = fetch_api("/api/v1/kpis/volume-par-produit")
        if volume and volume["data"]:
            df = pd.DataFrame(volume["data"])

            fig = px.treemap(
                df, path=["produit"], values="nb_ventes",
                color="ca_total",
                color_continuous_scale="RdYlGn"
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("💰 CA par Produit")
        if volume and volume["data"]:
            df = pd.DataFrame(volume["data"])

            fig = px.pie(
                df, values="ca_total", names="produit",
                hole=0.3
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Stats distribution
    st.subheader("📈 Statistiques de Distribution")
    stats = fetch_api("/api/v1/kpis/stats-distribution")
    if stats and stats["data"]:
        df = pd.DataFrame(stats["data"])

        # Display as table with formatting
        st.dataframe(
            df.style.format({
                "mean": "{:.2f} €",
                "std": "{:.2f} €",
                "min": "{:.2f} €",
                "max": "{:.2f} €",
                "25%": "{:.2f} €",
                "50%": "{:.2f} €",
                "75%": "{:.2f} €"
            }),
            use_container_width=True
        )

        # Box plot comparison
        st.subheader("📦 Comparaison des distributions")
        fig = go.Figure()
        for _, row in df.iterrows():
            fig.add_trace(go.Box(
                name=row["produit"],
                q1=[row["25%"]],
                median=[row["50%"]],
                q3=[row["75%"]],
                lowerfence=[row["min"]],
                upperfence=[row["max"]]
            ))
        fig.update_layout(showlegend=False, yaxis_title="Montant (€)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Pas de statistiques disponibles")


# ---------- Pipeline ----------
elif page == "⚙️ Pipeline":
    st.title("⚙️ Statut du Pipeline")

    # Pipeline status
    status = fetch_api("/api/v1/pipeline/status")
    if status:
        col1, col2, col3 = st.columns(3)
        col1.metric("Collections totales", status["total_collections"])
        col2.metric("Collections synchronisées", status["synced_collections"])

        sync_pct = (status["synced_collections"] / status["total_collections"] * 100) if status["total_collections"] > 0 else 0
        col3.metric("Taux de sync", f"{sync_pct:.0f}%")

        st.markdown("---")

        # Collections status
        st.subheader("📋 Statut des Collections")

        collections_df = pd.DataFrame(status["collections"])

        # Add status icons
        collections_df["status_icon"] = collections_df["synced"].apply(lambda x: "✅" if x else "❌")
        collections_df["display_name"] = collections_df["status_icon"] + " " + collections_df["name"]

        # Display as cards
        cols = st.columns(3)
        for i, (_, row) in enumerate(collections_df.iterrows()):
            with cols[i % 3]:
                with st.container():
                    st.markdown(f"**{row['display_name']}**")
                    if row["synced"]:
                        st.caption(f"Dernière sync: {row['last_sync'][:19] if row['last_sync'] else 'N/A'}")
                        st.caption(f"Lignes: {row['row_count']:,}" if row['row_count'] else "")
                    else:
                        st.caption("Non synchronisé")
                    st.markdown("---")

    # Refresh times
    st.subheader("⏱️ Temps de Rafraîchissement")
    refresh_times = fetch_api("/api/v1/pipeline/refresh-times")
    if refresh_times:
        df = pd.DataFrame(refresh_times)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Pas de données de refresh time")

    # MongoDB connection info
    st.markdown("---")
    st.subheader("🔗 Connexions")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**API FastAPI**")
        st.code(f"{API_BASE_URL}/docs")
    with col2:
        st.markdown("**Mongo Express**")
        st.code("http://localhost:8081")


# Footer
st.markdown("---")
st.caption(f"Dashboard mis à jour le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
