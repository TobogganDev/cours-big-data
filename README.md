# Big Data Analytics Pipeline

Pipeline de données analytiques utilisant une architecture medallion (Bronze/Silver/Gold) avec Prefect pour l'orchestration, MinIO pour le stockage objet, MongoDB pour la couche opérationnelle, et une API FastAPI avec dashboard Streamlit.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │────▶│   Bronze    │────▶│   Silver    │────▶│    Gold     │
│   (CSV)     │     │  (Parquet)  │     │  (Parquet)  │     │  (Parquet)  │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                                   │
                                                                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Dashboard  │◀────│   FastAPI   │◀────│   MongoDB   │◀────│   Loader    │
│  Streamlit  │     │    /api     │     │  analytics  │     │    Flow     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

### Couches de données

| Couche | Format | Description |
|--------|--------|-------------|
| **Sources** | CSV | Données brutes générées (clients, achats) |
| **Bronze** | Parquet | Données ingérées avec validation de schéma |
| **Silver** | Parquet | Données nettoyées et transformées |
| **Gold** | Parquet | Dimensions, faits et KPIs agrégés |
| **MongoDB** | Documents | Base opérationnelle pour l'API |

## Services

| Service | Port | Description |
|---------|------|-------------|
| MinIO | 9000/9001 | Stockage objet S3-compatible |
| Prefect | 4200 | Orchestration des workflows |
| MongoDB | 27017 | Base de données opérationnelle |
| Mongo Express | 8081 | Interface web MongoDB |
| FastAPI | 8000 | API REST |
| Streamlit | 8501 | Dashboard analytique |

---

## Améliorations du Pipeline (Phase 1)

### Configuration centralisée (`flows/config.py`)

- Variables d'environnement pour tous les services (MinIO, MongoDB, Prefect)
- Fonctions utilitaires : `get_minio_client()`, `get_mongo_database()`
- Gestion des métadonnées de traitement
- Calcul de hash pour l'idempotence
- Système de quarantaine pour les fichiers invalides

### Bronze Layer (`flows/bronze_ingestion.py`)

**Fonctionnalités ajoutées :**
- **Découverte dynamique** : Détection automatique des fichiers sources via patterns glob
- **Inférence de schéma** : Détection automatique des types de colonnes
- **Validation de schéma** : Vérification des colonnes requises et types attendus
- **Idempotence** : Skip des fichiers déjà traités (basé sur hash MD5)
- **Quarantaine** : Isolation des fichiers invalides avec raison documentée
- **Métadonnées** : Tracking du traitement (hash, timestamp, lignes)

### Silver Layer (`flows/silver_transformation.py`)

**Fonctionnalités ajoutées :**
- **Freshness check** : Vérification si les données Bronze ont changé
- **Validation des données** : Règles métier (montant min/max, etc.)
- **Nettoyage** : Suppression des doublons, gestion des valeurs nulles
- **Transformation des types** : Conversion des dates, normalisation
- **Traitement incrémental** : Skip si les données sont à jour

### Gold Layer (`flows/gold_aggregation.py`)

**Fonctionnalités ajoutées :**
- **Freshness check** : Comparaison des hash Silver pour éviter le retraitement
- **Tables de dimensions** :
  - `dim_clients` : Enrichissement avec métriques (nb achats, CA total, segment)
  - `dim_produits` : Stats par produit (CA, nb ventes, prix moyen)
  - `dim_temps` : Dimension temporelle
- **Table de faits** : `fact_ventes` avec clés étrangères
- **KPIs pré-calculés** :
  - CA par jour/mois/pays
  - Volume par produit
  - Top clients
  - Stats de distribution

---

## Infrastructure MongoDB (Phase 2)

### Docker Compose

Ajout des services :
- **MongoDB 7** : Base documentaire avec healthcheck
- **Mongo Express** : Interface d'administration web

### MongoDB Loader (`flows/mongodb_loader.py`)

**Fonctionnalités :**
- **Mapping Gold → MongoDB** : Correspondance fichiers Parquet ↔ collections
- **Upsert intelligent** : Mise à jour basée sur clés primaires
- **Freshness check** : Skip si les données MongoDB sont à jour
- **Métadonnées de sync** : Collection `_sync_metadata` pour tracking
- **Calcul refresh time** : Temps entre traitement Gold et sync MongoDB

**Collections créées :**
```
dim_clients, dim_produits, dim_temps
fact_ventes
kpi_ca_par_jour, kpi_ca_par_mois, kpi_ca_par_pays
kpi_volume_par_produit, kpi_top_clients, kpi_stats_distribution
```

---

## API FastAPI (Phase 3)

### Endpoints disponibles

**Health**
- `GET /health` - Statut API et MongoDB

**Dimensions**
- `GET /api/v1/clients` - Liste clients (pagination, filtre par pays)
- `GET /api/v1/clients/{id}` - Client par ID
- `GET /api/v1/produits` - Liste produits
- `GET /api/v1/produits/{id}` - Produit par ID

**Facts**
- `GET /api/v1/ventes` - Ventes avec filtres (client, produit, dates)
- `GET /api/v1/ventes/{id}` - Vente par ID

**KPIs**
- `GET /api/v1/kpis/ca-par-jour` - CA journalier
- `GET /api/v1/kpis/ca-par-mois` - CA mensuel
- `GET /api/v1/kpis/ca-par-pays` - CA par pays
- `GET /api/v1/kpis/volume-par-produit` - Volumes vendus
- `GET /api/v1/kpis/top-clients` - Meilleurs clients
- `GET /api/v1/kpis/stats-distribution` - Stats de distribution

**Pipeline**
- `GET /api/v1/pipeline/status` - Statut global
- `GET /api/v1/pipeline/refresh-times` - Temps de rafraîchissement

**Aggregations dynamiques**
- `GET /api/v1/aggregations/ca-total` - CA total
- `GET /api/v1/aggregations/ca-par-periode?periode=jour|mois|annee`

---

## Dashboard Streamlit (Phase 4)

### Pages

- **Vue d'ensemble** : KPIs principaux, graphiques CA par mois/pays, top clients
- **Chiffre d'affaires** : Analyse temporelle avec granularité ajustable
- **Clients** : Top clients, statistiques, liste filtrable
- **Produits** : Treemap volumes, distribution des prix
- **Pipeline** : Statut des collections, temps de rafraîchissement

---

## Lancement du projet

### Prérequis

- Docker et Docker Compose
- Python 3.11+

### 1. Cloner et installer les dépendances

```bash
git clone https://github.com/TobogganDev/cours-big-data.git
cd big-data
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

### 2. Démarrer l'infrastructure

```bash
docker-compose up -d
```

Attendre que tous les services soient healthy (~30s).

### 3. Générer les données de test

```bash
python script/generate_data.py
```

### 4. Exécuter le pipeline complet

```bash
cd flows

# Bronze : Ingestion des sources
python bronze_ingestion.py

# Silver : Transformation et nettoyage
python silver_transformation.py

# Gold : Agrégation et KPIs
python gold_aggregation.py

# MongoDB : Chargement dans la base opérationnelle
python mongodb_loader.py
```

### 5. Lancer l'API

```bash
cd api
python main.py
```

API disponible sur http://localhost:8000

Documentation Swagger : http://localhost:8000/docs

### 6. Lancer le Dashboard

```bash
streamlit run dashboard/app.py
```

Dashboard disponible sur http://localhost:8501

### Interfaces disponibles

| Interface | URL |
|-----------|-----|
| Dashboard Streamlit | http://localhost:8501 |
| API FastAPI | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| Prefect UI | http://localhost:4200 |
| MinIO Console | http://localhost:9001 |
| Mongo Express | http://localhost:8081 |

### Arrêter les services

```bash
docker-compose down
```

Pour supprimer les volumes (reset complet) :
```bash
docker-compose down -v
```

---

## Structure du projet

```
big-data/
├── api/
│   └── main.py              # API FastAPI
├── dashboard/
│   └── app.py               # Dashboard Streamlit
├── data/
│   └── sources/             # Fichiers CSV générés
├── flows/
│   ├── config.py            # Configuration centralisée
│   ├── bronze_ingestion.py  # Flow Bronze
│   ├── silver_transformation.py  # Flow Silver
│   ├── gold_aggregation.py  # Flow Gold
│   └── mongodb_loader.py    # Flow MongoDB
├── script/
│   └── generate_data.py     # Générateur de données
├── docker-compose.yml       # Infrastructure
├── requirements.txt         # Dépendances Python
└── README.md
```
