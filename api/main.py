"""
FastAPI application for Big Data Analytics.
Exposes MongoDB data through REST endpoints.
"""
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import sys
sys.path.insert(0, "..")
from flows.config import get_mongo_database

app = FastAPI(
    title="Big Data Analytics API",
    description="API pour accéder aux données analytiques du pipeline Big Data",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============== Response Models ==============

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    mongodb: str


class RefreshTimeResponse(BaseModel):
    collection_name: str
    gold_processed_at: Optional[str]
    mongo_synced_at: Optional[str]
    refresh_time_seconds: Optional[float]
    refresh_time_human: Optional[str]


class PipelineStatusResponse(BaseModel):
    total_collections: int
    synced_collections: int
    collections: list[dict]
    average_refresh_time_seconds: Optional[float]


# ============== Health Endpoints ==============

@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health_check():
    """Check API and MongoDB health."""
    try:
        db = get_mongo_database()
        db.command("ping")
        mongo_status = "connected"
    except Exception as e:
        mongo_status = f"error: {str(e)}"

    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "mongodb": mongo_status
    }


# ============== Dimensions Endpoints ==============

@app.get("/api/v1/clients", tags=["Dimensions"])
def get_clients(
    skip: int = Query(0, ge=0, description="Nombre d'enregistrements à sauter"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre max d'enregistrements"),
    pays: Optional[str] = Query(None, description="Filtrer par pays")
):
    """Récupère la liste des clients."""
    db = get_mongo_database()
    collection = db["dim_clients"]

    query = {}
    if pays:
        query["pays"] = pays

    total = collection.count_documents(query)
    clients = list(collection.find(query, {"_id": 0}).skip(skip).limit(limit))

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": clients
    }


@app.get("/api/v1/clients/{id_client}", tags=["Dimensions"])
def get_client(id_client: int):
    """Récupère un client par son ID."""
    db = get_mongo_database()
    collection = db["dim_clients"]

    client = collection.find_one({"id_client": id_client}, {"_id": 0})
    if not client:
        raise HTTPException(status_code=404, detail="Client non trouvé")

    return client


@app.get("/api/v1/produits", tags=["Dimensions"])
def get_produits(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Récupère la liste des produits."""
    db = get_mongo_database()
    collection = db["dim_produits"]

    total = collection.count_documents({})
    produits = list(collection.find({}, {"_id": 0}).skip(skip).limit(limit))

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": produits
    }


@app.get("/api/v1/produits/{id_produit}", tags=["Dimensions"])
def get_produit(id_produit: int):
    """Récupère un produit par son ID."""
    db = get_mongo_database()
    collection = db["dim_produits"]

    produit = collection.find_one({"id_produit": id_produit}, {"_id": 0})
    if not produit:
        raise HTTPException(status_code=404, detail="Produit non trouvé")

    return produit


# ============== Facts Endpoints ==============

@app.get("/api/v1/ventes", tags=["Facts"])
def get_ventes(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    id_client: Optional[int] = Query(None, description="Filtrer par client"),
    produit: Optional[str] = Query(None, description="Filtrer par produit"),
    date_debut: Optional[str] = Query(None, description="Date début (YYYY-MM-DD)"),
    date_fin: Optional[str] = Query(None, description="Date fin (YYYY-MM-DD)")
):
    """Récupère les ventes avec filtres optionnels."""
    db = get_mongo_database()
    collection = db["fact_ventes"]

    query = {}
    if id_client:
        query["id_client"] = id_client
    if produit:
        query["produit"] = produit
    if date_debut or date_fin:
        query["date_achat"] = {}
        if date_debut:
            query["date_achat"]["$gte"] = datetime.fromisoformat(date_debut)
        if date_fin:
            query["date_achat"]["$lte"] = datetime.fromisoformat(date_fin)

    total = collection.count_documents(query)
    ventes = list(collection.find(query, {"_id": 0}).skip(skip).limit(limit))

    # Convert datetime to string for JSON serialization
    for vente in ventes:
        if "date_achat" in vente and isinstance(vente["date_achat"], datetime):
            vente["date_achat"] = vente["date_achat"].isoformat()

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": ventes
    }


@app.get("/api/v1/ventes/{id_achat}", tags=["Facts"])
def get_vente(id_achat: int):
    """Récupère une vente par son ID."""
    db = get_mongo_database()
    collection = db["fact_ventes"]

    vente = collection.find_one({"id_achat": id_achat}, {"_id": 0})
    if not vente:
        raise HTTPException(status_code=404, detail="Vente non trouvée")

    if "date_achat" in vente and isinstance(vente["date_achat"], datetime):
        vente["date_achat"] = vente["date_achat"].isoformat()

    return vente


# ============== KPI Endpoints ==============

@app.get("/api/v1/kpis/ca-par-jour", tags=["KPIs"])
def get_ca_par_jour(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    date_debut: Optional[str] = Query(None, description="Date début (YYYY-MM-DD)"),
    date_fin: Optional[str] = Query(None, description="Date fin (YYYY-MM-DD)")
):
    """Récupère le chiffre d'affaires par jour."""
    db = get_mongo_database()
    collection = db["kpi_ca_par_jour"]

    query = {}
    if date_debut or date_fin:
        query["date"] = {}
        if date_debut:
            query["date"]["$gte"] = datetime.fromisoformat(date_debut)
        if date_fin:
            query["date"]["$lte"] = datetime.fromisoformat(date_fin)

    total = collection.count_documents(query)
    data = list(collection.find(query, {"_id": 0}).sort("date", -1).skip(skip).limit(limit))

    for item in data:
        if "date" in item and isinstance(item["date"], datetime):
            item["date"] = item["date"].isoformat()

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": data
    }


@app.get("/api/v1/kpis/ca-par-mois", tags=["KPIs"])
def get_ca_par_mois(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000)
):
    """Récupère le chiffre d'affaires par mois."""
    db = get_mongo_database()
    collection = db["kpi_ca_par_mois"]

    total = collection.count_documents({})
    data = list(collection.find({}, {"_id": 0}).sort("mois", -1).skip(skip).limit(limit))

    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "data": data
    }


@app.get("/api/v1/kpis/ca-par-pays", tags=["KPIs"])
def get_ca_par_pays():
    """Récupère le chiffre d'affaires par pays."""
    db = get_mongo_database()
    collection = db["kpi_ca_par_pays"]

    data = list(collection.find({}, {"_id": 0}).sort("ca_total", -1))

    return {
        "total": len(data),
        "data": data
    }


@app.get("/api/v1/kpis/volume-par-produit", tags=["KPIs"])
def get_volume_par_produit():
    """Récupère le volume de ventes par produit."""
    db = get_mongo_database()
    collection = db["kpi_volume_par_produit"]

    data = list(collection.find({}, {"_id": 0}).sort("quantite_totale", -1))

    return {
        "total": len(data),
        "data": data
    }


@app.get("/api/v1/kpis/top-clients", tags=["KPIs"])
def get_top_clients(
    limit: int = Query(10, ge=1, le=100, description="Nombre de top clients")
):
    """Récupère les meilleurs clients par CA."""
    db = get_mongo_database()
    collection = db["kpi_top_clients"]

    data = list(collection.find({}, {"_id": 0}).sort("ca_total", -1).limit(limit))

    return {
        "total": len(data),
        "data": data
    }


@app.get("/api/v1/kpis/stats-distribution", tags=["KPIs"])
def get_stats_distribution():
    """Récupère les statistiques de distribution par produit."""
    db = get_mongo_database()
    collection = db["kpi_stats_distribution"]

    data = list(collection.find({}, {"_id": 0}))

    return {
        "total": len(data),
        "data": data
    }


# ============== Pipeline Status Endpoints ==============

@app.get("/api/v1/pipeline/refresh-times", response_model=list[RefreshTimeResponse], tags=["Pipeline"])
def get_refresh_times():
    """Récupère les temps de rafraîchissement de toutes les collections."""
    db = get_mongo_database()
    sync_collection = db["_sync_metadata"]

    results = []
    for doc in sync_collection.find({}, {"_id": 0}):
        result = {
            "collection_name": doc.get("collection_name"),
            "gold_processed_at": None,
            "mongo_synced_at": None,
            "refresh_time_seconds": None,
            "refresh_time_human": None
        }

        last_sync = doc.get("last_sync")
        if last_sync:
            result["mongo_synced_at"] = last_sync.isoformat() if isinstance(last_sync, datetime) else str(last_sync)

        results.append(result)

    return results


@app.get("/api/v1/pipeline/refresh-time/{collection_name}", response_model=RefreshTimeResponse, tags=["Pipeline"])
def get_collection_refresh_time(collection_name: str):
    """Récupère le temps de rafraîchissement d'une collection spécifique."""
    db = get_mongo_database()
    sync_collection = db["_sync_metadata"]

    doc = sync_collection.find_one({"collection_name": collection_name})
    if not doc:
        raise HTTPException(status_code=404, detail="Collection non trouvée dans les métadonnées")

    result = {
        "collection_name": collection_name,
        "gold_processed_at": None,
        "mongo_synced_at": None,
        "refresh_time_seconds": None,
        "refresh_time_human": None
    }

    last_sync = doc.get("last_sync")
    if last_sync:
        result["mongo_synced_at"] = last_sync.isoformat() if isinstance(last_sync, datetime) else str(last_sync)

    return result


@app.get("/api/v1/pipeline/status", response_model=PipelineStatusResponse, tags=["Pipeline"])
def get_pipeline_status():
    """Récupère le statut global du pipeline."""
    db = get_mongo_database()
    sync_collection = db["_sync_metadata"]

    # Expected collections
    expected_collections = [
        "dim_clients", "dim_produits", "dim_temps",
        "fact_ventes",
        "kpi_ca_par_jour", "kpi_ca_par_mois", "kpi_ca_par_pays",
        "kpi_volume_par_produit", "kpi_top_clients", "kpi_stats_distribution"
    ]

    synced_docs = list(sync_collection.find({}, {"_id": 0}))
    synced_names = {doc["collection_name"] for doc in synced_docs}

    collections_status = []
    for coll_name in expected_collections:
        doc = next((d for d in synced_docs if d.get("collection_name") == coll_name), None)
        status = {
            "name": coll_name,
            "synced": coll_name in synced_names,
            "last_sync": None,
            "row_count": None
        }
        if doc:
            last_sync = doc.get("last_sync")
            if last_sync:
                status["last_sync"] = last_sync.isoformat() if isinstance(last_sync, datetime) else str(last_sync)
            status["row_count"] = doc.get("row_count")

        collections_status.append(status)

    return {
        "total_collections": len(expected_collections),
        "synced_collections": len(synced_names),
        "collections": collections_status,
        "average_refresh_time_seconds": None
    }


# ============== Aggregations Endpoints ==============

@app.get("/api/v1/aggregations/ca-total", tags=["Aggregations"])
def get_ca_total():
    """Calcule le CA total."""
    db = get_mongo_database()
    collection = db["fact_ventes"]

    pipeline = [
        {"$group": {"_id": None, "ca_total": {"$sum": "$montant"}, "nb_ventes": {"$sum": 1}}}
    ]

    result = list(collection.aggregate(pipeline))
    if result:
        return {
            "ca_total": round(result[0]["ca_total"], 2),
            "nb_ventes": result[0]["nb_ventes"]
        }

    return {"ca_total": 0, "nb_ventes": 0}


@app.get("/api/v1/aggregations/ca-par-periode", tags=["Aggregations"])
def get_ca_par_periode(
    periode: str = Query("jour", enum=["jour", "mois", "annee"], description="Granularité")
):
    """Calcule le CA agrégé par période."""
    db = get_mongo_database()
    collection = db["fact_ventes"]

    if periode == "jour":
        group_id = {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_achat"}}
    elif periode == "mois":
        group_id = {"$dateToString": {"format": "%Y-%m", "date": "$date_achat"}}
    else:
        group_id = {"$dateToString": {"format": "%Y", "date": "$date_achat"}}

    pipeline = [
        {"$group": {
            "_id": group_id,
            "ca": {"$sum": "$montant"},
            "nb_ventes": {"$sum": 1}
        }},
        {"$sort": {"_id": -1}},
        {"$project": {
            "_id": 0,
            "periode": "$_id",
            "ca": {"$round": ["$ca", 2]},
            "nb_ventes": 1
        }}
    ]

    data = list(collection.aggregate(pipeline))

    return {
        "granularite": periode,
        "total": len(data),
        "data": data
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
