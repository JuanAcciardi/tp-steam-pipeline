"""Configuración central del pipeline: rutas, ventana temporal y nombres de tablas.

Todo lo que dependa del entorno se resuelve con variables de entorno con defaults
razonables, así cada integrante corre lo mismo sin tocar código.
"""

from __future__ import annotations

import os
from pathlib import Path

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "~/airflow")).expanduser()

# Sistema origen simulado (lo genera scripts/prepare_source.py). No es parte del medallion.
LANDING_ROOT = Path(os.environ.get("STEAM_LANDING_ROOT", AIRFLOW_HOME / "landing")).expanduser()

# Capas medallion exigidas por la consigna.
DATA_ROOT = Path(os.environ.get("STEAM_DATA_ROOT", AIRFLOW_HOME / "data")).expanduser()
BRONZE_ROOT = DATA_ROOT / "bronze"
SILVER_ROOT = DATA_ROOT / "silver"
GOLD_ROOT = DATA_ROOT / "gold"
QUARANTINE_ROOT = SILVER_ROOT / "_quarantine"
AUDIT_ROOT = DATA_ROOT / "_audit"
TMP_ROOT = DATA_ROOT / "_tmp"  # mismo filesystem que las capas -> os.replace es atómico
CATALOG_DB = DATA_ROOT / "catalog.duckdb"

GOLD_DATASET = "user_game_reviews"

# Ventana del backfill. Ajustar a lo que imprime scripts/prepare_source.py (manifest).
FIRST_MONTH = "2020-01"
LAST_MONTH = "2022-12"

# Tablas incrementales: el origen entrega un archivo por mes de negocio.
INCREMENTAL_TABLES = ("recommendations", "users", "user_profiles")
# Tablas snapshot: el origen entrega el catálogo completo en cada extracción.
SNAPSHOT_TABLES = ("games", "games_metadata")

LANDING_FILES = {
    "recommendations": "recommendations_{yyyy}_{mm}.csv",
    "users": "users_{yyyy}_{mm}.csv",
    "user_profiles": "user_profiles_{yyyy}_{mm}.csv",
    "games": "games.csv",
    "games_metadata": "games_metadata.json",
}

EXPECTED_COLUMNS = {
    "recommendations": [
        "app_id", "helpful", "funny", "date", "is_recommended",
        "hours", "user_id", "review_id", "updated_at",
    ],
    "users": ["user_id", "products", "reviews"],
    "user_profiles": ["user_id", "username", "full_name", "email", "phone_number", "country_code"],
    "games": [
        "app_id", "title", "date_release", "win", "mac", "linux", "rating",
        "positive_ratio", "user_reviews", "price_final", "price_original",
        "discount", "steam_deck",
    ],
    "games_metadata": ["app_id", "description", "tags"],
}
