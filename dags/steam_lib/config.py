"""Configuración central del pipeline: rutas, ventana temporal y nombres de tablas.

Todo lo que dependa del entorno se resuelve con variables de entorno con defaults
razonables, así cada integrante corre lo mismo sin tocar código.
"""

from __future__ import annotations

import os
from pathlib import Path

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "~/airflow")).expanduser()

# Sistema origen: dataset fuente publicado en el release dataset-v1 del repo. No es parte del medallion.
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

# Ventana del backfill. Tiene que coincidir con los meses de landing/_manifest.json.
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

# Contrato mínimo que valida Bronze: sin estas columnas el run falla. El resto de las
# columnas son opcionales (Silver las trata como nulas si una extracción futura no las trae).
EXPECTED_COLUMNS = {
    "recommendations": ["review_id", "app_id", "user_id", "date", "is_recommended"],
    "users": ["user_id"],
    "user_profiles": ["user_id"],
    "games": ["app_id"],
    "games_metadata": ["app_id"],
}
