"""Pipeline medallion Steam: Bronze (raw) -> Silver (tipado + PII) -> Gold (particionado) -> DuckDB.

Cada run procesa UN mes de negocio (data_interval_start) y sobrescribe solo las
particiones de ese mes, por eso re-ejecutarlo da exactamente los mismos conteos.

Nada de este módulo toca la metadata DB al parsear: Variable.get vive dentro de los tasks.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

import pandas as pd
import pendulum
import pyarrow.parquet as pq
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Param, Variable, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from steam_lib import bronze, catalog, config, gold, pii, silver
from steam_lib.io_utils import read_parquet_dir, resolve_window, write_parquet, write_partition

log = logging.getLogger(__name__)


def _window():
    return resolve_window(get_current_context())


def _month_start(month: str) -> pendulum.DateTime:
    year, mm = month.split("-")
    return pendulum.datetime(int(year), int(mm), 1, tz="UTC")


@dag(
    dag_id="steam_medallion_pipeline",
    description="Steam reviews: landing -> bronze -> silver -> gold (hive) -> DuckDB",
    # Intervalos mensuales reales: data_interval_start = 1° del mes, data_interval_end = 1° del siguiente.
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=_month_start(config.FIRST_MONTH),
    end_date=_month_start(config.LAST_MONTH),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        # Los meses se procesan en orden: users/user_profiles se acumulan por cohorte mensual.
        "depends_on_past": True,
    },
    params={
        "month": Param(
            "",
            type="string",
            description="Opcional, YYYY-MM. Si se completa pisa al data interval (re-runs manuales).",
        ),
    },
    tags=["steam", "medallion", "tp-2026"],
)
def steam_medallion_pipeline():

    @task
    def land_raw_files() -> dict:
        window = _window()
        missing = bronze.missing_sources(window)
        if missing:
            raise AirflowSkipException(f"Sin extracción en landing para {window.label}: {missing}")
        landed = bronze.land(window)
        log.info("Mes de negocio %s ingestado en ingested_year=%s/ingested_month=%s",
                 window.label, window.ingested_year, window.ingested_month)
        for table in config.SNAPSHOT_TABLES:
            log.info("Snapshot %s: %s (%s)", table, landed[table]["status"], landed[table]["path"])
        return landed

    @task
    def validate_bronze() -> dict:
        counts = bronze.validate(_window())
        log.info("Filas Bronze: %s", counts)
        return counts

    @task
    def clean_catalog_silver() -> dict:
        window = _window()
        games, games_stats = silver.clean_games(silver.read_raw_csv(bronze.bronze_file("games", window)))
        write_parquet(games, silver.silver_snapshot("games"))

        raw_meta, bad_lines = silver.read_raw_jsonl(bronze.bronze_file("games_metadata", window))
        meta, meta_stats = silver.clean_games_metadata(raw_meta)
        meta_stats["dropped_malformed_lines"] = bad_lines
        write_parquet(meta, silver.silver_snapshot("games_metadata"))
        return {"games": games_stats, "games_metadata": meta_stats}

    @task
    def clean_users_silver() -> dict:
        window = _window()
        users, stats = silver.clean_users(silver.read_raw_csv(bronze.bronze_file("users", window)))
        write_parquet(users, silver.silver_part("users", window))
        return stats

    @task
    def mask_pii() -> dict:
        window = _window()
        salt = Variable.get("PII_SALT")  # en runtime, nunca a nivel de módulo
        profiles, stats = pii.mask_profiles(
            silver.read_raw_csv(bronze.bronze_file("user_profiles", window)), salt
        )
        write_parquet(profiles, silver.silver_part("user_profiles", window))
        return stats

    @task
    def clean_recommendations_silver() -> dict:
        window = _window()
        games = pd.read_parquet(silver.silver_snapshot("games"), columns=["app_id"])
        recs, quarantine, stats = silver.clean_recommendations(
            silver.read_raw_csv(bronze.bronze_file("recommendations", window)), window, games["app_id"]
        )
        write_parquet(recs, silver.silver_part("recommendations", window))
        # La cuarentena se sobrescribe aunque esté vacía, para que un re-run no deje restos.
        write_parquet(quarantine, silver.quarantine_part("recommendations", window))
        log.info("Silver recommendations %s: %s", window.label, stats)
        return stats

    @task
    def build_gold_partition() -> int:
        window = _window()
        df = gold.build(
            recommendations=pd.read_parquet(silver.silver_part("recommendations", window)),
            games=pd.read_parquet(silver.silver_snapshot("games")),
            games_metadata=pd.read_parquet(silver.silver_snapshot("games_metadata")),
            users=read_parquet_dir(config.SILVER_ROOT / "users"),
            profiles=read_parquet_dir(config.SILVER_ROOT / "user_profiles"),
        )
        rows = write_partition(df, gold.gold_partition_dir(window))
        log.info("Gold %s: %s filas en %s", window.label, rows, gold.gold_partition_dir(window))
        return rows

    @task
    def update_duckdb_catalog() -> dict:
        window = _window()
        return catalog.refresh(window.yyyy, window.mm)

    @task
    def audit_counts() -> dict:
        """Deja un registro por mes (sobrescrito) con los conteos que se comparan en el test de idempotencia."""
        window = _window()
        counts = {
            "month": window.label,
            "ingested": f"{window.ingested_year}-{window.ingested_month}",
            "silver_recommendations": pq.read_metadata(silver.silver_part("recommendations", window)).num_rows,
            "silver_quarantine": pq.read_metadata(silver.quarantine_part("recommendations", window)).num_rows,
            "silver_users": pq.read_metadata(silver.silver_part("users", window)).num_rows,
            "silver_user_profiles": pq.read_metadata(silver.silver_part("user_profiles", window)).num_rows,
            "gold_rows": sum(
                pq.read_metadata(f).num_rows for f in gold.gold_partition_dir(window).glob("*.parquet")
            ),
        }
        if counts["gold_rows"] != counts["silver_recommendations"]:
            raise ValueError(f"Gold ({counts['gold_rows']}) != Silver ({counts['silver_recommendations']})")
        config.AUDIT_ROOT.mkdir(parents=True, exist_ok=True)
        (config.AUDIT_ROOT / f"{window.label}.json").write_text(json.dumps(counts, indent=2))
        return counts

    landed = land_raw_files()
    validated = validate_bronze()
    catalog_silver = clean_catalog_silver()
    users_silver = clean_users_silver()
    profiles_silver = mask_pii()
    recs_silver = clean_recommendations_silver()
    gold_partition = build_gold_partition()
    duckdb_catalog = update_duckdb_catalog()
    audit = audit_counts()

    landed >> validated >> [catalog_silver, users_silver, profiles_silver]
    catalog_silver >> recs_silver
    [recs_silver, users_silver, profiles_silver] >> gold_partition >> duckdb_catalog >> audit


steam_medallion_pipeline()
