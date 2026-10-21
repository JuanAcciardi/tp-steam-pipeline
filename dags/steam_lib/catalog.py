"""Catálogo local: vistas DuckDB sobre Gold (y Silver) con hive_partitioning."""

from __future__ import annotations

import duckdb

from steam_lib import config


def gold_glob() -> str:
    return (config.GOLD_ROOT / config.GOLD_DATASET).as_posix() + "/**/*.parquet"


def refresh(year: str, month: str) -> dict:
    """Recrea las vistas (idempotente) y devuelve el conteo de la partición del run."""
    silver = config.SILVER_ROOT.as_posix()
    with duckdb.connect(str(config.CATALOG_DB)) as conn:
        conn.execute(f"""
            CREATE OR REPLACE VIEW {config.GOLD_DATASET} AS
            SELECT * FROM read_parquet('{gold_glob()}', hive_partitioning = true)
        """)
        for table in ("recommendations", "users", "user_profiles", "games", "games_metadata"):
            conn.execute(f"""
                CREATE OR REPLACE VIEW silver_{table} AS
                SELECT * FROM read_parquet('{silver}/{table}/*.parquet')
            """)
        partition_rows = conn.execute(
            f"SELECT COUNT(*) FROM {config.GOLD_DATASET} WHERE year = ? AND month = ?",
            [int(year), int(month)],
        ).fetchone()[0]
        partitions = conn.execute(
            f"SELECT COUNT(DISTINCT (year, month)) FROM {config.GOLD_DATASET}"
        ).fetchone()[0]
    return {"partition_rows": int(partition_rows), "total_partitions": int(partitions)}
