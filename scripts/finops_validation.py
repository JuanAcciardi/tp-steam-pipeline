"""Valida el partition pruning de Gold con DuckDB EXPLAIN ANALYZE (equivalente local de FinOps).

Uso:
    python scripts/finops_validation.py                       # usa la partición más grande
    python scripts/finops_validation.py --year 2021 --month 03 --out docs/finops_output.txt
"""

from __future__ import annotations

import argparse
import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "dags"))

from steam_lib.catalog import gold_glob  # noqa: E402


def explain(conn: duckdb.DuckDBPyConnection, sql: str) -> str:
    rows = conn.execute(sql).fetchall()
    return "\n".join(str(r[-1]) for r in rows)


def files_and_rows(conn: duckdb.DuckDBPyConnection, glob: str, where: str = "") -> tuple[int, int]:
    return conn.execute(f"""
        SELECT COUNT(DISTINCT filename), COUNT(*)
        FROM read_parquet('{glob}', hive_partitioning = true, filename = true) {where}
    """).fetchone()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year")
    parser.add_argument("--month")
    parser.add_argument("--out", type=Path, help="Guarda también la salida en este archivo")
    args = parser.parse_args()

    glob = gold_glob()
    conn = duckdb.connect()
    conn.execute("PRAGMA enable_profiling = 'query_tree'")

    year, month = args.year, args.month
    if not (year and month):
        year, month = conn.execute(f"""
            SELECT year, month FROM read_parquet('{glob}', hive_partitioning = true)
            GROUP BY ALL ORDER BY COUNT(*) DESC LIMIT 1
        """).fetchone()
    year, month = str(year), f"{int(month):02d}"

    full_sql = (REPO / "sql" / "finops_full_scan.sql").read_text().format(gold_glob=glob)
    pruned_sql = (REPO / "sql" / "finops_pruned_scan.sql").read_text().format(
        gold_glob=glob, year=year, month=month
    )
    full_files, full_rows = files_and_rows(conn, glob)
    pruned_files, pruned_rows = files_and_rows(conn, glob, f"WHERE year = '{year}' AND month = '{month}'")

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        print("=== FULL SCAN (sin filtro de partición) ===")
        print(explain(conn, full_sql))
        print(f"\n=== PRUNED SCAN (year={year}, month={month}) ===")
        print(explain(conn, pruned_sql))
        print("\n=== RESUMEN ===")
        print(f"{'':<14}{'Parquet leídos':>16}{'Filas leídas':>16}")
        print(f"{'Full scan':<14}{full_files:>16,}{full_rows:>16,}")
        print(f"{'Pruned scan':<14}{pruned_files:>16,}{pruned_rows:>16,}")
        ratio = 100 * (1 - pruned_rows / full_rows) if full_rows else 0
        print(f"\nEl filtro de partición evita leer el {ratio:.1f}% de las filas.")
        if pruned_files >= full_files:
            print("ERROR: el pruning no está funcionando, revisar la estructura year=/month=.")

    output = buffer.getvalue()
    print(output)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(output, encoding="utf-8")
        print(f"Salida guardada en {args.out}")


if __name__ == "__main__":
    main()
