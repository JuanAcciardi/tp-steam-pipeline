"""Bronze: copia byte a byte desde el origen a carpetas con el linaje de ingesta."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from steam_lib import config
from steam_lib.io_utils import Window, overwrite_dir_with_file


def landing_file(table: str, window: Window) -> Path:
    name = config.LANDING_FILES[table].format(yyyy=window.yyyy, mm=window.mm)
    return config.LANDING_ROOT / table / name


def bronze_dir(table: str, window: Window) -> Path:
    return (
        config.BRONZE_ROOT
        / table
        / f"ingested_year={window.ingested_year}"
        / f"ingested_month={window.ingested_month}"
    )


def bronze_file(table: str, window: Window) -> Path:
    return bronze_dir(table, window) / landing_file(table, window).name


def missing_sources(window: Window) -> list[Path]:
    tables = config.INCREMENTAL_TABLES + config.SNAPSHOT_TABLES
    return [p for p in (landing_file(t, window) for t in tables) if not p.exists()]


def land(window: Window) -> dict[str, str]:
    """Sin parseo ni modificaciones: el archivo queda tal cual lo entregó el origen."""
    landed = {}
    for table in config.INCREMENTAL_TABLES + config.SNAPSHOT_TABLES:
        dest = overwrite_dir_with_file(landing_file(table, window), bronze_dir(table, window))
        landed[table] = str(dest)
    return landed


def _csv_header_and_rows(path: Path) -> tuple[list[str], int]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        rows = sum(1 for _ in reader)
    return header, rows


def _jsonl_keys_and_rows(path: Path) -> tuple[list[str], int]:
    keys: list[str] = []
    rows = 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            if rows == 0:
                keys = list(json.loads(line).keys())
            rows += 1
    return keys, rows


def validate(window: Window) -> dict[str, int]:
    """Valida contrato mínimo (existe, no vacío, columnas esperadas). Falla el run si no."""
    counts, errors = {}, []
    for table, expected in config.EXPECTED_COLUMNS.items():
        path = bronze_file(table, window)
        if not path.exists():
            errors.append(f"{table}: falta {path}")
            continue
        reader = _jsonl_keys_and_rows if path.suffix == ".json" else _csv_header_and_rows
        columns, rows = reader(path)
        missing = sorted(set(expected) - set(columns))
        if missing:
            errors.append(f"{table}: faltan columnas {missing}")
        # Un mes sin altas de usuarios es válido; sin reviews o sin catálogo, no.
        if rows == 0 and table not in ("users", "user_profiles"):
            errors.append(f"{table}: archivo vacío")
        counts[table] = rows
    if errors:
        raise ValueError("Validación Bronze fallida:\n" + "\n".join(errors))
    return counts
