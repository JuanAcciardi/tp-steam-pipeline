"""Bronze: copia byte a byte desde el origen a carpetas con el linaje de ingesta.

Las tablas incrementales se copian en cada run. Los snapshots (catálogo completo) solo se
copian cuando el origen entrega una versión distinta de la última que ya está en Bronze, así
Bronze registra cada versión que entrega el origen sin duplicar 36 veces el mismo archivo.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
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


def snapshot_dir(table: str, window: Window) -> Path | None:
    """Última partición del snapshot con ingesta <= la del run (no la última en general).

    Así un mes viejo re-ejecutado después de uno más nuevo (o un backfill fuera de orden)
    encuentra la versión del catálogo que había en su momento.
    """
    limit = (window.ingested_year, window.ingested_month)
    name = landing_file(table, window).name
    found = []
    for part in (config.BRONZE_ROOT / table).glob("ingested_year=*/ingested_month=*"):
        key = (part.parent.name.split("=", 1)[1], part.name.split("=", 1)[1])
        if key <= limit and (part / name).exists():
            found.append((key, part))
    return max(found)[1] if found else None


def bronze_file(table: str, window: Window) -> Path:
    if table not in config.SNAPSHOT_TABLES:
        return bronze_dir(table, window) / landing_file(table, window).name
    part = snapshot_dir(table, window)
    if part is None:
        raise FileNotFoundError(
            f"{table}: no hay ningún snapshot en Bronze con ingested_year/ingested_month <= "
            f"{window.ingested_year}-{window.ingested_month} (mes de negocio {window.label}). "
            "Hay que correr land_raw_files para este mes."
        )
    return part / landing_file(table, window).name


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def missing_sources(window: Window) -> list[Path]:
    tables = config.INCREMENTAL_TABLES + config.SNAPSHOT_TABLES
    return [p for p in (landing_file(t, window) for t in tables) if not p.exists()]


def land(window: Window) -> dict[str, dict[str, str]]:
    """Sin parseo ni modificaciones: el archivo queda tal cual lo entregó el origen."""
    landed = {}
    for table in config.INCREMENTAL_TABLES:
        dest = overwrite_dir_with_file(landing_file(table, window), bronze_dir(table, window))
        landed[table] = {"status": "copiado", "path": str(dest)}
    for table in config.SNAPSHOT_TABLES:
        src = landing_file(table, window)
        previous = snapshot_dir(table, window)
        if previous is not None and _sha256(previous / src.name) == _sha256(src):
            landed[table] = {"status": "sin cambios", "path": str(previous / src.name)}
            continue
        dest = overwrite_dir_with_file(src, bronze_dir(table, window))
        landed[table] = {"status": "copiado", "path": str(dest)}
    return landed


def _csv_header_and_rows(path: Path) -> tuple[list[str], int]:
    with path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        rows = sum(1 for _ in reader)
    return header, rows


def _jsonl_keys_and_rows(path: Path) -> tuple[list[str], int]:
    keys: list[str] = []
    rows = 0
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            if not keys:
                try:
                    keys = list(json.loads(line).keys())
                except (json.JSONDecodeError, AttributeError):
                    pass
            rows += 1
    return keys, rows


def validate(window: Window) -> dict[str, int]:
    """Valida contrato mínimo (existe, no vacío, columnas esperadas). Falla el run si no."""
    counts, errors = {}, []
    for table, expected in config.EXPECTED_COLUMNS.items():
        try:
            path = bronze_file(table, window)
        except FileNotFoundError as exc:
            errors.append(str(exc))
            continue
        if not path.exists():
            errors.append(f"{table}: falta {path}")
            continue
        reader = _jsonl_keys_and_rows if path.suffix == ".json" else _csv_header_and_rows
        columns, rows = reader(path)
        # Mismo criterio que Silver: ' App ID' y 'APP_ID' cuentan como app_id.
        normalized = {re.sub(r"[\s\-]+", "_", c.strip().lower()) for c in columns}
        missing = sorted(set(expected) - normalized)
        if missing:
            errors.append(f"{table}: faltan columnas {missing}")
        # Un mes sin altas de usuarios es válido; sin reviews o sin catálogo, no.
        if rows == 0 and table not in ("users", "user_profiles"):
            errors.append(f"{table}: archivo vacío")
        counts[table] = rows
    if errors:
        raise ValueError("Validación Bronze fallida:\n" + "\n".join(errors))
    return counts
