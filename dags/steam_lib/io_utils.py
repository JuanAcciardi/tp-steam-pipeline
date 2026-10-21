"""Ventana temporal del run y escrituras idempotentes (overwrite atómico)."""

from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from steam_lib import config


@dataclass(frozen=True)
class Window:
    """Mes de negocio que procesa el run y mes en que se ingesta su extracción.

    El origen entrega la extracción del mes M el día 1 del mes M+1, por eso la
    fecha de ingesta (path de Bronze) es siempre el mes siguiente a la fecha de
    negocio (partición de Gold). Para un run programado coincide con
    data_interval_end.
    """

    business_start: date
    business_end: date  # exclusivo

    @property
    def yyyy(self) -> str:
        return f"{self.business_start.year:04d}"

    @property
    def mm(self) -> str:
        return f"{self.business_start.month:02d}"

    @property
    def label(self) -> str:
        return f"{self.yyyy}-{self.mm}"

    @property
    def ingested_year(self) -> str:
        return f"{self.business_end.year:04d}"

    @property
    def ingested_month(self) -> str:
        return f"{self.business_end.month:02d}"


def window_for_month(month: str) -> Window:
    """'2021-03' -> Window(2021-03-01, 2021-04-01)."""
    start = pd.Timestamp(f"{month}-01").date()
    end = (pd.Timestamp(start) + pd.offsets.MonthBegin(1)).date()
    return Window(start, end)


def resolve_window(context: dict) -> Window:
    """El param `month` (YYYY-MM) pisa al data interval en triggers manuales."""
    override = (context.get("params") or {}).get("month") or ""
    if override.strip():
        return window_for_month(override.strip())
    start = context["data_interval_start"]
    return window_for_month(f"{start.year:04d}-{start.month:02d}")


def _tmp_path(name: str) -> Path:
    config.TMP_ROOT.mkdir(parents=True, exist_ok=True)
    return config.TMP_ROOT / f"{name}.{uuid.uuid4().hex}"


def replace_dir(staged: Path, target: Path) -> None:
    """Reemplaza `target` completo por `staged`. Nunca deja archivos de un run anterior."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    os.replace(staged, target)


def overwrite_dir_with_file(src: Path, target_dir: Path) -> Path:
    """Copia `src` a una carpeta temporal y la intercambia por `target_dir` (Bronze)."""
    staged = _tmp_path(target_dir.name)
    staged.mkdir(parents=True)
    shutil.copy2(src, staged / src.name)
    replace_dir(staged, target_dir)
    return target_dir / src.name


def write_parquet(df: pd.DataFrame, target_file: Path, schema: pa.Schema | None = None) -> int:
    """Escribe un único Parquet reemplazando el anterior de forma atómica."""
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    target_file.parent.mkdir(parents=True, exist_ok=True)
    staged = _tmp_path(target_file.name)
    pq.write_table(table, staged, compression="snappy")
    os.replace(staged, target_file)
    return table.num_rows


def write_partition(df: pd.DataFrame, partition_dir: Path) -> int:
    """Overwrite de una partición Hive completa (Gold): se borra y se reescribe la carpeta."""
    staged = _tmp_path(partition_dir.name)
    staged.mkdir(parents=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, staged / "part-0.parquet", compression="snappy")
    replace_dir(staged, partition_dir)
    return table.num_rows


def read_parquet_dir(path: Path) -> pd.DataFrame:
    files = sorted(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No hay Parquet en {path}")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
