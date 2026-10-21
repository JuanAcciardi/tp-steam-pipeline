"""Silver: tipado, higiene, deduplicación y reglas de nulos/huérfanos.

Cada regla de nulos está documentada con su justificación en README.md
(sección "Reglas de calidad de datos"). Si cambian una regla acá, actualicen el README.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from steam_lib import config
from steam_lib.io_utils import Window

# Las reglas de parseo son genéricas a propósito: cubren los problemas que ya trae el
# origen y también los que podrían aparecer en extracciones futuras.
NULL_TOKENS = {"", "null", "none", "nan", "nat", "n/a", "na", "-", "--", "?", "undefined"}
TRUE_TOKENS = {"true", "t", "1", "1.0", "yes", "y", "si", "sí", "recommended"}
FALSE_TOKENS = {"false", "f", "0", "0.0", "no", "n", "not recommended"}

# Orden importa: los formatos ambiguos (día/mes) van después de los inequívocos.
REVIEW_DATE_FORMATS = (
    "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d", "%Y%m%d",
    "%d/%m/%Y", "%d-%m-%Y", "%b %d, %Y", "%d %b, %Y",
)
RELEASE_DATE_FORMATS = REVIEW_DATE_FORMATS
UPDATED_AT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


# --------------------------------------------------------------------------- paths

def silver_part(table: str, window: Window) -> Path:
    return config.SILVER_ROOT / table / f"part-{window.label}.parquet"


def silver_snapshot(table: str) -> Path:
    return config.SILVER_ROOT / table / f"{table}.parquet"


def quarantine_part(table: str, window: Window) -> Path:
    return config.QUARANTINE_ROOT / table / f"part-{window.label}.parquet"


# --------------------------------------------------------------------------- parsing helpers

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """' App ID' / 'APP_ID' -> 'app_id'. Columnas extra se ignoran más adelante."""
    df.columns = (
        pd.Index(df.columns).str.strip().str.lower().str.replace(r"[\s\-]+", "_", regex=True)
    )
    return df


def read_raw_csv(path: Path) -> pd.DataFrame:
    """Todo como string: el tipado lo decide Silver, no el inferidor de pandas.

    encoding_errors='replace': un byte inválido no tira abajo el run, queda como U+FFFD.
    """
    df = pd.read_csv(
        path, dtype="string", keep_default_na=False, na_values=[""],
        encoding="utf-8", encoding_errors="replace",
    )
    return normalize_columns(df)


def read_raw_jsonl(path: Path) -> tuple[pd.DataFrame, int]:
    """JSON lines tolerante: una línea corrupta se descarta y se cuenta, no rompe el run."""
    records, bad = [], 0
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue
            if isinstance(rec, dict):
                records.append(rec)
            else:
                bad += 1
    return normalize_columns(pd.DataFrame.from_records(records)), bad


def clean_str(s: pd.Series) -> pd.Series:
    """Trim (incluye NBSP y zero-width), colapsa espacios y convierte tokens de nulo en NA."""
    s = (
        s.astype("string")
        .str.replace(" ", " ", regex=False)
        .str.replace("​", "", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return s.mask(s.str.lower().isin(NULL_TOKENS))


def parse_float(s: pd.Series) -> pd.Series:
    """Acepta '1,234.5', '1.234,5', '12,5', '$ 9.99', 'USD 9.99'."""
    s = clean_str(s).str.replace(r"[^\d,.\-eE+]", "", regex=True)
    both = s.str.contains(",", regex=False) & s.str.contains(".", regex=False)
    comma_is_decimal = both & (s.str.rfind(",") > s.str.rfind("."))
    s = s.mask(both & ~comma_is_decimal, s.str.replace(",", "", regex=False))
    s = s.mask(comma_is_decimal, s.str.replace(".", "", regex=False))
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").astype("Float64")


def parse_int(s: pd.Series) -> pd.Series:
    """'12', '12.0', '1,234' -> entero. Valores con decimales reales -> NA (no se trunca)."""
    s = clean_str(s)
    # En columnas enteras, '1,234' / '1.234.567' son separadores de miles, no decimales.
    s = s.mask(s.str.fullmatch(r"-?\d{1,3}([,.]\d{3})+").fillna(False), s.str.replace(r"[,.]", "", regex=True))
    f = parse_float(s)
    return f.mask(f.notna() & (f != f.round())).round().astype("Int64")


def in_range(s: pd.Series, low: float | None = None, high: float | None = None) -> pd.Series:
    """Valores fuera del dominio de negocio -> NA (la regla de imputación decide después)."""
    bad = pd.Series(False, index=s.index)
    if low is not None:
        bad |= s < low
    if high is not None:
        bad |= s > high
    return s.mask(bad.fillna(False))


def parse_bool(s: pd.Series) -> pd.Series:
    s = clean_str(s).str.lower()
    out = pd.Series(pd.NA, index=s.index, dtype="boolean")
    out[s.isin(TRUE_TOKENS)] = True
    out[s.isin(FALSE_TOKENS)] = False
    return out


def parse_dates(s: pd.Series, formats: tuple[str, ...]) -> pd.Series:
    """Prueba cada formato conocido en orden; lo que no matchea ninguno queda NaT."""
    s = clean_str(s)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in formats:
        pending = out.isna() & s.notna()
        if not pending.any():
            break
        out[pending] = pd.to_datetime(s[pending], format=fmt, errors="coerce")
    return out


def _as_list(value) -> list[str]:
    """Tags como lista normalizada; también acepta un string 'a, b; c' si el origen cambia de formato."""
    if isinstance(value, str):
        value = re.split(r"[,;|]", value)
    if isinstance(value, (list, tuple, np.ndarray)):
        tags = (str(v).strip().lower() for v in value)
        return list(dict.fromkeys(t for t in tags if t and t not in NULL_TOKENS))
    return []


def optional(raw: pd.DataFrame, column: str) -> pd.Series:
    """Columna opcional: si una extracción futura no la trae, se trata como toda nula."""
    if column in raw.columns:
        return raw[column].astype("string")
    return pd.Series(pd.NA, index=raw.index, dtype="string")


# --------------------------------------------------------------------------- tables

def clean_games(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "app_id": parse_int(raw["app_id"]),
        "title": clean_str(optional(raw, "title")),
        "date_release": parse_dates(optional(raw, "date_release"), RELEASE_DATE_FORMATS),
        "win": parse_bool(optional(raw, "win")).fillna(False),
        "mac": parse_bool(optional(raw, "mac")).fillna(False),
        "linux": parse_bool(optional(raw, "linux")).fillna(False),
        "steam_deck": parse_bool(optional(raw, "steam_deck")).fillna(False),
        "rating": clean_str(optional(raw, "rating")).str.title().fillna("No Rating"),
        "positive_ratio": in_range(parse_int(optional(raw, "positive_ratio")), 0, 100),
        "user_reviews": in_range(parse_int(optional(raw, "user_reviews")), 0),
        "price_final": in_range(parse_float(optional(raw, "price_final")), 0),
        "price_original": in_range(parse_float(optional(raw, "price_original")), 0),
        "discount": in_range(parse_float(optional(raw, "discount")), 0, 100),
    })
    stats["dropped_null_app_id"] = int(df["app_id"].isna().sum())
    df = df[df["app_id"].notna()].copy()
    df["title"] = df["title"].fillna("Unknown Title")

    # El catálogo es un snapshot append-only: la última fila de cada app_id es la vigente.
    before = len(df)
    df = df.drop_duplicates("app_id", keep="last")
    stats["dropped_duplicates"] = before - len(df)

    df = df.sort_values("app_id").reset_index(drop=True)
    stats["rows_out"] = len(df)
    return df, stats


def clean_games_metadata(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "app_id": parse_int(raw["app_id"].astype("string")),
        "description": clean_str(optional(raw, "description")),
        # Sin optional(): los tags llegan como listas y no hay que pasarlos a string.
        "tags": raw["tags"].map(_as_list) if "tags" in raw.columns else [[] for _ in range(len(raw))],
    })
    stats["dropped_null_app_id"] = int(df["app_id"].isna().sum())
    df = df[df["app_id"].notna()]
    before = len(df)
    # Mismo criterio que games: la última versión del snapshot es la vigente.
    df = df.drop_duplicates("app_id", keep="last")
    stats["dropped_duplicates"] = before - len(df)
    df = df.sort_values("app_id").reset_index(drop=True)
    stats["rows_out"] = len(df)
    return df, stats


def clean_users(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "user_id": parse_int(raw["user_id"]),
        "products": in_range(parse_int(optional(raw, "products")), 0),
        "reviews": in_range(parse_int(optional(raw, "reviews")), 0),
    })
    stats["dropped_null_user_id"] = int(df["user_id"].isna().sum())
    df = df[df["user_id"].notna()]
    before = len(df)
    df = df.drop_duplicates("user_id", keep="last")
    stats["dropped_duplicates"] = before - len(df)
    df = df.sort_values("user_id").reset_index(drop=True)
    stats["rows_out"] = len(df)
    return df, stats


def clean_recommendations(
    raw: pd.DataFrame, window: Window, valid_app_ids: pd.Series
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Devuelve (silver, cuarentena de huérfanos, estadísticas)."""
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "review_id": parse_int(raw["review_id"]),
        "app_id": parse_int(raw["app_id"]),
        "user_id": parse_int(raw["user_id"]),
        "review_date": parse_dates(raw["date"], REVIEW_DATE_FORMATS),
        "updated_at": parse_dates(optional(raw, "updated_at"), UPDATED_AT_FORMATS),
        "is_recommended": parse_bool(raw["is_recommended"]),
        "hours": in_range(parse_float(optional(raw, "hours")), 0),
        "helpful": in_range(parse_int(optional(raw, "helpful")), 0),
        "funny": in_range(parse_int(optional(raw, "funny")), 0),
    })

    required = ["review_id", "app_id", "user_id", "review_date", "is_recommended"]
    null_mask = df[required].isna().any(axis=1)
    for col in required:
        stats[f"dropped_null_{col}"] = int(df[col].isna().sum())
    df = df[~null_mask]

    in_window = (df["review_date"] >= pd.Timestamp(window.business_start)) & (
        df["review_date"] < pd.Timestamp(window.business_end)
    )
    stats["dropped_out_of_window"] = int((~in_window).sum())
    df = df[in_window].copy()

    df["updated_at"] = df["updated_at"].fillna(df["review_date"])
    df["helpful"] = df["helpful"].fillna(0)
    df["funny"] = df["funny"].fillna(0)

    # Deduplicación: por review_id gana la versión con updated_at más reciente.
    before = len(df)
    # mergesort es estable: ante empate de updated_at el resultado no depende del azar.
    df = df.sort_values(["review_id", "updated_at"], ascending=[True, False], kind="mergesort")
    df = df.drop_duplicates("review_id", keep="first")
    stats["dropped_duplicates"] = before - len(df)

    orphan = ~df["app_id"].isin(valid_app_ids)
    quarantine = df[orphan].reset_index(drop=True)
    df = df[~orphan].reset_index(drop=True)
    stats["quarantined_orphan_app_id"] = len(quarantine)
    stats["rows_out"] = len(df)
    return df, quarantine, stats
