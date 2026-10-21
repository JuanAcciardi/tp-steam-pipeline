"""Silver: tipado, higiene, deduplicación y reglas de nulos/huérfanos.

Cada regla de nulos está documentada con su justificación en README.md
(sección "Reglas de calidad de datos"). Si cambian una regla acá, actualicen el README.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from steam_lib import config
from steam_lib.io_utils import Window

TRUE_TOKENS = {"true", "t", "1", "yes", "y"}
FALSE_TOKENS = {"false", "f", "0", "no", "n"}

REVIEW_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d")
RELEASE_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y")


# --------------------------------------------------------------------------- paths

def silver_part(table: str, window: Window) -> Path:
    return config.SILVER_ROOT / table / f"part-{window.label}.parquet"


def silver_snapshot(table: str) -> Path:
    return config.SILVER_ROOT / table / f"{table}.parquet"


def quarantine_part(table: str, window: Window) -> Path:
    return config.QUARANTINE_ROOT / table / f"part-{window.label}.parquet"


# --------------------------------------------------------------------------- parsing helpers

def read_raw_csv(path: Path) -> pd.DataFrame:
    """Todo como string: el tipado lo decide Silver, no el inferidor de pandas."""
    return pd.read_csv(path, dtype="string", keep_default_na=False, na_values=[""])


def clean_str(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip()
    return s.mask(s == "")


def parse_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(clean_str(s), errors="coerce").astype("Int64")


def parse_float(s: pd.Series) -> pd.Series:
    s = clean_str(s).str.replace("$", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").astype("Float64")


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
    if isinstance(value, (list, tuple, np.ndarray)):
        return [str(v).strip().lower() for v in value if str(v).strip()]
    return []


# --------------------------------------------------------------------------- tables

def clean_games(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "app_id": parse_int(raw["app_id"]),
        "title": clean_str(raw["title"]),
        "date_release": parse_dates(raw["date_release"], RELEASE_DATE_FORMATS),
        "win": parse_bool(raw["win"]).fillna(False),
        "mac": parse_bool(raw["mac"]).fillna(False),
        "linux": parse_bool(raw["linux"]).fillna(False),
        "steam_deck": parse_bool(raw["steam_deck"]).fillna(False),
        "rating": clean_str(raw["rating"]).str.title().fillna("No Rating"),
        "positive_ratio": parse_int(raw["positive_ratio"]),
        "user_reviews": parse_int(raw["user_reviews"]),
        "price_final": parse_float(raw["price_final"]),
        "price_original": parse_float(raw["price_original"]),
        "discount": parse_float(raw["discount"]),
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
        "description": clean_str(raw["description"].astype("string")),
        "tags": raw["tags"].map(_as_list),
    })
    stats["dropped_null_app_id"] = int(df["app_id"].isna().sum())
    df = df[df["app_id"].notna()]
    before = len(df)
    df = df.drop_duplicates("app_id", keep="last")
    stats["dropped_duplicates"] = before - len(df)
    df = df.sort_values("app_id").reset_index(drop=True)
    stats["rows_out"] = len(df)
    return df, stats


def clean_users(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "user_id": parse_int(raw["user_id"]),
        "products": parse_int(raw["products"]),
        "reviews": parse_int(raw["reviews"]),
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
        "updated_at": pd.to_datetime(clean_str(raw["updated_at"]), format="%Y-%m-%d %H:%M:%S", errors="coerce"),
        "is_recommended": parse_bool(raw["is_recommended"]),
        "hours": parse_float(raw["hours"]),
        "helpful": parse_int(raw["helpful"]),
        "funny": parse_int(raw["funny"]),
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
    df["hours"] = df["hours"].mask(df["hours"] < 0)  # negativas = error de medición -> nulo

    # Deduplicación: por review_id gana la versión con updated_at más reciente.
    before = len(df)
    df = df.sort_values(["review_id", "updated_at"], ascending=[True, False])
    df = df.drop_duplicates("review_id", keep="first")
    stats["dropped_duplicates"] = before - len(df)

    orphan = ~df["app_id"].isin(valid_app_ids)
    quarantine = df[orphan].reset_index(drop=True)
    df = df[~orphan].reset_index(drop=True)
    stats["quarantined_orphan_app_id"] = len(quarantine)
    stats["rows_out"] = len(df)
    return df, quarantine, stats
