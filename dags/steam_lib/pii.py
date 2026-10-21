"""Gobernanza de PII: seudonimización con HMAC-SHA256 usando el salt de la Airflow Variable.

El salt nunca se lee acá: lo recibe por parámetro desde el @task, que lo obtiene
con Variable.get("PII_SALT") en tiempo de ejecución.
"""

from __future__ import annotations

import hashlib
import hmac

import pandas as pd

from steam_lib.silver import clean_str, parse_int

# Columnas identificatorias que no tienen valor analítico: se eliminan en Silver.
DROPPED_PII = ("full_name", "phone_number")


def hash_value(value: str, salt: str) -> str:
    """HMAC-SHA256(salt, valor): mismo input + mismo salt -> mismo hash (joinable)."""
    return hmac.new(salt.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def hash_series(s: pd.Series, salt: str) -> pd.Series:
    """Normaliza (trim + lower) antes de hashear para que 'A@x.com ' y 'a@x.com' coincidan."""
    normalized = clean_str(s).str.lower()
    return pd.Series(
        [hash_value(v, salt) if isinstance(v, str) else None for v in normalized],
        index=s.index,
        dtype="string",
    )


def mask_profiles(raw: pd.DataFrame, salt: str) -> tuple[pd.DataFrame, dict]:
    if not salt:
        raise ValueError("PII_SALT vacío: configurarlo con `airflow variables set PII_SALT ...`")
    stats = {"rows_in": len(raw)}
    df = pd.DataFrame({
        "user_id": parse_int(raw["user_id"]),
        "email_hash": hash_series(raw["email"], salt),
        "username_hash": hash_series(raw["username"], salt),
        "country_code": clean_str(raw["country_code"]).str.upper().fillna("ZZ"),
    })
    stats["dropped_null_user_id"] = int(df["user_id"].isna().sum())
    stats["null_email"] = int(df["email_hash"].isna().sum())
    df = df[df["user_id"].notna()]
    before = len(df)
    df = df.drop_duplicates("user_id", keep="last")
    stats["dropped_duplicates"] = before - len(df)
    df = df.sort_values("user_id").reset_index(drop=True)
    stats["rows_out"] = len(df)
    return df, stats
