"""Gold: tabla desnormalizada review x juego x usuario, particionada por mes de negocio."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from steam_lib import config
from steam_lib.io_utils import Window


def gold_partition_dir(window: Window) -> Path:
    return config.GOLD_ROOT / config.GOLD_DATASET / f"year={window.yyyy}" / f"month={window.mm}"


def _tags(value) -> list[str]:
    return list(value) if isinstance(value, (list, tuple, np.ndarray)) else []


def build(
    recommendations: pd.DataFrame,
    games: pd.DataFrame,
    games_metadata: pd.DataFrame,
    users: pd.DataFrame,
    profiles: pd.DataFrame,
) -> pd.DataFrame:
    """Grano: una fila por review. Los joins son N:1, así que no puede haber fan-out."""
    games = games.rename(columns={
        "title": "game_title",
        "date_release": "game_release_date",
        "rating": "game_rating",
        "user_reviews": "game_review_count",
    })
    users = users.rename(columns={"products": "user_products", "reviews": "user_review_count"})

    df = (
        recommendations
        # inner: los huérfanos de app_id ya se mandaron a cuarentena en Silver
        .merge(games, on="app_id", how="inner", validate="many_to_one")
        .merge(games_metadata, on="app_id", how="left", validate="many_to_one")
        # left: un usuario sin perfil no invalida la review (ver README)
        .merge(users, on="user_id", how="left", validate="many_to_one")
        .merge(profiles, on="user_id", how="left", validate="many_to_one")
    )
    if len(df) != len(recommendations):
        raise AssertionError(f"Fan-out en Gold: {len(recommendations)} reviews -> {len(df)} filas")

    df["tags"] = df["tags"].map(_tags)
    df["primary_tag"] = df["tags"].map(lambda t: t[0] if t else "untagged")
    df["country_code"] = df["country_code"].fillna("ZZ")
    df["review_date"] = df["review_date"].dt.date
    df["game_release_date"] = df["game_release_date"].dt.date

    columns = [
        "review_id", "review_date", "updated_at", "is_recommended", "hours", "helpful", "funny",
        "user_id", "email_hash", "username_hash", "country_code", "user_products", "user_review_count",
        "app_id", "game_title", "game_release_date", "game_rating", "positive_ratio", "game_review_count",
        "price_final", "price_original", "discount", "win", "mac", "linux", "steam_deck",
        "primary_tag", "tags",
    ]
    return df[columns].sort_values("review_id").reset_index(drop=True)
