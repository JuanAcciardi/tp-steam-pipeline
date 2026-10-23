"""Deduplicación (una fila por clave, la más reciente) y parseo de fechas en los formatos sucios."""

import pandas as pd

from steam_lib.io_utils import window_for_month
from steam_lib.silver import REVIEW_DATE_FORMATS, clean_recommendations, clean_users, parse_dates

RECS_COLUMNS = ["review_id", "app_id", "user_id", "date", "is_recommended", "hours", "helpful", "funny", "updated_at"]


def _raw(rows: list[list[str]], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=columns, dtype="string")


def test_review_editada_gana_la_version_mas_reciente_aunque_venga_primero():
    raw = _raw([
        # review 1: la edición (más reciente) aparece antes que la original
        ["1", "10", "100", "2021-03-02", "false", "5", "0", "0", "2021-03-20 10:00:00"],
        ["1", "10", "100", "2021-03-02", "true", "5", "0", "0", "2021-03-02 08:00:00"],
        # review 2: duplicado exacto
        ["2", "10", "200", "2021-03-05", "true", "1", "0", "0", "2021-03-05 09:00:00"],
        ["2", "10", "200", "2021-03-05", "true", "1", "0", "0", "2021-03-05 09:00:00"],
        ["3", "10", "300", "2021-03-07", "TRUE", "2,5", "", "", ""],
    ], RECS_COLUMNS)
    recs, quarantine, stats = clean_recommendations(raw, window_for_month("2021-03"), pd.Series([10]))

    assert recs["review_id"].tolist() == [1, 2, 3]
    assert recs["review_id"].is_unique
    assert not recs.set_index("review_id").loc[1, "is_recommended"]  # quedó la edición
    assert stats["dropped_duplicates"] == 2
    assert quarantine.empty


def test_usuario_duplicado_queda_una_sola_fila_la_ultima():
    raw = _raw([["100", "5", "1"], ["100", "7", "2"], ["200", "3", "1"]], ["user_id", "products", "reviews"])
    users, stats = clean_users(raw)

    assert users["user_id"].tolist() == [100, 200]
    assert users.set_index("user_id").loc[100, "products"] == 7
    assert stats["dropped_duplicates"] == 1


def test_fechas_en_los_formatos_sucios_del_origen():
    raw = pd.Series(["2021-03-05", "05/03/2021", "2021/03/05", " 2021-03-05 ", "N/A", "", "no es fecha"])
    parsed = parse_dates(raw, REVIEW_DATE_FORMATS)

    # DD/MM/YYYY: 05/03/2021 es el 5 de marzo, no el 3 de mayo.
    assert (parsed[:4] == pd.Timestamp("2021-03-05")).all()
    assert parsed[4:].isna().all()
    assert str(parsed.dtype).startswith("datetime64")
