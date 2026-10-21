"""Simula el sistema origen a partir del dataset de Kaggle (Steam Game Recommendations).

Hace cuatro cosas, todas determinísticas (misma semilla -> mismos archivos byte a byte):

1. Muestrea recommendations.csv (~2 GB) a un volumen dentro del rango de la consigna
   (50-300 MB): usuarios con user_id % --user-modulo == 0 y reviews desde --min-date.
2. Genera la tabla sintética de PII user_profiles (nombre, username, email, teléfono).
3. Inyecta imperfecciones documentadas (ver README, "Imperfecciones simuladas"), porque
   el dataset publicado ya viene limpio.
4. Escribe la zona de landing que consume el DAG:

    landing/
      recommendations/recommendations_YYYY_MM.csv   incremental por mes de negocio
      users/users_YYYY_MM.csv                       altas del mes (cohorte = 1ª review)
      user_profiles/user_profiles_YYYY_MM.csv       PII sintética, misma cohorte
      games/games.csv                               snapshot del catálogo
      games_metadata/games_metadata.json            snapshot (JSON lines)
      _manifest.json

Uso (dentro de WSL):
    python scripts/prepare_source.py --raw-dir ~/datasets/steam --landing-dir ~/airflow/landing
"""

from __future__ import annotations

import argparse
import json
import shutil
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

RAW_FILES = ("recommendations.csv", "users.csv", "games.csv", "games_metadata.json")
FAKER_LOCALES = ("es_AR", "en_US", "pt_BR", "de_DE", "en_GB")
COUNTRIES = {  # código -> (peso, prefijo telefónico)
    "US": (0.30, "1"), "BR": (0.15, "55"), "AR": (0.10, "54"), "DE": (0.10, "49"),
    "GB": (0.10, "44"), "RU": (0.10, "7"), "CN": (0.15, "86"),
}
EMAIL_DOMAINS = ("gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "proton.me")
ORPHAN_APP_ID_BASE = 9_000_000


# --------------------------------------------------------------------------- muestreo

def sample_recommendations(raw_dir: Path, modulo: int, min_date: str, max_date: str | None) -> pd.DataFrame:
    parts = []
    reader = pd.read_csv(raw_dir / "recommendations.csv", chunksize=2_000_000, dtype={"date": "string"})
    for i, chunk in enumerate(reader, start=1):
        mask = (chunk["user_id"] % modulo == 0) & (chunk["date"] >= min_date)
        if max_date:
            mask &= chunk["date"] <= max_date
        parts.append(chunk[mask])
        print(f"  chunk {i}: {int(mask.sum()):,} filas seleccionadas")
    df = pd.concat(parts, ignore_index=True)
    return df.sort_values("review_id").reset_index(drop=True)


def sample_users(raw_dir: Path, user_ids: pd.Index) -> pd.DataFrame:
    parts = [
        chunk[chunk["user_id"].isin(user_ids)]
        for chunk in pd.read_csv(raw_dir / "users.csv", chunksize=2_000_000)
    ]
    return pd.concat(parts, ignore_index=True).sort_values("user_id").reset_index(drop=True)


def read_metadata(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    if text.lstrip().startswith("["):
        return json.loads(text)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


# --------------------------------------------------------------------------- PII sintética

def _ascii(value: str) -> str:
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return "".join(ch for ch in value if ch.isalpha())


def _name_pool(seed: int, attr: str, per_locale: int = 600) -> np.ndarray:
    names = set()
    for i, locale in enumerate(FAKER_LOCALES):
        fake = Faker(locale)
        fake.seed_instance(seed + i)
        names.update(n for n in (_ascii(getattr(fake, attr)()) for _ in range(per_locale)) if n)
    return np.array(sorted(names))


def build_profiles(user_ids: pd.Series, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = len(user_ids)
    first_pool = _name_pool(seed, "first_name")
    first = pd.Series(first_pool[rng.integers(0, len(first_pool), n)])
    last_pool = _name_pool(seed + 100, "last_name")
    last = pd.Series(last_pool[rng.integers(0, len(last_pool), n)])
    codes = list(COUNTRIES)
    weights = np.array([COUNTRIES[c][0] for c in codes])
    country = pd.Series(np.array(codes)[rng.choice(len(codes), size=n, p=weights / weights.sum())])
    prefix = country.map({c: v[1] for c, v in COUNTRIES.items()})
    digits = pd.Series(rng.integers(10**9, 10**10, n)).astype(str)
    domain = pd.Series(np.array(EMAIL_DOMAINS)[rng.integers(0, len(EMAIL_DOMAINS), n)])
    uid = pd.Series(user_ids.to_numpy()).astype(str)

    return pd.DataFrame({
        "user_id": user_ids.to_numpy(),
        "username": first.str.lower() + "_" + last.str.lower() + uid.str[-4:],
        "full_name": first + " " + last,
        "email": first.str.lower() + "." + last.str.lower() + "." + uid + "@" + domain,
        "phone_number": "+" + prefix + " " + digits,
        "country_code": country,
    })


# --------------------------------------------------------------------------- imperfecciones

def _pick(rng: np.random.Generator, n: int, p: float) -> np.ndarray:
    return rng.random(n) < p


def dirty_recommendations(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.copy()
    business_date = pd.to_datetime(df["date"])
    df["_month"] = business_date.dt.strftime("%Y_%m")
    updated = business_date + pd.to_timedelta(rng.integers(0, 86_400, len(df)), unit="s")

    # 2% de reviews editadas: misma review_id, updated_at posterior, más horas y votos.
    edit_idx = np.sort(rng.choice(len(df), size=int(len(df) * 0.02), replace=False))
    edits = df.iloc[edit_idx].copy()
    edits_updated = updated.iloc[edit_idx] + pd.to_timedelta(rng.integers(1, 31, len(edits)), unit="D")
    edits["hours"] = (edits["hours"] + rng.uniform(0, 50, len(edits))).round(1)
    edits["helpful"] = edits["helpful"] + rng.integers(0, 5, len(edits))
    # 0,5% de duplicados exactos (reenvío del mismo registro por el origen).
    exact_idx = np.sort(rng.choice(len(df), size=int(len(df) * 0.005), replace=False))
    exact = df.iloc[exact_idx].copy()

    df = pd.concat([df, edits, exact], ignore_index=True)
    updated = pd.concat([updated, edits_updated, updated.iloc[exact_idx]], ignore_index=True)
    df = df.astype({c: "string" for c in df.columns if c != "_month"})
    df["updated_at"] = updated.dt.strftime("%Y-%m-%d %H:%M:%S")
    n = len(df)

    dates = pd.to_datetime(df["date"])
    m = _pick(rng, n, 0.05)
    df.loc[m, "date"] = dates[m].dt.strftime("%d/%m/%Y")
    m = _pick(rng, n, 0.03) & ~m
    df.loc[m, "date"] = dates[m].dt.strftime("%Y/%m/%d")

    rec = df["is_recommended"].str.lower() == "true"
    df["is_recommended"] = np.where(rec, "true", "false")
    for p, yes, no in ((0.10, "True", "False"), (0.05, "TRUE", "FALSE"), (0.03, "1", "0")):
        m = _pick(rng, n, p)
        df.loc[m, "is_recommended"] = np.where(rec[m], yes, no)

    m = _pick(rng, n, 0.03)
    df.loc[m, "hours"] = df.loc[m, "hours"].str.replace(".", ",", regex=False)
    df.loc[_pick(rng, n, 0.01), "hours"] = ""
    df.loc[_pick(rng, n, 0.001), "hours"] = "-1"
    df.loc[_pick(rng, n, 0.01), "helpful"] = ""
    df.loc[_pick(rng, n, 0.01), "funny"] = ""
    df.loc[_pick(rng, n, 0.001), "review_id"] = ""
    df.loc[_pick(rng, n, 0.001), "is_recommended"] = ""
    m = _pick(rng, n, 0.002)
    df.loc[m, "app_id"] = (ORPHAN_APP_ID_BASE + rng.integers(0, 1000, int(m.sum()))).astype(str)

    # Orden de archivo aleatorio: la deduplicación no puede depender de "la última fila".
    return df.iloc[rng.permutation(n)].reset_index(drop=True)


def dirty_games(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.astype("string").fillna("")
    n = len(df)
    m = _pick(rng, n, 0.02)
    df.loc[m, "title"] = "  " + df.loc[m, "title"] + " "
    m = _pick(rng, n, 0.10)
    df.loc[m, "rating"] = df.loc[m, "rating"].str.lower()
    m = _pick(rng, n, 0.05)
    df.loc[m, "rating"] = df.loc[m, "rating"].str.upper()
    df.loc[_pick(rng, n, 0.01), "rating"] = ""
    m = _pick(rng, n, 0.05)
    df.loc[m, "price_final"] = "$" + df.loc[m, "price_final"]
    release = pd.to_datetime(df["date_release"], errors="coerce")
    m = _pick(rng, n, 0.05) & release.notna().to_numpy()
    df.loc[m, "date_release"] = release[m].dt.strftime("%d/%m/%Y")
    m = _pick(rng, n, 0.03) & release.notna().to_numpy() & ~m
    df.loc[m, "date_release"] = release[m].dt.strftime("%b %d, %Y")
    for col in ("win", "mac", "linux", "steam_deck"):
        m = _pick(rng, n, 0.05)
        df.loc[m, col] = df.loc[m, col].str.upper()

    # 1% de versiones viejas del juego (precio sin descuento) ANTES de la vigente.
    old = df.iloc[np.sort(rng.choice(n, size=int(n * 0.01), replace=False))].copy()
    old["price_final"] = old["price_original"]
    old["discount"] = "0.0"
    return pd.concat([old, df], ignore_index=True)


def dirty_metadata(records: list[dict], rng: np.random.Generator) -> list[dict]:
    out = []
    old_versions = set(rng.choice(len(records), size=int(len(records) * 0.005), replace=False).tolist())
    for i, rec in enumerate(records):
        rec = dict(rec)
        if i in old_versions:
            out.append({**rec, "description": "", "tags": []})
        if rng.random() < 0.02:
            rec["description"] = "   "
        if rng.random() < 0.05:
            rec["tags"] = [t.upper() for t in rec.get("tags", [])]
        out.append(rec)
    return out


def dirty_users(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.astype({c: "string" for c in ("user_id", "products", "reviews")})
    n = len(df)
    df.loc[_pick(rng, n, 0.01), "reviews"] = ""
    df.loc[_pick(rng, n, 0.001), "user_id"] = ""
    dup = df.iloc[np.sort(rng.choice(n, size=int(n * 0.01), replace=False))]
    return pd.concat([df, dup], ignore_index=True)


def dirty_profiles(df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    df = df.astype({"user_id": "string"})
    n = len(df)
    m = _pick(rng, n, 0.10)
    df.loc[m, "email"] = " " + df.loc[m, "email"].str.title() + " "
    df.loc[_pick(rng, n, 0.01), "email"] = ""
    m = _pick(rng, n, 0.03)
    df.loc[m, "country_code"] = df.loc[m, "country_code"].str.lower()
    dup = df.iloc[np.sort(rng.choice(n, size=int(n * 0.005), replace=False))]
    return pd.concat([df, dup], ignore_index=True)


# --------------------------------------------------------------------------- escritura

def write_monthly(df: pd.DataFrame, months: list[str], folder: Path, prefix: str, columns: list[str]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    grouped = dict(tuple(df.groupby("_month", sort=True)))
    for month in months:
        part = grouped.get(month, df.iloc[0:0])
        part[columns].to_csv(folder / f"{prefix}_{month}.csv", index=False, lineterminator="\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw-dir", type=Path, required=True, help="Carpeta con los CSV/JSON de Kaggle")
    parser.add_argument("--landing-dir", type=Path, default=Path("~/airflow/landing"))
    parser.add_argument("--user-modulo", type=int, default=20, help="Se queda con user_id %% N == 0")
    parser.add_argument("--min-date", default="2020-01-01")
    parser.add_argument("--max-date", default=None)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    raw_dir = args.raw_dir.expanduser()
    landing = args.landing_dir.expanduser()
    missing = [f for f in RAW_FILES if not (raw_dir / f).exists()]
    if missing:
        raise SystemExit(f"Faltan archivos en {raw_dir}: {missing}")

    rng = np.random.default_rng(args.seed)
    if landing.exists():
        shutil.rmtree(landing)

    print("Muestreando recommendations...")
    recs = sample_recommendations(raw_dir, args.user_modulo, args.min_date, args.max_date)
    cohort = (
        pd.to_datetime(recs["date"]).dt.strftime("%Y_%m").groupby(recs["user_id"]).min().rename("_month")
    )
    months = sorted(pd.to_datetime(recs["date"]).dt.strftime("%Y_%m").unique())
    print(f"  {len(recs):,} reviews, {len(cohort):,} usuarios, meses {months[0]} -> {months[-1]}")

    print("Muestreando users y generando PII sintética...")
    users = sample_users(raw_dir, cohort.index).merge(cohort, left_on="user_id", right_index=True)
    profiles = build_profiles(cohort.index.to_series(), args.seed).merge(
        cohort, left_on="user_id", right_index=True
    )

    print("Inyectando imperfecciones y escribiendo landing...")
    rec_cols = ["app_id", "helpful", "funny", "date", "is_recommended", "hours", "user_id", "review_id", "updated_at"]
    write_monthly(dirty_recommendations(recs, rng), months, landing / "recommendations", "recommendations", rec_cols)
    write_monthly(dirty_users(users, rng), months, landing / "users", "users", ["user_id", "products", "reviews"])
    write_monthly(
        dirty_profiles(profiles, rng), months, landing / "user_profiles", "user_profiles",
        ["user_id", "username", "full_name", "email", "phone_number", "country_code"],
    )

    (landing / "games").mkdir(parents=True)
    games = pd.read_csv(raw_dir / "games.csv", dtype="string", keep_default_na=False)
    dirty_games(games, rng).to_csv(landing / "games" / "games.csv", index=False, lineterminator="\n")

    (landing / "games_metadata").mkdir(parents=True)
    with (landing / "games_metadata" / "games_metadata.json").open("w", encoding="utf-8", newline="\n") as fh:
        for rec in dirty_metadata(read_metadata(raw_dir / "games_metadata.json"), rng):
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    sizes = {
        table.name: sum(f.stat().st_size for f in table.rglob("*") if f.is_file())
        for table in landing.iterdir() if table.is_dir()
    }
    total_mb = sum(sizes.values()) / 1024**2
    manifest = {
        "seed": args.seed,
        "user_modulo": args.user_modulo,
        "min_date": args.min_date,
        "max_date": args.max_date,
        "first_month": months[0].replace("_", "-"),
        "last_month": months[-1].replace("_", "-"),
        "months": len(months),
        "rows": {"recommendations": len(recs), "users": len(users), "user_profiles": len(profiles), "games": len(games)},
        "size_mb": {k: round(v / 1024**2, 1) for k, v in sizes.items()},
        "total_mb": round(total_mb, 1),
    }
    (landing / "_manifest.json").write_text(json.dumps(manifest, indent=2))

    print(json.dumps(manifest, indent=2))
    if not 50 <= total_mb <= 300:
        print(f"\nATENCIÓN: {total_mb:.0f} MB está fuera del rango 50-300 MB. Ajustar --user-modulo o --min-date.")
    print(
        f"\nActualizar dags/steam_lib/config.py:\n"
        f'    FIRST_MONTH = "{manifest["first_month"]}"\n    LAST_MONTH = "{manifest["last_month"]}"'
    )


if __name__ == "__main__":
    main()
