"""Arma el dataset-v2 a partir del dataset-v1: muestra determinística de usuarios, 36 meses.

    python3 scripts/downsample_v2.py --src landing --out datasets/landing-v2 --zip datasets/steam-landing.zip

- Un usuario queda si sha256("steam-tp-v2:" + user_id) cae en el primer KEEP_FRACTION del rango.
  Es un hash fijo (no usa PII_SALT ni nada aleatorio): correrlo dos veces da los mismos bytes.
- La decisión es por user_id en las tres tablas incrementales, así que cada usuario elegido
  conserva todas sus reviews y su perfil, y no se crean huérfanos nuevos.
- Las filas sin user_id (un problema de calidad del origen) se muestrean con el mismo hash
  sobre la fila completa, para que la proporción de ese problema se mantenga.
- Cada registro se copia byte a byte: la suciedad del origen (formatos, casing, duplicados)
  queda intacta. games y games_metadata se copian enteros.
- Imprime los indicadores de calidad antes/después, valida los invariantes y arma un zip
  reproducible (orden y fechas fijas).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import sys
import zipfile
from collections import Counter
from pathlib import Path

SEED = "steam-tp-v2"
KEEP_FRACTION = 0.63
V1_ZIP_SHA256 = "b4bcce006ada6376c2ab7d64285596f309bf3a47df690cfb3b412ce16855810a"
INCREMENTAL = ("recommendations", "users", "user_profiles")
SNAPSHOTS = {"games": "games.csv", "games_metadata": "games_metadata.json"}
ZIP_DATE = (2026, 1, 1, 0, 0, 0)

_BUCKETS = 10_000
_DATE_FORMATS = {
    "YYYY-MM-DD": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    "DD/MM/YYYY": re.compile(r"^\d{2}/\d{2}/\d{4}$"),
    "YYYY/MM/DD": re.compile(r"^\d{4}/\d{2}/\d{2}$"),
}


def keep(key: str) -> bool:
    digest = hashlib.sha256(f"{SEED}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % _BUCKETS < round(KEEP_FRACTION * _BUCKETS)


def _norm(name: str) -> str:
    return re.sub(r"[\s\-]+", "_", name.strip().lower())


def records(path: Path):
    """(header, [(registro_en_bytes, campos)]) respetando campos entre comillas con saltos de línea."""
    with path.open("rb") as fh:
        header_raw = fh.readline()
        header = [_norm(c) for c in next(csv.reader([header_raw.decode("utf-8", "replace")]))]
        rows, buf = [], b""
        for line in fh:
            buf += line
            if buf.count(b'"') % 2:
                continue
            rows.append((buf, next(csv.reader([buf.decode("utf-8", "replace")]), [])))
            buf = b""
        if buf:
            rows.append((buf, next(csv.reader([buf.decode("utf-8", "replace")]), [])))
    return header_raw, header, rows


def field(header: list[str], fields: list[str], name: str) -> str:
    i = header.index(name)
    return fields[i] if i < len(fields) else ""


def row_key(header: list[str], raw: bytes, fields: list[str]) -> str:
    user_id = field(header, fields, "user_id").strip()
    return user_id if user_id else "row:" + raw.decode("utf-8", "replace")


def build(src: Path, out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    for table in INCREMENTAL:
        (out / table).mkdir(parents=True)
        for path in sorted((src / table).glob("*.csv")):
            header_raw, header, rows = records(path)
            with (out / table / path.name).open("wb") as fh:
                fh.write(header_raw)
                for raw, fields in rows:
                    if keep(row_key(header, raw, fields)):
                        fh.write(raw)
    for table, name in SNAPSHOTS.items():
        (out / table).mkdir(parents=True)
        shutil.copyfile(src / table / name, out / table / name)


def table_sizes(root: Path) -> dict[str, int]:
    return {t: sum(p.stat().st_size for p in (root / t).iterdir()) for t in (*INCREMENTAL, *SNAPSHOTS)}


def _pct(part: int, total: int) -> float:
    return round(100 * part / total, 3) if total else 0.0


def quality(root: Path) -> dict:
    """Indicadores de calidad (en % de filas) y de integridad referencial de un dataset."""
    games_header, games_cols, games_rows = records(root / "games" / "games.csv")
    app_ids = {field(games_cols, f, "app_id").strip() for _, f in games_rows}
    q: dict = {}
    ids: dict[str, set] = {t: set() for t in INCREMENTAL}

    c, n = Counter(), 0
    for path in sorted((root / "recommendations").glob("*.csv")):
        _, h, rows = records(path)
        n += len(rows)
        seen_raw, seen_review = set(), set()
        for raw, f in rows:
            review_id, app_id, user_id = (field(h, f, k).strip() for k in ("review_id", "app_id", "user_id"))
            date, rec, hours = (field(h, f, k).strip() for k in ("date", "is_recommended", "hours"))
            if raw in seen_raw:
                c["duplicado_exacto"] += 1
            elif review_id and review_id in seen_review:
                c["review_id_repetido_(editada)"] += 1
            seen_raw.add(raw)
            seen_review.add(review_id)
            c["review_id_vacio"] += not review_id
            c["app_id_vacio"] += not app_id
            c["user_id_vacio"] += not user_id
            c["app_id_inexistente"] += bool(app_id) and app_id not in app_ids
            fmt = next((k for k, rx in _DATE_FORMATS.items() if rx.match(date)), "otro/vacio")
            c[f"fecha_{fmt}"] += 1
            c[f"is_recommended_{rec or 'vacio'}"] += 1
            c["hours_con_coma"] += "," in hours
            c["hours_vacia"] += not hours
            c["hours_negativa"] += hours.startswith("-")
            c["helpful_vacio"] += not field(h, f, "helpful").strip()
            c["funny_vacio"] += not field(h, f, "funny").strip()
            if user_id:
                ids["recommendations"].add(user_id)
    q["recommendations"] = {"filas": n, **{k: _pct(v, n) for k, v in sorted(c.items())}}

    c, n = Counter(), 0
    for path in sorted((root / "users").glob("*.csv")):
        _, h, rows = records(path)
        n += len(rows)
        seen = set()
        for raw, f in rows:
            user_id = field(h, f, "user_id").strip()
            c["duplicado_exacto"] += raw in seen
            seen.add(raw)
            c["user_id_vacio"] += not user_id
            c["reviews_vacio"] += not field(h, f, "reviews").strip()
            c["products_vacio"] += not field(h, f, "products").strip()
            if user_id:
                ids["users"].add(user_id)
    q["users"] = {"filas": n, **{k: _pct(v, n) for k, v in sorted(c.items())}}

    c, n = Counter(), 0
    for path in sorted((root / "user_profiles").glob("*.csv")):
        _, h, rows = records(path)
        n += len(rows)
        seen_raw, seen_user = set(), set()
        for raw, f in rows:
            user_id, email, country = (field(h, f, k) for k in ("user_id", "email", "country_code"))
            if raw in seen_raw:
                c["duplicado_exacto"] += 1
            elif user_id.strip() in seen_user:
                c["user_id_repetido"] += 1
            seen_raw.add(raw)
            seen_user.add(user_id.strip())
            c["email_vacio"] += not email.strip()
            c["email_mayusculas_o_espacios"] += bool(email.strip()) and (email != email.strip().lower())
            c["country_minuscula"] += bool(country.strip()) and country.strip() != country.strip().upper()
            c["country_vacio"] += not country.strip()
            if user_id.strip():
                ids["user_profiles"].add(user_id.strip())
    q["user_profiles"] = {"filas": n, **{k: _pct(v, n) for k, v in sorted(c.items())}}

    q["integridad"] = {
        "usuarios_en_reviews_sin_fila_en_users": len(ids["recommendations"] - ids["users"]),
        "usuarios_con_perfil_sin_fila_en_users": len(ids["user_profiles"] - ids["users"]),
        "usuarios_sin_perfil": len(ids["users"] - ids["user_profiles"]),
    }
    q["_ids"] = ids
    return q


def check_invariants(before: dict, after: dict) -> list[str]:
    """Ningún huérfano nuevo, y cada usuario elegido conserva todas sus reviews y su perfil."""
    errors = []
    b, a = before["_ids"], after["_ids"]
    for table in ("recommendations", "user_profiles"):
        new_orphans = (a[table] - a["users"]) - (b[table] - b["users"])
        if new_orphans:
            errors.append(f"{table}: {len(new_orphans)} usuarios huérfanos nuevos")
    for table in INCREMENTAL:
        expected = {u for u in b[table] if keep(u)}
        if a[table] != expected:
            errors.append(f"{table}: los user_id no coinciden con la selección por hash")
    return errors


def write_manifest(src: Path, out: Path, after: dict, sizes: dict[str, int]) -> dict:
    v1 = json.loads((src / "_manifest.json").read_text(encoding="utf-8"))
    rows = {t: after[t]["filas"] for t in INCREMENTAL}
    for table, name in SNAPSHOTS.items():
        _, _, snapshot_rows = records(out / table / name) if name.endswith(".csv") else (None, None, None)
        rows[table] = len(snapshot_rows) if snapshot_rows is not None else sum(
            1 for line in (out / table / name).open("rb") if line.strip()
        )
    manifest = {
        "dataset": "dataset-v2",
        "derived_from": {"release": "dataset-v1", "zip_sha256": V1_ZIP_SHA256, "manifest": v1},
        "sampling": {
            "method": f"sha256('{SEED}:' + user_id), primeros 8 bytes mod {_BUCKETS} < {round(KEEP_FRACTION * _BUCKETS)}",
            "user_keep_fraction": KEEP_FRACTION,
        },
        "sample_pct": round(v1["sample_pct"] * KEEP_FRACTION, 2),
        "first_month": v1["first_month"],
        "last_month": v1["last_month"],
        "months": v1["months"],
        "rows": rows,
        "size_mb": {t: round(s / 1e6, 1) for t, s in sizes.items()},
        "size_mib": {t: round(s / 2**20, 1) for t, s in sizes.items()},
        "total_mb": round(sum(sizes.values()) / 1e6, 1),
        "total_mib": round(sum(sizes.values()) / 2**20, 1),
    }
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest


def make_zip(root: Path, zip_path: Path) -> str:
    """Zip reproducible: entradas ordenadas, fecha y permisos fijos."""
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as zf:
        for path in sorted(p for p in root.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(root).as_posix(), date_time=ZIP_DATE)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, path.read_bytes(), compresslevel=9)
    return hashlib.sha256(zip_path.read_bytes()).hexdigest()


def print_comparison(before: dict, after: dict, sizes_before: dict, sizes_after: dict) -> None:
    print(f"\n{'tabla':16} {'MB antes':>9} {'MB después':>11} {'MiB después':>12}")
    for t in sizes_before:
        print(f"{t:16} {sizes_before[t] / 1e6:9.1f} {sizes_after[t] / 1e6:11.1f} {sizes_after[t] / 2**20:12.1f}")
    tb, ta = sum(sizes_before.values()), sum(sizes_after.values())
    print(f"{'TOTAL':16} {tb / 1e6:9.1f} {ta / 1e6:11.1f} {ta / 2**20:12.1f}")
    for section in (*INCREMENTAL, "integridad"):
        print(f"\n[{section}]  {'antes':>12} {'después':>12}")
        for key in sorted(set(before[section]) | set(after[section])):
            print(f"  {key:40} {before[section].get(key, 0):>12} {after[section].get(key, 0):>12}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", type=Path, required=True, help="landing/ con el dataset-v1 descomprimido")
    parser.add_argument("--out", type=Path, required=True, help="carpeta donde se arma el dataset-v2")
    parser.add_argument("--zip", type=Path, required=True, help="ruta del steam-landing.zip a generar")
    parser.add_argument("--max-mb", type=float, default=280.0)
    args = parser.parse_args()

    if json.loads((args.src / "_manifest.json").read_text(encoding="utf-8")).get("dataset"):
        sys.exit(f"{args.src} no es el dataset-v1: el v2 se deriva siempre del v1.")

    build(args.src, args.out)
    sizes_before, sizes_after = table_sizes(args.src), table_sizes(args.out)
    before, after = quality(args.src), quality(args.out)
    print_comparison(before, after, sizes_before, sizes_after)

    errors = check_invariants(before, after)
    total_mb = sum(sizes_after.values()) / 1e6
    if total_mb > args.max_mb:
        errors.append(f"el dataset-v2 pesa {total_mb:.1f} MB (> {args.max_mb} MB)")
    if errors:
        sys.exit("Invariantes rotos:\n" + "\n".join(errors))

    manifest = write_manifest(args.src, args.out, after, sizes_after)
    sha = make_zip(args.out, args.zip)
    print(f"\nInvariantes OK. Usuarios conservados: {KEEP_FRACTION:.0%} (sample_pct {manifest['sample_pct']}%).")
    print(f"{args.zip}: {args.zip.stat().st_size / 1e6:.1f} MB, sha256 {sha}")


if __name__ == "__main__":
    main()
