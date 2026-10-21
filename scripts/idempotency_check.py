"""Evidencia del test de idempotencia: conteos y checksums de Silver/Gold para un mes.

Flujo para la entrega / defensa:
    python scripts/idempotency_check.py snapshot --month 2021-03 --out docs/idempotency_before.json
    # en la UI: Clear del run 2021-03 (o trigger con params {"month": "2021-03"})
    python scripts/idempotency_check.py compare --month 2021-03 --baseline docs/idempotency_before.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "dags"))

from steam_lib import config, gold, silver  # noqa: E402
from steam_lib.io_utils import window_for_month  # noqa: E402


def describe(path: Path) -> dict:
    files = sorted(path.glob("*.parquet")) if path.is_dir() else [path]
    return {
        "path": str(path),
        "files": [f.name for f in files],
        "rows": sum(pq.read_metadata(f).num_rows for f in files),
        "sha256": [hashlib.sha256(f.read_bytes()).hexdigest()[:16] for f in files],
    }


def snapshot(month: str) -> dict:
    window = window_for_month(month)
    return {
        "month": month,
        "silver_recommendations": describe(silver.silver_part("recommendations", window)),
        "silver_quarantine": describe(silver.quarantine_part("recommendations", window)),
        "silver_users": describe(silver.silver_part("users", window)),
        "silver_user_profiles": describe(silver.silver_part("user_profiles", window)),
        "gold_partition": describe(gold.gold_partition_dir(window)),
        "gold_total_files": len(list((config.GOLD_ROOT / config.GOLD_DATASET).rglob("*.parquet"))),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["snapshot", "compare"])
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()

    current = snapshot(args.month)
    if args.mode == "snapshot":
        text = json.dumps(current, indent=2)
        print(text)
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text)
        return

    before = json.loads(args.baseline.read_text())
    print(f"{'capa':<24}{'antes':>12}{'después':>12}  idéntico")
    ok = True
    for key, value in current.items():
        if not isinstance(value, dict):
            same = before[key] == value
            print(f"{key:<24}{before[key]:>12}{value:>12}  {'sí' if same else 'NO'}")
        else:
            same = before[key]["rows"] == value["rows"] and before[key]["files"] == value["files"]
            print(f"{key:<24}{before[key]['rows']:>12,}{value['rows']:>12,}  {'sí' if same else 'NO'}")
        ok &= same
    print("\nIDEMPOTENTE" if ok else "\nNO IDEMPOTENTE: revisar estrategia de escritura")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
