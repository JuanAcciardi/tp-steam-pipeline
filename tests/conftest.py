"""steam_lib vive en dags/: en el contenedor lo resuelve PYTHONPATH, fuera de él este conftest."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
