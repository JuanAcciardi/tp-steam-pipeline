#!/usr/bin/env bash
# Setup inicial del TP en Linux / macOS / WSL con Docker. Se corre una sola vez por máquina:
#   ./scripts/setup.sh
# Es re-ejecutable: lo que ya existe (.env, dataset, usuario de Airflow) no se pisa.
set -euo pipefail

cd "$(dirname "$0")/.."
DATASET_URL="https://github.com/JuanAcciardi/tp-steam-pipeline/releases/download/dataset-v1/steam-landing.zip"

echo "1/6 Verificando Docker..."
docker info > /dev/null 2>&1 || { echo "Docker no está corriendo."; exit 1; }

echo "2/6 Generando .env (UID + Fernet key para cifrar las Variables)..."
if [[ ! -f .env ]]; then
  fernet=$(head -c 32 /dev/urandom | base64 | tr '+/' '-_')
  printf "AIRFLOW_UID=%s\nAIRFLOW_FERNET_KEY=%s\n" "$(id -u)" "$fernet" > .env
else
  echo "    .env ya existe, se conserva."
fi

echo "3/6 Descargando el dataset de origen (release dataset-v1)..."
mkdir -p data landing logs
if [[ ! -f landing/_manifest.json ]]; then
  tmp=$(mktemp -d)
  curl -fL "$DATASET_URL" -o "$tmp/steam-landing.zip"
  if command -v unzip > /dev/null; then
    unzip -q "$tmp/steam-landing.zip" -d landing
  else
    python3 -m zipfile -e "$tmp/steam-landing.zip" landing
  fi
  rm -rf "$tmp"
else
  echo "    landing/ ya tiene el dataset, se conserva."
fi

echo "4/6 Construyendo la imagen e inicializando Airflow..."
docker compose build
docker compose up airflow-init

echo "5/6 Guardando PII_SALT como Airflow Variable (no queda en ningún archivo)..."
read -rsp "    PII_SALT (es el mismo para todo el equipo): " salt; echo
[[ -n "$salt" ]] || { echo "PII_SALT vacío"; exit 1; }
docker compose run --rm airflow-cli airflow variables set PII_SALT "$salt" > /dev/null

echo "6/6 Levantando Airflow..."
docker compose up -d
echo
echo "Listo: http://localhost:8080  (usuario airflow / clave airflow)"
echo "Activá el DAG steam_medallion_pipeline para correr el backfill 2020-01 -> 2022-12."
