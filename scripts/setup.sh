#!/usr/bin/env bash
# Setup inicial del TP en Linux / macOS / WSL con Docker. Se corre una sola vez por máquina:
#   ./scripts/setup.sh
# Es re-ejecutable: lo que ya existe (.env, dataset, usuario de Airflow) no se pisa.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_SLUG="JuanAcciardi/tp-steam-pipeline"
DATASET_SHA256="b4bcce006ada6376c2ab7d64285596f309bf3a47df690cfb3b412ce16855810a"

echo "1/6 Verificando Docker..."
docker info > /dev/null 2>&1 || { echo "Docker no está corriendo."; exit 1; }
# El repo es privado: el dataset se baja con gh (autenticado), no con una URL pública.
gh auth status > /dev/null 2>&1 || { echo "Falta gh autenticado: instalar GitHub CLI y correr 'gh auth login'."; exit 1; }

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
  gh release download dataset-v1 -R "$REPO_SLUG" -p steam-landing.zip -D "$tmp"
  if command -v sha256sum > /dev/null; then
    echo "$DATASET_SHA256  $tmp/steam-landing.zip" | sha256sum -c --quiet
  else
    echo "$DATASET_SHA256  $tmp/steam-landing.zip" | shasum -a 256 -c --quiet
  fi
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
