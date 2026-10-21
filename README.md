# Steam Medallion Pipeline — TP Data Engineering 2026

Pipeline end-to-end local orquestado con **Apache Airflow 3.0.4**. Toma el dataset
[Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam),
lo procesa en una arquitectura medallion (Bronze → Silver → Gold) en el filesystem local y lo
expone a **DuckDB** con particionado Hive para validar el partition pruning.

## Estructura

```
dags/
  steam_pipeline.py        DAG (solo orquestación)
  steam_lib/               lógica testeable sin Airflow: bronze, silver, pii, gold, catalog
scripts/
  prepare_source.py        muestreo + PII sintética + imperfecciones -> ~/airflow/landing
  finops_validation.py     EXPLAIN ANALYZE full scan vs. pruned scan
  idempotency_check.py     evidencia del test de idempotencia
sql/                       consultas de validación y de ejemplo
docs/                      arquitectura, workflow del equipo, preparación de la defensa
tests/                     tests unitarios de las transformaciones
```

## Setup (cada integrante, dentro de WSL Ubuntu)

```bash
# 1. Airflow (pasos 1-5 de la consigna) y el repo DENTRO de WSL (no en /mnt/c)
python3.12 -m venv ~/airflow-env && source ~/airflow-env/bin/activate
git clone https://github.com/<org>/tp-steam-pipeline.git ~/tp-steam-pipeline
cd ~/tp-steam-pipeline
pip install -r requirements.txt --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.4/constraints-3.12.txt"
airflow db migrate

# 2. Airflow lee los DAGs directamente del repo
mkdir -p ~/airflow && ln -sfn ~/tp-steam-pipeline/dags ~/airflow/dags

# 3. Salt de PII (el valor real se comparte por fuera del repo, es el mismo para todo el equipo)
airflow variables set PII_SALT '<valor-secreto>'

# 4. Dataset de Kaggle descomprimido en ~/datasets/steam y generación del origen simulado
python scripts/prepare_source.py --raw-dir ~/datasets/steam --landing-dir ~/airflow/landing
#    -> copiar FIRST_MONTH / LAST_MONTH que imprime al final en dags/steam_lib/config.py

# 5. Levantar Airflow y activar el DAG steam_medallion_pipeline (hace el backfill mensual)
airflow standalone
```

## Diseño

| Tema | Decisión |
|---|---|
| Fecha de negocio | `recommendations.date` (día de la review). Particiona Gold. |
| Fecha de ingesta | El origen entrega la extracción del mes M el 1° de M+1 (= `data_interval_end`). Particiona Bronze. Ej.: reviews de 2021-03 → `bronze/recommendations/ingested_year=2021/ingested_month=04/`. |
| Granularidad | Un DAG run = un mes de negocio (`CronDataIntervalTimetable` mensual, `catchup=True`, `max_active_runs=1`). El param `month` (YYYY-MM) permite forzar un mes en un trigger manual. |
| Tablas incrementales | `recommendations`, `users`, `user_profiles` (alta de usuario = mes de su primera review). |
| Tablas snapshot | `games`, `games_metadata` (catálogo completo en cada extracción). |
| Gold | `gold/user_game_reviews/year=YYYY/month=MM/`. Grano: 1 fila por review. Joins N:1 de 5 entidades Silver: recommendations ⨝ games ⨝ games_metadata ⨝ users ⨝ user_profiles. |

### DAG

```
land_raw_files >> validate_bronze >> [clean_catalog_silver, clean_users_silver, mask_pii]
clean_catalog_silver >> clean_recommendations_silver
[clean_recommendations_silver, clean_users_silver, mask_pii] >> build_gold_partition
build_gold_partition >> update_duckdb_catalog >> audit_counts
```

### Idempotencia

- Todas las rutas se derivan de la ventana del run, nunca de `now()`.
- **Bronze y Gold:** la carpeta de la partición se arma en `data/_tmp/` y reemplaza completa a la anterior (`rmtree` + `os.replace`). Nunca se hace append.
- **Silver:** un archivo por mes (`part-YYYY-MM.parquet`) o por snapshot, reemplazado de forma atómica.
- `audit_counts` falla si Gold ≠ Silver (no hay fan-out en los joins) y deja `data/_audit/YYYY-MM.json`.
- La evidencia se genera con `scripts/idempotency_check.py` (snapshot → re-run → compare).

### PII

- `email` y `username` → `HMAC-SHA256(PII_SALT, valor normalizado)`. Se normaliza (trim + lower) antes de hashear para que el mismo email siempre dé el mismo hash y los analistas puedan joinear.
- `full_name` y `phone_number` se eliminan en Silver porque no tienen uso analítico.
- El salt se lee con `Variable.get("PII_SALT")` **dentro** del task `mask_pii`, nunca a nivel de módulo, y no aparece en ningún archivo del repo.

## Imperfecciones simuladas

El dataset publicado ya viene limpio. Para poder demostrar la higiene de datos,
`scripts/prepare_source.py` agrega imperfecciones con semilla fija, así que son reproducibles:

| Tabla | Imperfección | % aprox. |
|---|---|---|
| recommendations | reviews editadas (mismo `review_id`, `updated_at` posterior) | 2% |
| recommendations | duplicados exactos | 0,5% |
| recommendations | `date` en `DD/MM/YYYY` o `YYYY/MM/DD` | 8% |
| recommendations | `is_recommended` como `True`/`TRUE`/`1` | 18% |
| recommendations | `hours` con coma decimal / vacío / negativo | 3% / 1% / 0,1% |
| recommendations | `helpful`, `funny` vacíos | 1% c/u |
| recommendations | `review_id`, `is_recommended` vacíos | 0,1% c/u |
| recommendations | `app_id` inexistente (huérfano) | 0,2% |
| games | espacios en `title`, casing de `rating`, `$` en `price_final`, otros formatos de fecha, booleanos en mayúscula | 2-10% |
| games | versión vieja del juego antes de la vigente | 1% |
| games_metadata | descripción en blanco, tags en mayúscula, versión vieja vacía | 0,5-5% |
| users | `reviews` vacío, `user_id` vacío, duplicados exactos | 1% / 0,1% / 1% |
| user_profiles | email con mayúsculas/espacios, email vacío, país en minúscula, duplicados | 10% / 1% / 3% / 0,5% |

## Reglas de calidad de datos (Silver)

| Tabla.campo | Regla | Justificación |
|---|---|---|
| recommendations.review_id nulo | se descarta la fila | Es la clave de deduplicación y el grano de Gold: sin ella no se puede identificar la review. |
| recommendations.app_id / user_id nulo | se descarta | No se puede joinear con ninguna dimensión. |
| recommendations.date nula o no parseable | se descarta | Sin fecha de negocio no se puede asignar a una partición. |
| recommendations.date fuera del mes del run | se descarta | El run procesa estrictamente su ventana lógica. |
| recommendations.is_recommended nulo | se descarta | Es la métrica principal: imputarla sesgaría el % de recomendación. |
| recommendations.hours nulo o negativo | queda nulo | Un 0 sesgaría los promedios; `AVG` ignora los nulos. |
| recommendations.helpful / funny nulo | se imputa 0 | Que no haya votos registrados equivale a 0 votos. |
| recommendations.updated_at nulo | se imputa con `date` | La versión original de la review es la del día en que se publicó. |
| recommendations duplicadas | por `review_id` gana el `updated_at` más reciente | Una review editada reemplaza a la anterior. Los duplicados exactos colapsan igual. |
| recommendations con app_id inexistente | van a `silver/_quarantine/recommendations/` | Son huérfanos: no se pierden y se pueden auditar, pero no contaminan Gold. |
| games.app_id nulo | se descarta | Es la clave del catálogo. |
| games duplicados | gana la última fila del snapshot | El catálogo es append-only: la última fila es la versión vigente. |
| games.title nulo | `"Unknown Title"` | El juego sigue siendo válido para las métricas. |
| games.rating nulo | `"No Rating"` | Es una categoría explícita, en lugar de un hueco en los group by. |
| games.win/mac/linux/steam_deck nulo | `False` | Si la plataforma no está declarada, no se considera soportada. |
| games.price_* / date_release no parseable | queda nulo | Es un atributo descriptivo y no bloquea los joins. |
| games_metadata.description en blanco | nulo | Un texto vacío no aporta información. |
| users.user_id nulo | se descarta | No se puede joinear. |
| users.products / reviews nulo | queda nulo | Imputar 0 sesgaría los perfiles de actividad. |
| users duplicados | gana la última fila | Son duplicados exactos del origen. |
| user_profiles.email nulo | `email_hash` nulo, la fila se mantiene | El usuario existe aunque no haya dejado su email. |
| user_profiles.country_code nulo | `"ZZ"` (código ISO reservado para "desconocido") | Permite agrupar sin perder filas. |
| Gold: usuario sin perfil | left join, atributos nulos | Que falte el perfil no invalida la review. |

## Validaciones de la entrega

```bash
python scripts/finops_validation.py --out docs/finops_output.txt
python scripts/idempotency_check.py snapshot --month 2021-03 --out docs/idempotency_before.json
# Clear del run 2021-03 en la UI
python scripts/idempotency_check.py compare --month 2021-03 --baseline docs/idempotency_before.json
```

## Workflow del equipo

`main` (protegida, solo PR) ← `develop` (integración) ← `feature/<tema>`. Ver [docs/team_workflow.md](docs/team_workflow.md).
