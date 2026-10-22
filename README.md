# Steam Medallion Pipeline — TP Data Engineering 2026

Pipeline end-to-end local orquestado con **Apache Airflow 3.0.4** (Docker Compose). Procesa
reviews de Steam en una arquitectura medallion (Bronze → Silver → Gold) sobre el filesystem
local y expone Gold a **DuckDB** con particionado Hive para validar el partition pruning.

## Empezá acá (integrantes del equipo)

1. **Si clonaste el repo antes del 22/10/2026:** borrá tu copia y volvé a clonar. El historial
   se reescribió y tu copia vieja no es compatible.
2. Instalá [Docker Desktop](https://www.docker.com/products/docker-desktop/) (le tienen que
   quedar al menos 4 GB de RAM), [git](https://git-scm.com/) y [GitHub CLI](https://cli.github.com/).
   Después corré `gh auth login`.
3. **Tené a mano el `PII_SALT` que se pasó por el grupo de WhatsApp.** El setup te lo va a pedir.
   **Nunca** lo escribas en un archivo del repo, en un commit, en un issue ni en un PR: la consigna
   exige que no aparezca en ningún archivo versionado.
4. Corré el setup (sección siguiente) y activá el DAG.
5. Mirá [Estado del TP](#estado-del-tp) para ver qué falta, y
   [docs/team_workflow.md](docs/team_workflow.md) para saber cómo trabajar con ramas y PRs.

## Estructura

```
dags/
  steam_pipeline.py        DAG (solo orquestación)
  steam_lib/               lógica testeable sin Airflow: bronze, silver, pii, gold, catalog
scripts/
  setup.ps1 / setup.sh     setup inicial de cada integrante (Docker + dataset + PII_SALT)
  finops_validation.py     EXPLAIN ANALYZE full scan vs. pruned scan
  idempotency_check.py     evidencia del test de idempotencia
sql/                       consultas de validación y de ejemplo
docs/                      arquitectura, defensa y workflow del equipo
docker-compose.yaml        Airflow 3.0.4 + Postgres, LocalExecutor
landing/  (no versionado)  origen: extracciones mensuales tal como llegan
data/     (no versionado)  bronze / silver / gold / catalog.duckdb
```

## Setup (una vez por integrante)

Requisitos: Docker Desktop (o Docker Engine), git y [GitHub CLI](https://cli.github.com/) autenticado (`gh auth login`): el repo es privado y el dataset se descarga del release con `gh`.

```powershell
git clone https://github.com/JuanAcciardi/tp-steam-pipeline.git
cd tp-steam-pipeline
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1     # Windows
./scripts/setup.sh                                                # Linux / macOS / WSL
```

El script hace todo, y se puede volver a correr sin pisar lo que ya existe:

1. Genera `.env` con una Fernet key propia, que cifra las Variables en la metadata DB.
2. Descarga el dataset de origen (y verifica su SHA-256) desde el release [`dataset-v1`](https://github.com/JuanAcciardi/tp-steam-pipeline/releases/tag/dataset-v1) (zip de 146 MB) y lo descomprime en `landing/` (399,9 MB).
3. Construye la imagen e inicializa Airflow.
4. Pide el `PII_SALT` (el del grupo de WhatsApp) y lo guarda con `airflow variables set PII_SALT ...`.
   No se escribe en ningún archivo. Tiene que ser el mismo para los 5, así los hashes coinciden.
5. Levanta Airflow en http://localhost:8080 (usuario `airflow`, clave `airflow`).

Día a día: `docker compose up -d` / `docker compose down`.
CLI de Airflow: `docker compose run --rm airflow-cli airflow <comando>`.

### Correr el pipeline

- **Backfill completo:** en la UI, activar (unpause) `steam_medallion_pipeline`. Airflow crea un
  run por mes, de 2020-01 a 2022-12 (36 runs), y los ejecuta en orden, de a uno.
- **Un mes puntual:** *Trigger DAG* con params `{"month": "2021-03"}`, o *Clear* sobre un run existente.
- **Ver el resultado:** las capas quedan en `data/`. Para consultar Gold con DuckDB:
  ```bash
  docker compose exec airflow-scheduler python -c "import duckdb; print(duckdb.connect('data/catalog.duckdb').sql('SELECT year, month, COUNT(*) FROM user_game_reviews GROUP BY ALL ORDER BY ALL'))"
  ```
- **Empezar de cero:** `docker compose down -v` y borrar `data/` (no borren `landing/`, es el dataset).

### Problemas comunes

| Síntoma | Solución |
|---|---|
| El puerto 8080 está ocupado | Hay otro Airflow levantado (por ejemplo, el de otra materia). Bajalo con `docker compose down` en su carpeta. |
| Docker se cae o el disco C: se llena | Docker Desktop → Settings → Resources → Advanced → *Disk image location* → elegí otro disco → *Apply & restart*. |
| `Falta gh autenticado` | `gh auth login`, y verificá que aceptaste la invitación al repo. |
| `Falta AIRFLOW_FERNET_KEY en .env` | Corré el script de setup (genera `.env`). No copies el `.env` de otro integrante. |
| El DAG no aparece o muestra *Import error* | `docker compose run --rm airflow-cli airflow dags list-import-errors` |
| `mask_pii` falla con `PII_SALT vacío` | `docker compose run --rm airflow-cli airflow variables set PII_SALT '<el del WhatsApp>'` |

> La consigna propone `airflow standalone` en WSL. Usamos Docker Compose con la misma versión
> (3.0.4) para que los 5 integrantes tengan un entorno idéntico sin importar el sistema operativo.
> Dentro del contenedor `AIRFLOW_HOME=/opt/airflow`, así que las capas quedan en
> `$AIRFLOW_HOME/data/{bronze,silver,gold}`, la misma estructura que `~/airflow/data/` de la consigna.

## Dataset de origen

Se deriva de [Game Recommendations on Steam](https://www.kaggle.com/datasets/antonkozyriev/game-recommendations-on-steam)
(41,2M reviews, 2010-10 → 2022-12). Es una muestra determinística del **13,4% de los usuarios**
con todas sus reviews de los **últimos 3 años disponibles (2020-01 → 2022-12)**, más una tabla
de identidad sintética (PII) por usuario. Se publica una sola vez en el release `dataset-v1` y es
el punto de partida del TP.

| Tabla (landing) | Entrega | Filas | MB |
|---|---|---|---|
| recommendations | 1 CSV por mes de negocio (36) | 3.467.515 | 234,5 |
| users | 1 CSV por mes (altas: usuarios con su 1ª review en ese mes) | 1.415.203 | 18,3 |
| user_profiles (PII) | 1 CSV por mes, misma cohorte que users | 1.415.203 | 125,1 |
| games | snapshot completo del catálogo | 50.872 | 4,7 |
| games_metadata | snapshot completo (JSON lines) | 50.872 | 17,3 |
| **Total** | | | **399,9** |

> El release se descarga como `steam-landing.zip`, que pesa **146 MB comprimido**. Al descomprimirlo en
> `landing/` ocupa los **399,9 MB** de la tabla: el pipeline trabaja siempre sobre los CSV/JSON descomprimidos.

El dataset publicado en Kaggle ya viene limpio. Con la conformidad de la cátedra, el origen
del TP incluye **problemas de calidad típicos de un sistema real**, para mostrar la limpieza en acción:

| Tabla | Problema presente en el origen |
|---|---|
| recommendations | reviews editadas (mismo `review_id`, `updated_at` posterior) y duplicados exactos; fechas en `YYYY-MM-DD`, `DD/MM/YYYY` y `YYYY/MM/DD`; `is_recommended` como `true`/`True`/`TRUE`/`1`; `hours` con coma decimal, vacía o negativa; votos vacíos; claves vacías; `app_id` inexistentes |
| games | títulos con espacios, `rating` con casing mezclado o vacío, precios con `$`, fechas en 3 formatos, booleanos en mayúscula, versiones viejas duplicadas |
| games_metadata | descripciones en blanco, tags en mayúscula, versiones viejas duplicadas |
| users | `user_id` o `reviews` vacíos, duplicados exactos |
| user_profiles | emails con mayúsculas/espacios o vacíos, país en minúscula, duplicados |

## Diseño

| Tema | Decisión |
|---|---|
| Fecha de negocio | `recommendations.date` (día de la review). Particiona Gold. |
| Fecha de ingesta | El origen entrega la extracción del mes M el 1° de M+1 (= `data_interval_end`). Particiona Bronze. Ej.: reviews de 2021-03 → `bronze/recommendations/ingested_year=2021/ingested_month=04/`. |
| Granularidad | Un DAG run = un mes de negocio (`CronDataIntervalTimetable` mensual, `catchup=True`, `max_active_runs=1`). El param `month` (YYYY-MM) permite forzar un mes en un trigger manual. |
| Tablas incrementales | `recommendations`, `users`, `user_profiles`. |
| Tablas snapshot | `games`, `games_metadata`. |
| Gold | `gold/user_game_reviews/year=YYYY/month=MM/`. Grano: 1 fila por review. Joins N:1 de 5 entidades Silver: recommendations ⨝ games ⨝ games_metadata ⨝ users ⨝ user_profiles. |

Más detalle en [docs/architecture.md](docs/architecture.md).

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
- El salt se lee con `Variable.get("PII_SALT")` **dentro** del task `mask_pii`, nunca a nivel de módulo. No aparece en ningún archivo del repo y queda cifrado con Fernet en la metadata DB.

## Reglas de calidad de datos (Silver)

La limpieza no está atada a los problemas que hoy tiene el origen. Está pensada para que una
extracción futura con otros problemas no rompa el pipeline ni contamine Gold.

### Reglas genéricas (todas las tablas)

| Situación | Regla |
|---|---|
| Nombres de columna con mayúsculas/espacios (`' App ID'`) | se normalizan a `snake_case` antes de validar y limpiar |
| Columnas extra | se ignoran |
| Falta una columna clave | Bronze falla el run con un mensaje explícito (contrato mínimo en `config.EXPECTED_COLUMNS`) |
| Falta una columna no clave | se trata como toda nula y se aplica la regla de nulos de esa columna |
| Bytes con encoding inválido | se reemplazan por U+FFFD; el run no se cae |
| Línea JSON corrupta | se descarta y se cuenta (`dropped_malformed_lines`) |
| Espacios, NBSP, zero-width | trim + colapso de espacios |
| `""`, `null`, `None`, `NaN`, `N/A`, `-`, `?` | se tratan como nulo |
| Números | aceptan `1,234.5`, `1.234,5`, `12,5`, `$ 9.99`, `USD 9.99`; en enteros, `1,234` es separador de miles |
| Enteros con decimales reales (`12.7`) | nulo (no se trunca en silencio) |
| Booleanos | `true/t/1/yes/y/si/sí/recommended` y equivalentes negativos, sin importar el casing |
| Fechas | lista explícita de formatos, probados en orden (primero los inequívocos); lo que no matchea queda nulo |
| Valores fuera de dominio (negativos, % > 100) | nulo, y después se aplica la regla de nulos de la columna |
| Tags | lista o string separado por `, ; \|`, en minúscula y sin repetidos |

### Reglas por campo

| Tabla.campo | Regla | Justificación |
|---|---|---|
| recommendations.review_id nulo | se descarta la fila | Es la clave de deduplicación y el grano de Gold: sin ella no se puede identificar la review. |
| recommendations.app_id / user_id nulo | se descarta | No se puede joinear con ninguna dimensión. |
| recommendations.date nula o no parseable | se descarta | Sin fecha de negocio no se puede asignar a una partición. |
| recommendations.date fuera del mes del run | se descarta | El run procesa estrictamente su ventana lógica. |
| recommendations.is_recommended nulo | se descarta | Es la métrica principal: imputarla sesgaría el % de recomendación. |
| recommendations.hours nulo o negativo | queda nulo | Un 0 sesgaría los promedios; `AVG` ignora los nulos. |
| recommendations.helpful / funny nulo o negativo | se imputa 0 | Que no haya votos registrados equivale a 0 votos. |
| recommendations.updated_at nulo | se imputa con `date` | La versión original de la review es la del día en que se publicó. |
| recommendations duplicadas | por `review_id` gana el `updated_at` más reciente (orden estable ante empates) | Una review editada reemplaza a la anterior. Los duplicados exactos colapsan igual. |
| recommendations con app_id inexistente | van a `silver/_quarantine/recommendations/` | Son huérfanos: no se pierden y se pueden auditar, pero no contaminan Gold. |
| games.app_id nulo | se descarta | Es la clave del catálogo. |
| games / games_metadata duplicados | gana la última fila del snapshot | El catálogo es append-only: la última fila es la versión vigente. |
| games.title nulo | `"Unknown Title"` | El juego sigue siendo válido para las métricas. |
| games.rating nulo | `"No Rating"` | Es una categoría explícita, en lugar de un hueco en los group by. |
| games.win/mac/linux/steam_deck nulo | `False` | Si la plataforma no está declarada, no se considera soportada. |
| games.price_* negativo / discount o positive_ratio fuera de 0–100 | nulo | Son imposibles de negocio; mejor nulo que un valor falso. |
| games.date_release no parseable | queda nulo | Es un atributo descriptivo y no bloquea los joins. |
| games_metadata.description en blanco | nulo | Un texto vacío no aporta información. |
| users.user_id nulo | se descarta | No se puede joinear. |
| users.products / reviews nulo o negativo | queda nulo | Imputar 0 sesgaría los perfiles de actividad. |
| users / user_profiles duplicados | gana la última fila | La última versión del registro es la vigente. |
| user_profiles.email nulo | `email_hash` nulo, la fila se mantiene | El usuario existe aunque no haya dejado su email. |
| user_profiles.country_code nulo | `"ZZ"` (código ISO reservado para "desconocido") | Permite agrupar sin perder filas. |
| Gold: usuario sin perfil | left join, atributos nulos | Que falte el perfil no invalida la review. |

## Validaciones de la entrega

```bash
docker compose exec airflow-scheduler python scripts/finops_validation.py --out docs/finops_output.txt
docker compose exec airflow-scheduler python scripts/idempotency_check.py snapshot --month 2021-03 --out docs/idempotency_before.json
# Clear del run 2021-03 en la UI (o trigger con params {"month": "2021-03"})
docker compose exec airflow-scheduler python scripts/idempotency_check.py compare --month 2021-03 --baseline docs/idempotency_before.json
```

## Estado del TP

Actualizado al 22/10/2026.

| Requisito de la consigna | Estado |
|---|---|
| Dataset ≥3 tablas, fecha nativa, PII sintética, datos sucios | ✅ release `dataset-v1` (5 tablas, 2020-01 → 2022-12) |
| Tamaño 50–300 MB | ⚠️ son 399,9 MB descomprimidos. Decisión del equipo: hay que justificarla en la defensa o bajar la muestra |
| Estructura `bronze/{table}/ingested_year=/ingested_month=`, `silver/{table}`, `gold/{dataset}/year=/month=` | ✅ implementada |
| Silver con pandas + pyarrow en `@task`, dedup, tipado, nulos documentados | ✅ implementado y documentado en este README |
| PII con SHA-256 + salt, salt en Airflow Variable leída dentro del task | ✅ HMAC-SHA256, `Variable.get` dentro de `mask_pii` |
| Gold con ≥4 entidades y particionado Hive | ✅ 5 entidades |
| DAG con ≥5 tasks y `>>` | ✅ 9 tasks |
| Idempotencia (overwrite de particiones) | ✅ implementada; validada sin Airflow (re-run de un mes → conteos idénticos) |
| DuckDB EXPLAIN ANALYZE full vs. pruned | ✅ script listo; validado sin Airflow (36 → 1 Parquet, −95% de filas) |
| Repo con `/dags`, `/scripts`, `/sql`, `/docs` y `.gitignore` | ✅ |
| Ramas `main` / `develop` / `feature/*` con PRs | ✅ en uso. ⚠️ La protección automática de `main` no está activa (repo privado + cuenta gratuita); se activa con GitHub Education/Pro. Mientras tanto, **nadie pushea directo a `main` ni a `develop`** |

### Próximos pasos

- [ ] Correr el backfill completo **dentro de Airflow** y revisar los logs de cada task.
- [ ] Generar las evidencias de la entrega y commitearlas en `docs/`: `finops_output.txt` y la salida de `idempotency_check.py` (ver [Validaciones de la entrega](#validaciones-de-la-entrega)).
- [ ] Capturas de la UI: Graph view y un run exitoso.
- [ ] Tests unitarios de `steam_lib` y CI en GitHub Actions.
- [ ] Decidir el tema del tamaño (399,9 MB vs. 50–300 MB).
- [ ] Ensayar la defensa con [docs/defense.md](docs/defense.md).

## Workflow del equipo

`main` (protegida, solo PR) ← `develop` (integración) ← `feature/<tema>`. Ver [docs/team_workflow.md](docs/team_workflow.md).
Preparación de la defensa: [docs/defense.md](docs/defense.md).
