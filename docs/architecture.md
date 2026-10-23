# Arquitectura

## Vista general

```
                 ┌──────────────────────────── Airflow 3.0.4 (Docker Compose, LocalExecutor) ───────────────────────────┐
 landing/        │                                                                                                      │
 (origen,        │  land_raw_files ─► validate_bronze ─┬─► clean_catalog_silver ─► clean_recommendations_silver ─┐      │
  1 extracción   │                                     ├─► clean_users_silver ──────────────────────────────────┤      │
  por mes)       │                                     └─► mask_pii ────────────────────────────────────────────┤      │
                 │                                                                                              ▼      │
                 │                                   audit_counts ◄─ update_duckdb_catalog ◄─ build_gold_partition     │
                 └──────────────────────────────────────────────────────────────────────────────────────────────────────┘
                        │                    │                         │                        │
                        ▼                    ▼                         ▼                        ▼
                  data/bronze/         data/silver/              data/gold/              data/catalog.duckdb
                  CSV/JSON crudos      Parquet tipado,           Parquet Hive            vistas sobre Gold
                  por fecha de         deduplicado, PII          year=/month=            y Silver
                  INGESTA              hasheada                  por fecha de NEGOCIO
```

## Capas

### Landing (origen)

Simula el sistema transaccional, que deja una extracción por mes:

```
landing/
  recommendations/recommendations_YYYY_MM.csv   reviews del mes de negocio
  users/users_YYYY_MM.csv                       usuarios dados de alta ese mes
  user_profiles/user_profiles_YYYY_MM.csv       PII de esos usuarios
  games/games.csv                               snapshot completo del catálogo
  games_metadata/games_metadata.json            snapshot completo (JSON lines)
```

### Bronze

- Copia **byte a byte** (`shutil.copy2`), sin parseo, a
  `bronze/{table}/ingested_year=YYYY/ingested_month=MM/`.
- La fecha de ingesta es el mes siguiente al de negocio: la extracción de marzo llega el 1° de abril.
  Para un run programado es `data_interval_end`.
- Los snapshots (`games`, `games_metadata`) solo se copian si su SHA-256 difiere del último snapshot
  con partición ≤ al mes de ingesta del run. Silver lee ese snapshot (`bronze.snapshot_dir`). Detalle y
  motivo (incluido por qué no hardlinks) en el README, sección *Bronze y los snapshots*.
- `validate_bronze` verifica el contrato mínimo: que cada archivo exista, que tenga las columnas
  clave y que no esté vacío. Si algo falla, el run se corta antes de tocar Silver.

### Silver

| Tabla | Archivo | Escritura |
|---|---|---|
| recommendations | `silver/recommendations/part-YYYY-MM.parquet` | 1 archivo por mes de negocio |
| users | `silver/users/part-YYYY-MM.parquet` | 1 archivo por cohorte mensual |
| user_profiles | `silver/user_profiles/part-YYYY-MM.parquet` | ídem, ya con PII hasheada |
| games | `silver/games/games.parquet` | snapshot |
| games_metadata | `silver/games_metadata/games_metadata.parquet` | snapshot |
| huérfanos | `silver/_quarantine/recommendations/part-YYYY-MM.parquet` | 1 archivo por mes |

Todas las reglas de tipado, nulos y deduplicación están en el README.

### Gold

`gold/user_game_reviews/year=YYYY/month=MM/part-0.parquet`: una fila por review con atributos del
juego, de su metadata, del usuario y de su perfil seudonimizado.

**Por qué desnormalizar:** las consultas analíticas típicas ("% de recomendación por tag y país",
"horas promedio por rating") cruzan las 5 entidades. En 3FN, cada consulta paga 4 joins.
Desnormalizado, se paga una vez al escribir. En un motor columnar (Parquet + DuckDB, o Athena en
la nube) leer solo las columnas necesarias de una tabla ancha es barato, y el partition pruning
funciona sobre la tabla de hechos, que es la que crece.

**Por qué particionar por año y mes de la review:**
- Es la dimensión por la que filtran casi todas las consultas.
- Coincide con la ventana de procesamiento del DAG: un run = una partición, lo que hace trivial
  el overwrite idempotente.
- Da 36 particiones de entre 40k y 108k filas cada una (pocos MB por archivo). Particionar por día daría
  más de 1000 archivos chicos, y particionar por `app_id` haría que el run de un mes reescriba
  miles de carpetas.

### Catálogo

`update_duckdb_catalog` recrea (`CREATE OR REPLACE VIEW`) la vista `user_game_reviews` con
`read_parquet('.../**/*.parquet', hive_partitioning = true)` y las vistas `silver_*`, en
`data/catalog.duckdb`.

## Idempotencia

1. Cada task calcula su ventana a partir del contexto del run (`data_interval_start`, o el param `month`).
   No hay `now()` en ninguna ruta ni en ningún dato.
2. Cada escritura reemplaza exactamente lo que le corresponde al mes:
   - Bronze y Gold: se arma la carpeta en `data/_tmp/`, se borra la carpeta destino y se
     renombra (`os.replace`, atómico dentro del mismo filesystem).
   - Silver: `pq.write_table` a un temporal y `os.replace` sobre `part-YYYY-MM.parquet`.
3. Las transformaciones son deterministas: se ordena antes de deduplicar (`mergesort` estable) y
   antes de escribir.
4. `audit_counts` deja `data/_audit/YYYY-MM.json` y falla si Gold ≠ Silver.

Re-ejecutar un mes produce los mismos archivos, con las mismas filas, en las mismas rutas.
Un retry a mitad de camino tampoco deja basura, porque nada se escribe directo sobre el destino.

## Decisiones y trade-offs

| Decisión | Alternativa descartada | Por qué |
|---|---|---|
| Docker Compose | `airflow standalone` en WSL | Entorno idéntico para 5 personas en Windows/macOS; misma versión 3.0.4 |
| LocalExecutor | CeleryExecutor | Un solo nodo: Celery agrega Redis y workers sin beneficio |
| `depends_on_past=True`, `max_active_runs=1` | runs en paralelo | users y user_profiles se acumulan por cohorte; el orden garantiza que todas las cohortes anteriores existan |
| Cuarentena de huérfanos | descartarlos | No se pierde información y se pueden auditar |
| HMAC-SHA256 | `sha256(salt + email)` | Es la construcción estándar de hash con clave; evita ataques de extensión de longitud |
| Lógica en `steam_lib/` | todo dentro del DAG | Se puede testear sin Airflow; el DAG queda como orquestación pura |
