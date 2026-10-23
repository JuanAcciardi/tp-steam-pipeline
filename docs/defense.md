# Preparación de la defensa

Guion para cada bloque de preguntas de la consigna (sección 7). Antes de la defensa: Airflow
levantado, backfill completo y una terminal con el repo abierta.

## Demo en vivo (≈5 min)

1. UI → `steam_medallion_pipeline` → **Graph**: recorrer los 9 tasks.
2. Mostrar un run ya completado (por ejemplo 2021-03) y los logs de `clean_recommendations_silver`
   con las estadísticas de limpieza: duplicados, nulos descartados, cuarentena.
3. Test de idempotencia:
   ```bash
   docker compose exec airflow-scheduler python scripts/idempotency_check.py snapshot --month 2021-03 --out docs/idempotency_before.json
   # UI: run 2021-03 -> Clear -> esperar a que termine
   docker compose exec airflow-scheduler python scripts/idempotency_check.py compare --month 2021-03 --baseline docs/idempotency_before.json
   ```
4. FinOps: `docker compose exec airflow-scheduler python scripts/finops_validation.py`.

## Idempotencia

**¿Qué pasa al re-ejecutar un mes?** Se reescriben exactamente los mismos archivos:
`bronze/*/ingested_year=2021/ingested_month=04/`, `silver/*/part-2021-03.parquet` y
`gold/user_game_reviews/year=2021/month=03/`. Los conteos quedan iguales; `idempotency_check.py`
compara filas y archivos de cada capa.

**Código que lo garantiza:** `steam_lib/io_utils.py`
- `write_partition()`: escribe en `data/_tmp/<uuid>`, hace `rmtree` de la partición y
  `os.replace` del temporal.
- `write_parquet()`: escribe en un temporal y hace `os.replace` sobre el archivo final.
- `resolve_window()`: las rutas salen de la ventana del run, nunca de `now()`.

**¿Por qué el overwrite garantiza idempotencia?** Porque el resultado depende solo de la entrada
del mes, no del estado previo del destino. Con append, cada re-run sumaría las mismas filas otra
vez. Como escribimos en un temporal, un fallo a mitad de camino deja la versión anterior intacta.

## Composición de tasks

**¿Por qué separar los tasks?**
- **Reintentos granulares:** si falla Gold, el retry no vuelve a copiar Bronze ni a limpiar Silver.
- **Paralelismo:** catálogo, usuarios y PII no dependen entre sí.
- **Aislamiento de lo sensible:** `mask_pii` es el único task que lee el salt y el único que toca
  la PII en claro.
- **Observabilidad:** cada task devuelve sus estadísticas por XCom y se ve en qué paso se perdieron filas.
- `clean_recommendations_silver` depende del catálogo porque necesita los `app_id` válidos para
  mandar los huérfanos a cuarentena.

## Higiene de datos

- Todas las reglas, con su justificación, están en el README ("Reglas de calidad de datos").
- **¿Por qué descartar las reviews sin `is_recommended`?** Es la métrica principal. Imputar
  `true` o `false` mueve directamente el % de recomendación. Son el 0,1%, así que descartarlas
  no cambia nada material.
- **¿Por qué `hours` nulo y no 0?** Un 0 es un dato ("no jugó") y bajaría el promedio. Nulo
  significa "no sabemos", y `AVG` lo ignora.
- **Deduplicación real:** por clave de negocio (`review_id`) quedándose con el `updated_at` más
  reciente, no con `drop_duplicates()` sobre la fila entera. Así colapsan tanto los duplicados
  exactos como las ediciones. El archivo viene desordenado, por eso no confiamos en "la última fila".
- **Datos sucios futuros:** las reglas son genéricas (tokens de nulo, separadores de miles,
  múltiples formatos de fecha, columnas faltantes o renombradas, encoding inválido, JSON corrupto).

## Particionado y performance

- **Clave:** año/mes de `review_date`. Ver la justificación en `docs/architecture.md`.
- **Evidencia:** el full scan lee 36 Parquet; el pruned lee 1, con un ~95% menos de filas.
- **Fecha de ingesta vs. fecha de negocio:** Bronze se particiona por cuándo llegó el archivo
  (linaje y auditoría: "¿qué recibimos en abril?"). Gold se particiona por cuándo ocurrió la
  review (análisis: "¿qué pasó en marzo?"). Las reviews de marzo están en
  `bronze/.../ingested_month=04/` y en `gold/.../month=03/`.

## Seguridad y gobernanza

- **Cómo se obtiene el salt:** `Variable.get("PII_SALT")` dentro de `mask_pii`
  (`dags/steam_pipeline.py`), nunca a nivel de módulo. Así no golpeamos la metadata DB en cada
  parseo del DAG.
- **¿Qué impide revertir el hash?** Es HMAC-SHA256 con una clave secreta. Sin el salt, un
  atacante no puede armar un diccionario de emails→hash, porque cada hash depende de un secreto
  que no está en los archivos. Sin salt, `sha256("juan@gmail.com")` se rompe por fuerza bruta en
  segundos.
- **¿Por qué una Airflow Variable y no una constante?** Una constante queda en el repo (privado)
  y en su historial para siempre. La Variable vive en la metadata DB, cifrada con la Fernet key
  de `.env` (que tampoco se versiona), y se puede rotar sin tocar código.
- `full_name` y `phone_number` ni siquiera llegan a Silver.

## Modelado

- **Joins:** recommendations ⨝ games (inner, los huérfanos ya están en cuarentena) ⨝
  games_metadata (left) ⨝ users (left) ⨝ user_profiles (left). Todos son N:1 y lo validamos con
  `validate="many_to_one"`. Además, `audit_counts` verifica que Gold tenga la misma cantidad de
  filas que Silver, o sea que no hay fan-out.
- **¿Por qué desnormalizar?** Ver `docs/architecture.md`: menos joins en lectura, motor columnar
  y pruning sobre la tabla de hechos.

## Local → AWS

| Local | AWS | Cambio de código |
|---|---|---|
| Airflow en Docker Compose | Amazon MWAA (o Airflow en ECS/EKS) | Ninguno en el DAG; subir `dags/` y `requirements.txt` a S3 |
| `landing/`, `data/{bronze,silver,gold}` | S3: `s3://bucket/{landing,bronze,silver,gold}/` con las mismas rutas Hive | Cambiar `config.py` a URIs `s3://`; leer y escribir con `pyarrow.fs.S3FileSystem` / `s3fs` |
| `rmtree` + `os.replace` | S3 no tiene rename atómico: borrar el prefijo y escribir (o escribir en un prefijo versionado y mover un puntero) | Reemplazar `replace_dir()` por `delete_objects(prefix)` + escritura, o pasar a Iceberg |
| Airflow Variable `PII_SALT` | AWS Secrets Manager como secrets backend de Airflow | Ninguno: `Variable.get` resuelve contra Secrets Manager |
| pandas dentro del task | Igual para este volumen; si crece, AWS Glue (Spark) o EMR Serverless lanzados desde Airflow | Mover `steam_lib` a jobs de Glue |
| DuckDB + `catalog.duckdb` | AWS Glue Data Catalog + Amazon Athena | Crear la tabla con `PARTITIONED BY (year, month)` y `MSCK REPAIR` / partition projection |
| `finops_validation.py` | Athena "Data scanned" por query (USD 5/TB) | Comparar los bytes escaneados con y sin `WHERE year/month` |
| Logs en `./logs` | CloudWatch Logs | Configuración de MWAA |
