-- Misma consulta con filtro sobre las columnas de partición: DuckDB descarta carpetas
-- year=/month= que no matchean antes de abrir los archivos (partition pruning).
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS records, AVG(hours) AS avg_hours,
       AVG(CASE WHEN is_recommended THEN 1 ELSE 0 END) AS pct_recommended
FROM read_parquet('{gold_glob}', hive_partitioning = true)
WHERE year = '{year}' AND month = '{month}'
GROUP BY year, month
ORDER BY year, month;
