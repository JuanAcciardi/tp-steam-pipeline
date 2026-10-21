-- Escaneo completo: sin filtro de partición, DuckDB lee todos los Parquet de Gold.
EXPLAIN ANALYZE
SELECT year, month, COUNT(*) AS records, AVG(hours) AS avg_hours,
       AVG(CASE WHEN is_recommended THEN 1 ELSE 0 END) AS pct_recommended
FROM read_parquet('{gold_glob}', hive_partitioning = true)
GROUP BY year, month
ORDER BY year, month;
