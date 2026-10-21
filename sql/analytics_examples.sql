-- Consultas de ejemplo sobre el catálogo (duckdb ~/airflow/data/catalog.duckdb).

-- Top 10 juegos por reviews positivas en un mes
SELECT game_title, COUNT(*) AS reviews, AVG(is_recommended::INT) AS pct_recommended
FROM user_game_reviews
WHERE year = 2021 AND month = 3
GROUP BY game_title
ORDER BY reviews DESC
LIMIT 10;

-- Horas jugadas promedio por país y tag principal
SELECT country_code, primary_tag, ROUND(AVG(hours), 1) AS avg_hours, COUNT(*) AS reviews
FROM user_game_reviews
GROUP BY ALL
HAVING COUNT(*) > 100
ORDER BY reviews DESC;

-- Los analistas pueden unir por email_hash sin conocer el email real
SELECT email_hash, COUNT(*) AS reviews
FROM user_game_reviews
GROUP BY email_hash
ORDER BY reviews DESC
LIMIT 5;
