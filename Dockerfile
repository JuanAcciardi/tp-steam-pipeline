# Imagen de Airflow de la consigna (3.0.4, Python 3.12) + dependencias del pipeline.
FROM apache/airflow:3.0.4-python3.12

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.4/constraints-3.12.txt"
