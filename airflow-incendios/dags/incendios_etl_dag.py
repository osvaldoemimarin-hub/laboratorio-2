from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, task

PROJECT_DIR = Path("/opt/airflow/incendios_etl")
sys.path.insert(0, str(PROJECT_DIR))

ZONA = "America/Bogota"

# Programación según la disponibilidad de los datos:
# - NOAA-20 pasa sobre Sudamérica dos veces al día: de noche (≈ 03:00–09:00 UTC) y de día
#   (≈ 15:00–20:00 UTC).
# - FIRMS publica los datos casi en tiempo real con unas 3 horas de demora (medido: 2,2–3,0 h).
# - Por eso el DAG corre a las 07:00 y a las 19:00 hora de Colombia (12:00 y 00:00 UTC),
#   cuando cada pasada ya está publicada completa.
# - catchup=False: si el PC estaba apagado, al volver se ejecuta solo la corrida más reciente.
HORARIO = "0 7,19 * * *"

default_args = {
    "owner": "equipo-lab2",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),  # cubre cortes breves de red o de la API
}

with DAG(
    dag_id="incendios_firms_medallion",
    description="Green Data: API NASA FIRMS + Excel Banco Mundial -> Bronze -> Silver (JOIN) -> Gold (KPIs) -> Almacén -> Modelo",
    start_date=pendulum.datetime(2026, 11, 1, tz=ZONA),
    schedule=HORARIO,
    catchup=False,
    max_active_runs=1,  # nunca dos ejecuciones a la vez sobre los mismos archivos
    default_args=default_args,
    tags=["etl", "medallion", "incendios", "nasa-firms"],
) as dag:

    @task
    def extraer_firms():
        from src.pipeline import etapa_extraer
        return etapa_extraer()

    @task
    def extraer_excel_bosque():
        from src.pipeline import etapa_extraer_excel
        return etapa_extraer_excel()

    @task(retries=0)
    def validar_bronze():
        from src.pipeline import etapa_validar_bronze
        return etapa_validar_bronze()

    @task
    def transformar_a_silver():
        from src.pipeline import etapa_silver
        return etapa_silver()

    @task
    def integrar_a_gold():
        from src.pipeline import etapa_gold
        return etapa_gold()

    @task
    def cargar_datawarehouse():
        from src.pipeline import etapa_cargar
        return etapa_cargar()

    @task
    def entrenar_modelo():
        from src.pipeline import etapa_entrenar
        return etapa_entrenar()

    api = extraer_firms()
    excel = extraer_excel_bosque()
    bronze = validar_bronze()
    silver = transformar_a_silver()
    gold = integrar_a_gold()
    load = cargar_datawarehouse()
    modelo = entrenar_modelo()

    # Las dos fuentes se extraen en paralelo; Bronze se valida cuando ambas terminan.
    [api, excel] >> bronze >> silver >> gold >> load >> modelo
