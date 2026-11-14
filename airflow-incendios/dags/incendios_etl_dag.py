from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task

PROJECT_DIR = Path("/opt/airflow/incendios_etl")
sys.path.insert(0, str(PROJECT_DIR))

default_args = {
    "owner": "equipo-lab2",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="incendios_firms_medallion",
    description="Green Data: API NASA FIRMS + Excel Banco Mundial -> Bronze -> Silver (JOIN) -> Gold (KPIs) -> Almacén -> Modelo",
    start_date=datetime(2026, 11, 1),
    schedule="0 6 * * *",  # todos los días a las 06:00 UTC (FIRMS se actualiza a diario)
    catchup=False,
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
