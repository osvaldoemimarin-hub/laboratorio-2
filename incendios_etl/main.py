"""Ejecuta el pipeline completo sin Airflow (útil para desarrollo y pruebas).

    python main.py               # extrae datos nuevos y procesa todo
    python main.py --sin-extraer # reprocesa solo lo que ya está en Bronze
"""
import argparse

from src import pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sin-extraer", action="store_true")
    args = parser.parse_args()

    if not args.sin_extraer:
        pipeline.etapa_extraer()
    pipeline.etapa_validar_bronze()
    pipeline.etapa_silver()
    pipeline.etapa_gold()
    pipeline.etapa_cargar()
    pipeline.etapa_entrenar()
