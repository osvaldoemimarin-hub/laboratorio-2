"""Funciones de cada etapa del pipeline. Las usan tanto main.py como el DAG de Airflow."""
from __future__ import annotations

import pandas as pd

from src import config as C
from src.extract.extract_firms import descargar_firms, extraer_datos, listar_bronze
from src.load.load_datawarehouse import cargar_datos
from src.model.train import entrenar_modelos
from src.transform.clean_firms import unir_snapshots
from src.transform.eda import generar_graficas
from src.transform.gold_data import integrar_datos
from src.transform.validate_bronze import validar_bronze


def etapa_extraer() -> str:
    C.asegurar_directorios()
    return str(descargar_firms(C.cargar_config()))


def etapa_validar_bronze() -> dict:
    resumen = validar_bronze(listar_bronze(), C.cargar_config())
    total = sum(r["filas"] for r in resumen.values())
    print(f"Bronze validado: {len(resumen)} archivos, {total:,} filas")
    return {"archivos": len(resumen), "filas": total}


def etapa_silver() -> int:
    C.asegurar_directorios()
    dfs = [extraer_datos(p) for p in listar_bronze()]
    silver = unir_snapshots(dfs)
    silver.to_csv(C.SILVER_FILE, index=False)
    print(f"Silver: {len(silver):,} detecciones únicas -> {C.SILVER_FILE.name}")
    return len(silver)


def etapa_gold() -> dict:
    silver = pd.read_csv(C.SILVER_FILE)
    gold = integrar_datos(silver, C.cargar_config())
    gold["detecciones"].to_csv(C.GOLD_DETECCIONES, index=False)
    gold["resumen_diario"].to_csv(C.GOLD_RESUMEN, index=False)
    generar_graficas(gold["detecciones"])
    print(f"Gold: {gold['detecciones'].shape} detecciones, {gold['resumen_diario'].shape} resumen")
    return {k: len(v) for k, v in gold.items()}


def etapa_cargar() -> dict:
    gold = {
        "detecciones": pd.read_csv(C.GOLD_DETECCIONES),
        "resumen_diario": pd.read_csv(C.GOLD_RESUMEN),
    }
    return cargar_datos(gold)


def etapa_entrenar() -> dict:
    metricas = entrenar_modelos(pd.read_csv(C.GOLD_DETECCIONES), C.cargar_config())
    return {
        "recomendado": metricas["modelo_recomendado"],
        "f1_lr": metricas["modelos"]["logistic_regression"]["f1_macro"],
        "f1_rf": metricas["modelos"]["random_forest"]["f1_macro"],
    }
