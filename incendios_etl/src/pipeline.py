"""Funciones de cada etapa del pipeline. Las usan tanto main.py como el DAG de Airflow."""
from __future__ import annotations

import pandas as pd

from src import config as C
from src.extract.extract_excel import descargar_excel_bosque, leer_excel_bosque, listar_bronze_excel
from src.extract.extract_firms import descargar_firms, extraer_datos, listar_bronze
from src.load.load_datawarehouse import cargar_datos
from src.model.train import entrenar_modelos
from src.transform.clean_firms import unir_snapshots
from src.transform.eda import generar_graficas
from src.transform.gold_data import integrar_datos
from src.transform.integrar_fuentes import limpieza_bosque, unir_fuentes
from src.transform.validate_bronze import validar_bronze, validar_bronze_excel


def _excel_mas_reciente():
    archivos = listar_bronze_excel()
    return archivos[-1] if archivos else None


def etapa_extraer() -> str:
    """Fuente 1: API REST de NASA FIRMS."""
    C.asegurar_directorios()
    return str(descargar_firms(C.cargar_config()))


def etapa_extraer_excel() -> str:
    """Fuente 2: archivo Excel del Banco Mundial (área de bosque por país)."""
    C.asegurar_directorios()
    return str(descargar_excel_bosque(C.cargar_config()))


def etapa_validar_bronze() -> dict:
    cfg = C.cargar_config()
    resumen = validar_bronze(listar_bronze(), cfg)
    total = sum(r["filas"] for r in resumen.values())
    excel = _excel_mas_reciente()
    info_excel = validar_bronze_excel(excel, leer_excel_bosque(excel, cfg) if excel else None, cfg)
    print(f"Bronze validado: {len(resumen)} snapshots FIRMS ({total:,} filas) + Excel {info_excel['archivo']}")
    return {"archivos_firms": len(resumen), "filas_firms": total, "excel": info_excel["archivo"]}


def etapa_silver() -> dict:
    C.asegurar_directorios()
    cfg = C.cargar_config()

    detecciones = unir_snapshots([extraer_datos(p) for p in listar_bronze()])
    detecciones.to_csv(C.SILVER_FILE, index=False)

    bosque = limpieza_bosque(leer_excel_bosque(_excel_mas_reciente(), cfg), cfg["paises_sudamerica"])
    bosque.to_csv(C.SILVER_BOSQUE, index=False)

    integrado = unir_fuentes(detecciones, bosque, cfg)
    integrado.to_csv(C.SILVER_INTEGRADO, index=False)

    con_bosque = int(integrado["tiene_dato_bosque"].sum())
    print(f"Silver: {len(detecciones):,} detecciones únicas; bosque de {len(bosque)} países; "
          f"JOIN: {con_bosque:,} detecciones con dato de bosque ({con_bosque / len(integrado):.1%})")
    return {"detecciones": len(detecciones), "paises_bosque": len(bosque), "con_bosque": con_bosque}


def etapa_gold() -> dict:
    integrado = pd.read_csv(C.SILVER_INTEGRADO)
    gold = integrar_datos(integrado, C.cargar_config())
    gold["detecciones"].to_csv(C.GOLD_DETECCIONES, index=False)
    gold["resumen_diario"].to_csv(C.GOLD_RESUMEN, index=False)
    gold["kpis_pais"].to_csv(C.GOLD_FINAL, index=False)
    generar_graficas(gold["detecciones"], gold["kpis_pais"])
    print(f"Gold: {len(gold['detecciones']):,} detecciones; gold_final.csv con KPIs de "
          f"{len(gold['kpis_pais'])} países")
    return {k: len(v) for k, v in gold.items()}


def etapa_cargar() -> dict:
    gold = {
        "detecciones": pd.read_csv(C.GOLD_DETECCIONES),
        "resumen_diario": pd.read_csv(C.GOLD_RESUMEN),
        "kpis_pais": pd.read_csv(C.GOLD_FINAL),
    }
    return cargar_datos(gold)


def etapa_entrenar() -> dict:
    metricas = entrenar_modelos(pd.read_csv(C.GOLD_DETECCIONES), C.cargar_config())
    return {
        "recomendado": metricas["modelo_recomendado"],
        "f1_lr": metricas["modelos"]["logistic_regression"]["f1_macro"],
        "f1_rf": metricas["modelos"]["random_forest"]["f1_macro"],
    }
