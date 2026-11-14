"""Controles de calidad sobre la capa Bronze antes de transformar."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def validar_bronze(archivos: list[Path], cfg: dict) -> dict:
    """Valida cada snapshot de Bronze. Lanza ValueError si alguno no es usable."""
    if not archivos:
        raise FileNotFoundError("No hay archivos en la capa Bronze. Ejecute primero la extracción.")

    columnas = cfg["calidad"]["columnas_requeridas"]
    min_filas = cfg["calidad"]["min_filas_bronze"]
    resumen = {}
    for archivo in archivos:
        df = pd.read_csv(archivo)
        faltantes = [c for c in columnas if c not in df.columns]
        if faltantes:
            raise ValueError(f"{archivo.name}: faltan columnas {faltantes}")
        if len(df) < min_filas:
            raise ValueError(f"{archivo.name}: solo {len(df)} filas (mínimo {min_filas})")
        fuera_rango = (~df["latitude"].between(-90, 90) | ~df["longitude"].between(-180, 180)).sum()
        if fuera_rango:
            raise ValueError(f"{archivo.name}: {fuera_rango} coordenadas fuera de rango")
        resumen[archivo.name] = {
            "filas": int(len(df)),
            "nulos": int(df[columnas].isna().sum().sum()),
            "fecha_min": str(df["acq_date"].min()),
            "fecha_max": str(df["acq_date"].max()),
        }
    return resumen
