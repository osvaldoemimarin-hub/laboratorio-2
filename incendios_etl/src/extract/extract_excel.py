"""Extracción de la segunda fuente: archivo Excel del Banco Mundial con el área de bosque
por país (indicador AG.LND.FRST.K2, km²).

Se intenta descargar la versión más reciente; si no hay conexión se usa la copia de
respaldo incluida en el repositorio. En ambos casos el archivo se guarda sin cambios
en la capa Bronze, con la fecha de extracción en el nombre.
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.config import BRONZE_DIR, PROJECT_DIR, cargar_config

FIRMA_XLS = b"\xd0\xcf\x11\xe0"  # Excel 97-2003 (.xls)
FIRMA_XLSX = b"PK"               # Excel moderno (.xlsx, es un ZIP)


def es_excel(contenido: bytes) -> bool:
    return contenido.startswith(FIRMA_XLS) or contenido.startswith(FIRMA_XLSX)


def descargar_excel_bosque(cfg: dict | None = None, destino: Path = BRONZE_DIR) -> Path:
    cfg = cfg or cargar_config()
    fx = cfg["fuente_excel"]
    destino.mkdir(parents=True, exist_ok=True)
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    try:
        resp = requests.get(fx["url"], timeout=fx["timeout_s"])
        resp.raise_for_status()
        if not es_excel(resp.content):
            raise ValueError("la respuesta no es un archivo Excel")
        extension = ".xls" if resp.content.startswith(FIRMA_XLS) else ".xlsx"
        archivo = destino / f"bosque_banco_mundial_descarga_{marca}{extension}"
        archivo.write_bytes(resp.content)
        origen = "descarga del Banco Mundial"
    except (requests.RequestException, ValueError) as e:
        respaldo = PROJECT_DIR / fx["archivo_respaldo"]
        archivo = destino / f"bosque_banco_mundial_respaldo_{marca}{respaldo.suffix}"
        shutil.copyfile(respaldo, archivo)
        origen = f"copia de respaldo ({e})"

    print(f"Excel de bosque guardado en Bronze ({origen}) -> {archivo.name}")
    return archivo


def listar_bronze_excel(directorio: Path = BRONZE_DIR) -> list[Path]:
    return sorted(directorio.glob("bosque_banco_mundial_*.xls*"))


def leer_excel_bosque(ruta: str | Path, cfg: dict | None = None) -> pd.DataFrame:
    cfg = cfg or cargar_config()
    fx = cfg["fuente_excel"]
    return pd.read_excel(ruta, sheet_name=fx["hoja"], skiprows=fx["filas_encabezado"])
