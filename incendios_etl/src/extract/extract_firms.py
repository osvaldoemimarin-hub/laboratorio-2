"""Extracción: descarga detecciones de incendios activos desde NASA FIRMS (Earthdata).

Si existe la variable de entorno FIRMS_MAP_KEY se usa la API de área
(https://firms.modaps.eosdis.nasa.gov/api/area/). Si no, se usa el feed público
de 7 días, que no requiere registro. Cada descarga se guarda tal cual en la
capa Bronze con la fecha/hora de extracción en el nombre (histórico inmutable).
"""
from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.config import BRONZE_DIR, cargar_config


def construir_url(cfg: dict, map_key: str | None) -> tuple[str, str]:
    """Devuelve (url, origen). El origen queda registrado en el nombre del archivo."""
    fuente = cfg["fuente"]
    if map_key:
        url = fuente["url_api"].format(
            key=map_key, sensor=fuente["sensor_api"], bbox=fuente["bbox"], dias=fuente["dias"]
        )
        return url, "api"
    return fuente["url_publica"], "feed"


def validar_csv_firms(texto: str, columnas: list[str]) -> pd.DataFrame:
    """Convierte la respuesta en DataFrame y verifica que tenga el esquema de FIRMS.

    La API responde texto plano (no CSV) cuando la clave es inválida o se excede
    la cuota, así que validar las columnas evita guardar basura en Bronze.
    """
    df = pd.read_csv(io.StringIO(texto))
    faltantes = [c for c in columnas if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"La respuesta de FIRMS no tiene el esquema esperado. Faltan {faltantes}. "
            f"Inicio de la respuesta: {texto[:200]!r}"
        )
    return df


def descargar_firms(cfg: dict | None = None, destino: Path = BRONZE_DIR) -> Path:
    cfg = cfg or cargar_config()
    map_key = os.environ.get("FIRMS_MAP_KEY") or None
    url, origen = construir_url(cfg, map_key)

    resp = requests.get(url, timeout=cfg["fuente"]["timeout_s"])
    resp.raise_for_status()
    df = validar_csv_firms(resp.text, cfg["calidad"]["columnas_requeridas"])

    destino.mkdir(parents=True, exist_ok=True)
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archivo = destino / f"firms_viirs_noaa20_{origen}_{marca}.csv"
    archivo.write_text(resp.text, encoding="utf-8")
    print(f"Descargadas {len(df):,} detecciones ({origen}) -> {archivo.name}")
    return archivo


def listar_bronze(directorio: Path = BRONZE_DIR) -> list[Path]:
    return sorted(directorio.glob("firms_*.csv"))


def extraer_datos(ruta: str | Path) -> pd.DataFrame:
    return pd.read_csv(ruta)


if __name__ == "__main__":
    descargar_firms()
