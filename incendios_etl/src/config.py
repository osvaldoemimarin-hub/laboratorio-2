"""Rutas y configuración compartidas por todas las etapas del pipeline."""
from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_DIR = Path(__file__).resolve().parents[1]
CONFIG_FILE = PROJECT_DIR / "config" / "config.yaml"

DATA_DIR = PROJECT_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
WAREHOUSE_DB = WAREHOUSE_DIR / "incendios_dw.db"

OUTPUTS_DIR = PROJECT_DIR / "outputs"
MODELS_DIR = OUTPUTS_DIR / "models"
REPORTS_DIR = OUTPUTS_DIR / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

REFERENCIA_DIR = DATA_DIR / "referencia"

SILVER_FILE = SILVER_DIR / "silver_detecciones.csv"
SILVER_BOSQUE = SILVER_DIR / "silver_bosque_pais.csv"
SILVER_INTEGRADO = SILVER_DIR / "silver_integrado.csv"
GOLD_DETECCIONES = GOLD_DIR / "gold_detecciones.csv"
GOLD_RESUMEN = GOLD_DIR / "gold_resumen_diario.csv"
GOLD_FINAL = GOLD_DIR / "gold_final.csv"


def cargar_config(path: Path = CONFIG_FILE) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def asegurar_directorios() -> None:
    for d in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, WAREHOUSE_DIR, MODELS_DIR, FIGURES_DIR):
        d.mkdir(parents=True, exist_ok=True)
