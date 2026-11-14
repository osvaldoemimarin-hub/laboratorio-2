"""Gold -> Almacén de datos (SQLite).

Modelo en estrella sencillo:
  fact_detecciones   una fila por detección de incendio
  dim_fecha          calendario de las fechas presentes
  agg_diario_celda   agregado diario por celda de 1°
Cada carga reemplaza las tablas (la capa Silver ya acumula el histórico).
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from src.config import WAREHOUSE_DB


def construir_dim_fecha(fechas: pd.Series) -> pd.DataFrame:
    f = pd.to_datetime(pd.Series(sorted(fechas.unique())))
    return pd.DataFrame({
        "fecha": f.dt.strftime("%Y-%m-%d"),
        "anio": f.dt.year,
        "mes": f.dt.month,
        "dia": f.dt.day,
        "dia_semana": f.dt.dayofweek,
        "semana_iso": f.dt.isocalendar().week.astype(int).values,
    })


def cargar_datos(gold: dict[str, pd.DataFrame], db_path: Path = WAREHOUSE_DB) -> dict[str, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    detecciones = gold["detecciones"]
    tablas = {
        "fact_detecciones": detecciones,
        "dim_fecha": construir_dim_fecha(detecciones["fecha"]),
        "agg_diario_celda": gold["resumen_diario"],
    }
    with sqlite3.connect(db_path) as conn:
        for nombre, df in tablas.items():
            df.to_sql(nombre, conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fact_fecha ON fact_detecciones(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fact_celda ON fact_detecciones(celda_id)")
        conteos = {
            n: conn.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0] for n in tablas
        }
    print(f"Almacén actualizado en {db_path}: {conteos}")
    return conteos
