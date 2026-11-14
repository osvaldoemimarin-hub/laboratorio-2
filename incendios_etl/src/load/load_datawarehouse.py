"""Gold -> Almacén de datos (SQLite).

Modelo en estrella sencillo:
  fact_detecciones   una fila por detección de incendio (con su país)
  dim_fecha          calendario de las fechas presentes
  dim_pais           país con su área de bosque (Excel del Banco Mundial)
  agg_diario_celda   agregado diario por celda de 1°
  kpi_pais           indicadores de negocio por país (= gold_final.csv)
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
        "dia_semana": f.dt.day_of_week,
        "semana_iso": f.dt.isocalendar().week.astype(int).values,
    })


def cargar_datos(gold: dict[str, pd.DataFrame], db_path: Path = WAREHOUSE_DB) -> dict[str, int]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    detecciones = gold["detecciones"]
    kpis = gold["kpis_pais"]
    tablas = {
        "fact_detecciones": detecciones,
        "dim_fecha": construir_dim_fecha(detecciones["fecha"]),
        "dim_pais": kpis[["pais_iso3", "pais_nombre", "anio_bosque", "area_bosque_km2"]],
        "agg_diario_celda": gold["resumen_diario"],
        "kpi_pais": kpis,
    }
    with sqlite3.connect(db_path) as conn:
        for nombre, df in tablas.items():
            df.to_sql(nombre, conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fact_fecha ON fact_detecciones(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fact_celda ON fact_detecciones(celda_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_fact_pais ON fact_detecciones(pais_iso3)")
        conteos = {
            n: conn.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0] for n in tablas
        }
    print(f"Almacén actualizado en {db_path}: {conteos}")
    return conteos
