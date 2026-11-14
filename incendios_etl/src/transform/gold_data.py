"""Silver -> Gold: tablas listas para análisis y para el modelo."""
from __future__ import annotations

import numpy as np
import pandas as pd


def _en_region(df: pd.DataFrame, region: dict) -> pd.Series:
    return (
        df["latitud"].between(region["lat_min"], region["lat_max"])
        & df["longitud"].between(region["lon_min"], region["lon_max"])
    )


def integrar_datos(silver: pd.DataFrame, cfg: dict) -> dict[str, pd.DataFrame]:
    df = silver.copy()
    df["fecha_hora_utc"] = pd.to_datetime(df["fecha_hora_utc"], utc=True)

    # Variables derivadas (feature engineering)
    df["hora_utc"] = df["fecha_hora_utc"].dt.hour
    df["dif_brillo"] = df["brillo_ti4"] - df["brillo_ti5"]
    df["es_noche"] = (df["periodo"] == "noche").astype(int)
    df["celda_lat"] = np.floor(df["latitud"]).astype(int)
    df["celda_lon"] = np.floor(df["longitud"]).astype(int)
    df["celda_id"] = df["celda_lat"].astype(str) + "_" + df["celda_lon"].astype(str)
    df["en_region_interes"] = _en_region(df, cfg["region_interes"]).astype(int)

    # Resumen diario por celda de 1° x 1° (≈ 110 km)
    resumen = (
        df.groupby(["fecha", "celda_id", "celda_lat", "celda_lon"], as_index=False)
        .agg(
            n_detecciones=("id_deteccion", "count"),
            frp_total=("frp", "sum"),
            frp_medio=("frp", "mean"),
            frp_max=("frp", "max"),
            pct_alta_confianza=("confianza", lambda s: float((s == "alta").mean())),
            en_region_interes=("en_region_interes", "max"),
        )
        .sort_values(["fecha", "n_detecciones"], ascending=[True, False])
    )

    df["fecha_hora_utc"] = df["fecha_hora_utc"].dt.strftime("%Y-%m-%dT%H:%MZ")
    return {"detecciones": df.reset_index(drop=True), "resumen_diario": resumen.reset_index(drop=True)}
