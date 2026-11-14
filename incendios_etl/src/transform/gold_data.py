"""Silver -> Gold: tablas listas para análisis, KPIs de negocio y datos para el modelo."""
from __future__ import annotations

import numpy as np
import pandas as pd


def _detecciones_enriquecidas(silver: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = silver.copy()
    df["fecha_hora_utc"] = pd.to_datetime(df["fecha_hora_utc"], utc=True)
    df["hora_utc"] = df["fecha_hora_utc"].dt.hour
    df["dif_brillo"] = df["brillo_ti4"] - df["brillo_ti5"]
    df["es_noche"] = (df["periodo"] == "noche").astype(int)
    df["celda_lat"] = np.floor(df["latitud"]).astype(int)
    df["celda_lon"] = np.floor(df["longitud"]).astype(int)
    df["celda_id"] = df["celda_lat"].astype(str) + "_" + df["celda_lon"].astype(str)
    df["en_region_interes"] = (df["pais_iso3"] == cfg["region_interes"]["iso3"]).astype(int)
    return df


def _resumen_diario(df: pd.DataFrame) -> pd.DataFrame:
    """Resumen diario por celda de 1° x 1° (≈ 110 km)."""
    return (
        df.groupby(["fecha", "celda_id", "celda_lat", "celda_lon"], as_index=False)
        .agg(
            n_detecciones=("id_deteccion", "count"),
            frp_total=("frp", "sum"),
            frp_medio=("frp", "mean"),
            frp_max=("frp", "max"),
            pct_alta_confianza=("confianza", lambda s: float((s == "alta").mean())),
            en_region_interes=("en_region_interes", "max"),
        )
        .sort_values(["fecha", "n_detecciones", "celda_id"], ascending=[True, False, True])
        .reset_index(drop=True)
    )


def kpis_por_pais(df: pd.DataFrame) -> pd.DataFrame:
    """gold_final: un registro por país con indicadores de negocio (tema Green Data).

    Solo entran los países con dato de bosque (resultado del JOIN con el Excel).
    """
    base = df[df["tiene_dato_bosque"] == 1]
    total = len(base)
    k = (
        base.groupby(["pais_iso3", "pais_nombre", "anio_bosque", "area_bosque_km2"], as_index=False)
        .agg(
            n_detecciones=("id_deteccion", "count"),
            dias_con_fuego=("fecha", "nunique"),
            celdas_afectadas=("celda_id", "nunique"),
            frp_total_mw=("frp", "sum"),
            frp_medio_mw=("frp", "mean"),
            pct_alta_confianza=("confianza", lambda s: (s == "alta").mean() * 100),
            pct_nocturnas=("es_noche", lambda s: s.mean() * 100),
        )
    )
    k["anio_bosque"] = k["anio_bosque"].astype(int)
    k["detecciones_por_1000km2_bosque"] = k["n_detecciones"] / k["area_bosque_km2"] * 1000
    k["frp_por_1000km2_bosque"] = k["frp_total_mw"] / k["area_bosque_km2"] * 1000
    k["participacion_pct"] = k["n_detecciones"] / total * 100
    k["ranking_presion_bosque"] = (
        k["detecciones_por_1000km2_bosque"].rank(ascending=False, method="min").astype(int)
    )
    k = k.sort_values("ranking_presion_bosque").reset_index(drop=True)
    redondeo = {c: 2 for c in k.columns if k[c].dtype.kind == "f"}
    return k.round(redondeo)


def integrar_datos(silver_integrado: pd.DataFrame, cfg: dict) -> dict[str, pd.DataFrame]:
    df = _detecciones_enriquecidas(silver_integrado, cfg)
    resumen = _resumen_diario(df)
    kpis = kpis_por_pais(df)
    df["fecha_hora_utc"] = df["fecha_hora_utc"].dt.strftime("%Y-%m-%dT%H:%MZ")
    return {
        "detecciones": df.reset_index(drop=True),
        "resumen_diario": resumen,
        "kpis_pais": kpis,
    }
