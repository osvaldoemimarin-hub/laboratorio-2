"""Silver: limpieza del Excel de bosque, asignación de país a cada detección y JOIN
entre las dos fuentes (API FIRMS + Excel del Banco Mundial)."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from shapely import STRtree

from src.config import PROJECT_DIR

SIN_PAIS = "SIN_PAIS"


def limpieza_bosque(df: pd.DataFrame, paises: list[str]) -> pd.DataFrame:
    """Excel ancho (una columna por año) -> una fila por país con el último dato disponible."""
    columnas_anio = [c for c in df.columns if str(c).strip().isdigit()]
    largo = df.melt(
        id_vars=["Country Name", "Country Code"], value_vars=columnas_anio,
        var_name="anio", value_name="area_bosque_km2",
    )
    largo = largo.rename(columns={"Country Name": "pais_nombre_bm", "Country Code": "pais_iso3"})
    largo["pais_iso3"] = largo["pais_iso3"].astype(str).str.strip().str.upper()
    largo["anio"] = largo["anio"].astype(int)
    largo["area_bosque_km2"] = pd.to_numeric(largo["area_bosque_km2"], errors="coerce")

    # Calidad: solo países de interés, sin nulos ni valores no positivos, sin duplicados.
    largo = largo[largo["pais_iso3"].isin(paises)]
    largo = largo.dropna(subset=["area_bosque_km2"])
    largo = largo[largo["area_bosque_km2"] > 0].drop_duplicates(subset=["pais_iso3", "anio"])

    ultimo = (
        largo.sort_values(["pais_iso3", "anio"])
        .groupby("pais_iso3", as_index=False)
        .last()
        .rename(columns={"anio": "anio_bosque"})
    )
    faltantes = sorted(set(paises) - set(ultimo["pais_iso3"]))
    if faltantes:
        print(f"Aviso: sin dato de bosque para {faltantes}")
    return ultimo[["pais_iso3", "pais_nombre_bm", "anio_bosque", "area_bosque_km2"]].reset_index(drop=True)


@lru_cache(maxsize=2)
def _cargar_limites(ruta: str, campo_iso3: str, campo_nombre: str):
    with open(ruta, encoding="utf-8") as f:
        features = json.load(f)["features"]
    geoms = [shapely.geometry.shape(ft["geometry"]) for ft in features]
    iso3 = np.array([ft["properties"].get(campo_iso3) for ft in features], dtype=object)
    nombres = np.array([ft["properties"].get(campo_nombre) for ft in features], dtype=object)
    return STRtree(geoms), iso3, nombres


def asignar_pais(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Agrega pais_iso3 y pais_nombre según el polígono del país que contiene cada punto."""
    lim = cfg["limites_paises"]
    arbol, iso3, nombres = _cargar_limites(
        str(PROJECT_DIR / lim["archivo"]), lim["campo_iso3"], lim["campo_nombre"],
    )
    puntos = shapely.points(df["longitud"].to_numpy(), df["latitud"].to_numpy())
    idx_punto, idx_pais = arbol.query(puntos, predicate="within")
    # Si un punto cae en dos polígonos (fronteras), se queda el primero de forma determinista.
    _, primero = np.unique(idx_punto, return_index=True)
    idx_punto, idx_pais = idx_punto[primero], idx_pais[primero]

    pais = np.full(len(df), SIN_PAIS, dtype=object)
    nombre = np.full(len(df), "Sin país (mar o fuera de límites)", dtype=object)
    pais[idx_punto] = iso3[idx_pais]
    nombre[idx_punto] = nombres[idx_pais]
    out = df.copy()
    out["pais_iso3"] = pais
    out["pais_nombre"] = nombre
    return out


def unir_fuentes(detecciones: pd.DataFrame, bosque: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """LEFT JOIN detecciones (API) con bosque por país (Excel) usando pais_iso3."""
    con_pais = asignar_pais(detecciones, cfg)
    integrado = con_pais.merge(bosque, on="pais_iso3", how="left", validate="many_to_one")
    integrado["tiene_dato_bosque"] = integrado["area_bosque_km2"].notna().astype(int)
    integrado["anio_bosque"] = integrado["anio_bosque"].astype("Int64")  # entero con vacíos
    return integrado.sort_values("id_deteccion", kind="mergesort").reset_index(drop=True)
