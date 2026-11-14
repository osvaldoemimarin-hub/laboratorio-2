"""Bronze -> Silver: limpieza y estandarización de las detecciones FIRMS."""
from __future__ import annotations

import pandas as pd

# La API devuelve 'l','n','h' y el feed público 'low','nominal','high'.
MAPA_CONFIANZA = {
    "l": "baja", "low": "baja",
    "n": "nominal", "nominal": "nominal",
    "h": "alta", "high": "alta",
}

COLUMNAS_SILVER = [
    "id_deteccion", "fecha_hora_utc", "fecha", "latitud", "longitud",
    "brillo_ti4", "brillo_ti5", "scan", "track", "frp",
    "periodo", "satelite", "confianza", "version",
]

LLAVE_NATURAL = ["latitud", "longitud", "fecha_hora_utc", "satelite"]


def limpieza_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    hora = df["acq_time"].astype(int).astype(str).str.zfill(4)
    df["fecha_hora_utc"] = pd.to_datetime(
        df["acq_date"].astype(str) + " " + hora, format="%Y-%m-%d %H%M", utc=True
    )

    df = df.rename(columns={
        "latitude": "latitud", "longitude": "longitud",
        "bright_ti4": "brillo_ti4", "bright_ti5": "brillo_ti5",
        "satellite": "satelite",
    })
    df["confianza"] = df["confidence"].astype(str).str.strip().str.lower().map(MAPA_CONFIANZA)
    df["periodo"] = df["daynight"].map({"D": "dia", "N": "noche"})
    df["fecha"] = df["fecha_hora_utc"].dt.date.astype(str)
    df["version"] = df["version"].astype(str)

    # Reglas de calidad: sin nulos en campos clave y valores físicamente plausibles.
    df = df.dropna(subset=["latitud", "longitud", "brillo_ti4", "brillo_ti5", "frp", "confianza", "periodo"])
    df = df[(df["brillo_ti4"] > 0) & (df["brillo_ti5"] > 0) & (df["frp"] >= 0)]

    # La misma detección puede venir en varios snapshots de Bronze.
    df = df.sort_values("fecha_hora_utc").drop_duplicates(subset=LLAVE_NATURAL, keep="last")

    df["id_deteccion"] = (
        df["satelite"].astype(str) + "_"
        + df["fecha_hora_utc"].dt.strftime("%Y%m%d%H%M") + "_"
        + df["latitud"].round(5).astype(str) + "_"
        + df["longitud"].round(5).astype(str)
    )
    return df[COLUMNAS_SILVER].reset_index(drop=True)


def unir_snapshots(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Integra todos los snapshots de Bronze en una sola tabla Silver sin duplicados."""
    return limpieza_data(pd.concat(dfs, ignore_index=True))
