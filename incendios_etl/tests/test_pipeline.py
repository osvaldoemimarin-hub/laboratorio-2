import sqlite3

import pandas as pd
import pytest

from src.config import cargar_config
from src.extract.extract_firms import construir_url, validar_csv_firms
from src.load.load_datawarehouse import cargar_datos
from src.transform.clean_firms import limpieza_data, unir_snapshots
from src.transform.gold_data import integrar_datos

CFG = cargar_config()


def bronze_ejemplo() -> pd.DataFrame:
    return pd.DataFrame({
        "latitude": [4.5, 4.5, -10.2, 5.1],
        "longitude": [-72.1, -72.1, -55.3, -70.0],
        "bright_ti4": [340.1, 340.1, 367.0, 310.0],
        "scan": [0.4, 0.4, 0.5, 0.39],
        "track": [0.36, 0.36, 0.5, 0.44],
        "acq_date": ["2026-11-10", "2026-11-10", "2026-11-11", "2026-11-11"],
        "acq_time": [608, 608, 1735, 545],
        "satellite": ["N20"] * 4,
        "confidence": ["nominal", "nominal", "h", "low"],
        "version": ["2.0NRT"] * 4,
        "bright_ti5": [290.0, 290.0, 305.2, None],
        "frp": [5.2, 5.2, 80.4, 1.1],
        "daynight": ["N", "N", "D", "N"],
    })


def test_limpieza_normaliza_y_deduplica():
    silver = limpieza_data(bronze_ejemplo())
    # 1 duplicado exacto y 1 fila con brillo_ti5 nulo se eliminan
    assert len(silver) == 2
    assert set(silver["confianza"]) == {"nominal", "alta"}
    assert silver.loc[silver["confianza"] == "nominal", "fecha_hora_utc"].iloc[0].strftime("%H:%M") == "06:08"
    assert silver["id_deteccion"].is_unique


def test_unir_snapshots_no_duplica_entre_descargas():
    b = bronze_ejemplo()
    assert len(unir_snapshots([b, b])) == len(limpieza_data(b))


def test_silver_es_reproducible_sin_importar_el_orden():
    b = bronze_ejemplo()
    # Dos detecciones distintas con la misma fecha y hora (el caso que antes desordenaba Silver)
    extra = b.iloc[[2]].assign(latitude=-10.3, longitude=-55.4)
    base = pd.concat([b, extra], ignore_index=True)
    esperado = limpieza_data(base)
    for semilla in range(5):
        revuelto = base.sample(frac=1, random_state=semilla).reset_index(drop=True)
        pd.testing.assert_frame_equal(limpieza_data(revuelto), esperado)
    pd.testing.assert_frame_equal(unir_snapshots([extra, b]), unir_snapshots([b, extra]))


def test_gold_agrega_por_celda_y_marca_region():
    gold = integrar_datos(limpieza_data(bronze_ejemplo()), CFG)
    det, res = gold["detecciones"], gold["resumen_diario"]
    assert {"dif_brillo", "hora_utc", "es_noche", "celda_id"} <= set(det.columns)
    assert det.loc[det["latitud"] == 4.5, "en_region_interes"].iloc[0] == 1
    assert det.loc[det["latitud"] == -10.2, "en_region_interes"].iloc[0] == 0
    assert res["n_detecciones"].sum() == len(det)


def test_carga_almacen(tmp_path):
    gold = integrar_datos(limpieza_data(bronze_ejemplo()), CFG)
    db = tmp_path / "dw.db"
    conteos = cargar_datos(gold, db)
    assert conteos["fact_detecciones"] == 2
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM dim_fecha").fetchone()[0] == 2


def test_url_api_y_feed():
    url, origen = construir_url(CFG, None)
    assert origen == "feed" and url.endswith(".csv")
    url, origen = construir_url(CFG, "CLAVE")
    assert origen == "api" and "/CLAVE/VIIRS_NOAA20_NRT/" in url


def test_respuesta_invalida_de_api_falla():
    with pytest.raises(ValueError):
        validar_csv_firms("Invalid MAP_KEY.", CFG["calidad"]["columnas_requeridas"])
