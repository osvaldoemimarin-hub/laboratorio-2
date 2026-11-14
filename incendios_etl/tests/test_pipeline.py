import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from src.config import cargar_config
from src.extract.extract_excel import es_excel
from src.extract.extract_firms import construir_url, validar_csv_firms
from src.load.load_datawarehouse import cargar_datos
from src.transform.clean_firms import limpieza_data, unir_snapshots
from src.transform.gold_data import integrar_datos
from src.transform.integrar_fuentes import SIN_PAIS, limpieza_bosque, unir_fuentes
from src.transform.validate_bronze import validar_bronze_excel

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


def excel_bosque_ejemplo() -> pd.DataFrame:
    """Imita la hoja 'Data' del Excel del Banco Mundial (formato ancho, un año por columna)."""
    return pd.DataFrame({
        "Country Name": ["Colombia", "Brazil", "Colombia", "Germany"],
        "Country Code": ["COL", "BRA", "COL", "DEU"],
        "Indicator Name": ["Forest area (sq. km)"] * 4,
        "Indicator Code": ["AG.LND.FRST.K2"] * 4,
        "2022": [598567.1, 4958547.8, 598567.1, 114190.0],
        "2023": [597234.3, None, 597234.3, 114190.0],
    })


def integrado_ejemplo() -> pd.DataFrame:
    bosque = limpieza_bosque(excel_bosque_ejemplo(), ["COL", "BRA"])
    return unir_fuentes(limpieza_data(bronze_ejemplo()), bosque, CFG)


def test_limpieza_bosque_toma_ultimo_anio_y_filtra():
    b = limpieza_bosque(excel_bosque_ejemplo(), ["COL", "BRA"])
    assert list(b["pais_iso3"]) == ["BRA", "COL"]          # DEU fuera, COL sin duplicar
    assert b.set_index("pais_iso3").loc["COL", "anio_bosque"] == 2023
    assert b.set_index("pais_iso3").loc["BRA", "anio_bosque"] == 2022   # 2023 era nulo


def test_join_asigna_pais_y_bosque():
    integ = integrado_ejemplo()
    fila_col = integ[integ["latitud"] == 4.5].iloc[0]       # punto en Colombia
    fila_bra = integ[integ["latitud"] == -10.2].iloc[0]     # punto en Brasil
    assert fila_col["pais_iso3"] == "COL" and fila_col["area_bosque_km2"] == 597234.3
    assert fila_bra["pais_iso3"] == "BRA" and fila_bra["tiene_dato_bosque"] == 1


def test_punto_en_el_mar_queda_sin_pais():
    mar = limpieza_data(bronze_ejemplo()).assign(latitud=0.0, longitud=-100.0).iloc[[0]]
    integ = unir_fuentes(mar, limpieza_bosque(excel_bosque_ejemplo(), ["COL"]), CFG)
    assert integ["pais_iso3"].iloc[0] == SIN_PAIS and integ["tiene_dato_bosque"].iloc[0] == 0


def test_validar_excel_detecta_indicador_equivocado():
    malo = excel_bosque_ejemplo().assign(**{"Indicator Code": "OTRO"})
    with pytest.raises(ValueError):
        validar_bronze_excel(Path("x.xls"), malo, CFG)


def test_gold_kpis_y_region():
    gold = integrar_datos(integrado_ejemplo(), CFG)
    det, res, kpis = gold["detecciones"], gold["resumen_diario"], gold["kpis_pais"]
    assert {"dif_brillo", "hora_utc", "es_noche", "celda_id"} <= set(det.columns)
    assert det.loc[det["latitud"] == 4.5, "en_region_interes"].iloc[0] == 1
    assert det.loc[det["latitud"] == -10.2, "en_region_interes"].iloc[0] == 0
    assert res["n_detecciones"].sum() == len(det)
    col = kpis.set_index("pais_iso3").loc["COL"]
    assert col["n_detecciones"] == 1
    assert col["detecciones_por_1000km2_bosque"] == round(1 / 597234.3 * 1000, 2)
    assert kpis["participacion_pct"].sum() == pytest.approx(100)


def test_carga_almacen(tmp_path):
    gold = integrar_datos(integrado_ejemplo(), CFG)
    db = tmp_path / "dw.db"
    conteos = cargar_datos(gold, db)
    assert conteos["fact_detecciones"] == 2
    assert conteos["kpi_pais"] == 2 and conteos["dim_pais"] == 2
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


def test_reconoce_archivos_excel():
    assert es_excel(b"\xd0\xcf\x11\xe0resto") and es_excel(b"PK\x03\x04resto")
    assert not es_excel(b"<html>error</html>")
