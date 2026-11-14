# Laboratorio 2 — Pipeline ETL Medallion de incendios forestales con Apache Airflow

**Curso:** Gestión de Proyectos de IA — Universidad Autónoma de Occidente
**Fuente de datos:** NASA FIRMS (Fire Information for Resource Management System), parte de [NASA Earthdata](https://www.earthdata.nasa.gov/data/tools/firms)

Este laboratorio retoma el flujo del Laboratorio 1 (CRISP-DM + TDSP + Scrum ML), con dos cambios:

1. **Los datos vienen de una API**: detecciones de incendios activos del sensor VIIRS (satélite NOAA-20) sobre Sudamérica, no de un archivo estático.
2. **Apache Airflow orquesta el pipeline**: corre en Docker sobre WSL2 y sigue la arquitectura Medallion **Bronze → Silver → Gold → Almacén de datos**. Al final se entrena el modelo.

```
Windows ─► WSL2 ─► Docker Desktop ─► Apache Airflow 3.3.2 ─► DAG incendios_firms_medallion

extraer_firms ─► validar_bronze ─► transformar_a_silver ─► integrar_a_gold ─► cargar_datawarehouse ─► entrenar_modelo
   (API)          (calidad)          (limpieza/dedup)       (features/agreg.)     (SQLite estrella)        (LR vs RF)
```

## Estructura

```
laboratorio-2/
├── incendios_etl/                 # Proyecto ETL + ML (equivale a "medallon" en la guía)
│   ├── config/config.yaml         # fuente, región, reglas de calidad, parámetros del modelo
│   ├── data/
│   │   ├── bronze/                # snapshots crudos de la API (inmutables, uno por ejecución)
│   │   ├── silver/                # detecciones limpias y sin duplicados
│   │   ├── gold/                  # detecciones con variables derivadas + resumen diario por celda
│   │   └── warehouse/             # incendios_dw.db (SQLite)
│   ├── src/
│   │   ├── extract/extract_firms.py
│   │   ├── transform/{validate_bronze,clean_firms,gold_data,eda}.py
│   │   ├── load/load_datawarehouse.py
│   │   ├── model/train.py
│   │   └── pipeline.py            # una función por etapa (la usan main.py y el DAG)
│   ├── outputs/reports/           # métricas JSON y figuras
│   ├── tests/test_pipeline.py
│   └── main.py                    # ejecuta todo sin Airflow
├── airflow-incendios/             # Orquestador (equivale a "airflow-medallon")
│   ├── dags/incendios_etl_dag.py
│   ├── Dockerfile
│   ├── requirements-airflow.txt
│   ├── docker-compose.yaml        # oficial 3.3.2 + build . + montaje ../incendios_etl
│   └── .env.example
└── docs/
    ├── reporte_final.md
    └── scrum/                     # backlog, sprints, tablero, DoD, retrospectiva
```

## Ejecución

### Requisitos
Windows 10 2004+ / 11, WSL2, Docker Desktop con backend WSL2, mínimo 4 GB de RAM para Docker.

### 1. Con Airflow (flujo del laboratorio)

```powershell
cd airflow-incendios
copy .env.example .env          # opcional: agregar FIRMS_MAP_KEY
docker compose build
docker compose up airflow-init  # debe terminar con "exited with code 0"
docker compose up -d
```

Abrir http://localhost:8080 (usuario `airflow` / contraseña `airflow`), activar `incendios_firms_medallion` y pulsar **Trigger**. También se puede hacer desde la terminal:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags trigger incendios_firms_medallion
docker compose exec airflow-scheduler airflow dags list-runs incendios_firms_medallion
```

El DAG está programado para las **06:00 UTC todos los días** (`schedule="0 6 * * *"`, `catchup=False`). La extracción reintenta 2 veces si falla la red.

### 2. Sin Airflow (desarrollo)

```powershell
cd incendios_etl
python -m venv .venv; .venv\Scripts\activate
pip install -r requirements.txt
python main.py                 # extrae + procesa
python main.py --sin-extraer   # reprocesa lo que ya hay en Bronze
python -m pytest -q tests
```

### Clave de la API FIRMS (opcional)

Sin clave, la extracción usa el **feed público de 7 días** (no requiere registro). Con una clave gratuita (*MAP_KEY*, se solicita en https://firms.modaps.eosdis.nasa.gov/api/map_key/) se usa la **API de área**, que permite elegir la caja geográfica y el número de días. Se pone en `airflow-incendios/.env` como `FIRMS_MAP_KEY=...` y nunca se sube al repositorio.

## Resultados (ejecución en Airflow del 14-nov-2026)

| | |
|---|---|
| Detecciones únicas (7–14 nov 2026) | **61.689** en Sudamérica, de ellas 1.703 en Colombia |
| Ejecuciones del DAG | 4 (3 manuales + 1 programada), todas `success` |
| Almacén | `fact_detecciones` 61.689 · `dim_fecha` 8 · `agg_diario_celda` 4.562 |
| Regresión logística (baseline) | F1 macro **0,697** · accuracy 0,708 |
| Random Forest | F1 macro **0,936** · accuracy 0,975 · CV 0,937 ± 0,004 |
| Modelo recomendado | **Random Forest** (+0,24 F1, supera el margen mínimo de 0,01) |

El análisis completo está en [docs/reporte_final.md](docs/reporte_final.md).

## Equipo y roles Scrum

| Integrante | Rol |
|---|---|
| Osvaldo Emilio Marin | Scrum Master + ML Engineer |
| Nilson Jacobo Gonzales | Product Owner |
| Edgar Adrian Ortiz | Data Engineer / Analyst |
