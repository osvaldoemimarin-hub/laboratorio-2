# Sprints

## Sprint 1: Extracción y capa Bronze

**Objetivo:** obtener datos reales de NASA FIRMS de forma automática y verificable.

| Tarea | Historia | Responsable | Resultado |
|---|---|---|---|
| Crear repositorio y estructura Medallion/TDSP | PB-00 | Osvaldo | `incendios_etl/` (bronze, silver, gold, warehouse) + `airflow-incendios/` |
| Explorar las fuentes de FIRMS | PB-01 | Nilson | La API de área necesita `MAP_KEY`; el feed público de 7 días no. VIIRS S-NPP llega vacío; se usa **NOAA-20** |
| Implementar `extract_firms.py` con API y feed como respaldo | PB-01 | Nilson | 61.689 detecciones descargadas |
| Guardar snapshots con fecha y hora | PB-02 | Nilson | `firms_viirs_noaa20_feed_<UTC>.csv` |
| Reglas de calidad de Bronze | PB-03 | Nilson | Columnas requeridas, ≥ 100 filas, coordenadas válidas |

**Sprint review:** se aceptan los entregables. **Hallazgo:** la API responde texto plano (no CSV) cuando la clave es inválida. Se agrega una validación del esquema antes de escribir en Bronze, con su prueba unitaria.

---

## Sprint 2: Silver, Gold, almacén y modelo

**Objetivo:** convertir Bronze en datos analíticos y responder la pregunta del PO.

| Tarea | Historia | Responsable | Resultado |
|---|---|---|---|
| Limpieza y normalización de confianza (`l/n/h` y `low/nominal/high`) | PB-04 | Nilson | `clean_firms.py` |
| Deduplicación entre snapshots | PB-04 | Nilson | Llave `(lat, lon, fecha_hora, satélite)` |
| Variables derivadas y resumen diario por celda de 1° | PB-05 | Nilson | 61.689 × 21 y 4.562 filas de resumen |
| Almacén SQLite en estrella | PB-06 | Nilson | `fact_detecciones`, `dim_fecha`, `agg_diario_celda` |
| LR vs RF con F1 macro y CV | PB-07 | Juan Andrés | LR 0,697 · RF 0,936 |
| Análisis exploratorio | PB-07 | Nilson | El 100 % de "alta" está saturado en I4; "baja" solo de día |

**Sprint review:** a diferencia del Laboratorio 1, el Random Forest **sí** supera claramente al baseline (+0,24 de F1). El PO acepta recomendarlo. El equipo advierte que la confianza la calcula el algoritmo de NASA con las mismas variables. Se agrega al backlog PB-13, un objetivo de negocio con valor propio.

---

## Sprint 3: Orquestación con Airflow y cierre

**Objetivo:** que el pipeline corra solo, con observabilidad, y que cualquiera pueda reproducirlo.

| Tarea | Historia | Responsable | Resultado |
|---|---|---|---|
| WSL2 + Docker Desktop verificados | PB-08 | Osvaldo | Docker 29.9, Compose v5.6 |
| `docker-compose.yaml` oficial 3.3.2 + `build: .` + montaje del proyecto | PB-08 | Osvaldo | Imagen `airflow-incendios:3.3.2` |
| DAG `incendios_firms_medallion` (6 tareas, reintentos) | PB-08 | Osvaldo | Sin errores de importación |
| Programación diaria a las 06:00 UTC | PB-09 | Osvaldo | La ejecución programada corrió con éxito |
| Pruebas con pytest | PB-10 | Nilson / Juan Andrés | 7/7 aprobadas (incluye reproducibilidad de Silver) |
| README, reporte final y artefactos Scrum | PB-11 | Osvaldo / equipo | `docs/` |

**Sprint review:** 4 ejecuciones del DAG, todas en `success`. Bronze acumuló 4 snapshots y Silver se mantuvo en 61.689 detecciones únicas: se confirma la idempotencia. El incremento queda aceptado.

---

## Sprint 4: Segunda fuente, JOIN y KPIs (enunciado oficial "Green Data")

**Objetivo:** cumplir el enunciado oficial, que pide integrar una API REST y un archivo Excel/CSV con un JOIN y entregar `gold_final.csv` con KPIs.

| Tarea | Historia | Responsable | Resultado |
|---|---|---|---|
| Elegir la segunda fuente "verde" | PB-15 | Edgar / equipo | Área de bosque por país, Banco Mundial (`AG.LND.FRST.K2`) |
| Extracción del Excel a Bronze con respaldo | PB-15 | Nilson | `extract_excel.py`, tarea `extraer_excel_bosque` en paralelo con la API |
| Validación y limpieza del Excel (ancho → largo, último año) | PB-15 | Nilson | 12 países, año 2025 |
| Asignar país a cada detección con los límites reales | PB-12, PB-16 | Nilson | Natural Earth + `shapely`; Colombia pasa de 1.805 (caja) a 1.026 detecciones |
| LEFT JOIN por `pais_iso3` | PB-16 | Nilson | 98,5 % de las detecciones con dato de bosque |
| KPIs por país en `gold_final.csv` y tabla `kpi_pais` | PB-17 | Juan Andrés / Nilson | Ranking de presión del fuego sobre el bosque |
| Pruebas de la nueva fuente y del JOIN | PB-15, PB-16 | Juan Andrés | 12/12 aprobadas |
| Imagen de Airflow con shapely/xlrd/openpyxl y DAG de 7 tareas | PB-15 | Osvaldo | `success` |

**Sprint review:** el JOIN cambia la lectura del problema. Brasil tiene el 57 % de las detecciones, pero **Guyana y Paraguay** sufren la mayor presión sobre su bosque. Se agrega PB-18 (temporada completa) para que los KPIs no dependan de una sola semana.
