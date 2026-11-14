# Product Backlog: Laboratorio 2 (Incendios NASA FIRMS + Airflow)

| Rol | Integrante | Responsabilidades |
|---|---|---|
| Product Owner | Edgar Adrian Ortiz | Define objetivos, prioriza el backlog, valida la entrega final |
| Scrum Master | Osvaldo Emilio Marin | Coordina reuniones, mantiene el tablero, README y flujo Git |
| Data Engineer / Analyst | Nilson Jacobo Gonzales | Descarga, calidad, limpieza y EDA |
| ML Engineer | Juan Andrés Ruiz | Entrenamiento, evaluación y despliegue del modelo |


**Visión:** disponer de un pipeline diario y automático que convierta las detecciones satelitales de incendios de NASA en datos confiables y consultables, con un modelo que explique su nivel de confianza.

| ID | Historia de usuario | Prioridad | Puntos | Sprint | Estado |
|---|---|---|---|---|---|
| PB-00 | Como equipo, quiero un repositorio con estructura Medallion + TDSP para trabajar ordenados | Alta | 2 | 1 | Hecho |
| PB-01 | Como analista, quiero descargar automáticamente las detecciones de FIRMS (API o feed público) para no hacerlo a mano | Alta | 5 | 1 | Hecho |
| PB-02 | Como analista, quiero que cada descarga quede guardada sin cambios (Bronze) para poder auditarla | Alta | 2 | 1 | Hecho |
| PB-03 | Como analista, quiero que el pipeline se detenga si los datos llegan mal (validación) para no contaminar el almacén | Alta | 3 | 1 | Hecho |
| PB-04 | Como analista, quiero datos limpios y sin duplicados entre descargas (Silver) | Alta | 5 | 2 | Hecho |
| PB-05 | Como analista, quiero variables derivadas y resúmenes por celda y día (Gold) | Media | 5 | 2 | Hecho |
| PB-06 | Como usuario de BI, quiero consultar los datos en un almacén con modelo en estrella | Media | 3 | 2 | Hecho |
| PB-07 | Como PO, quiero un modelo baseline y uno alternativo comparados con F1 macro | Media | 5 | 2 | Hecho |
| PB-08 | Como equipo, quiero Airflow en Docker/WSL2 orquestando todas las etapas con reintentos y logs | Alta | 8 | 3 | Hecho |
| PB-09 | Como PO, quiero que el pipeline corra solo todos los días | Media | 1 | 3 | Hecho |
| PB-10 | Como equipo, quiero pruebas automáticas del pipeline | Media | 3 | 3 | Hecho |
| PB-11 | Como PO, quiero un reporte final y la documentación para reproducir el proyecto | Media | 3 | 3 | Hecho |
| PB-12 | Como analista, quiero el polígono oficial de Colombia en lugar de una caja aproximada | Baja | 3 | 4 | Hecho (límites Natural Earth) |
| PB-13 | Como PO, quiero predecir la persistencia del fuego por celda (objetivo con valor real) | Baja | 8 | — | Pendiente |
| PB-14 | Como equipo, quiero cargar el almacén en PostgreSQL usando una Connection de Airflow | Baja | 5 | — | Pendiente |
| PB-15 | Como PO, quiero integrar un archivo Excel/CSV como segunda fuente (requisito del enunciado) | Alta | 3 | 4 | Hecho (Excel de bosque, Banco Mundial) |
| PB-16 | Como analista, quiero unir las dos fuentes con un JOIN en Silver | Alta | 5 | 4 | Hecho (país de cada detección + LEFT JOIN) |
| PB-17 | Como PO, quiero KPIs de negocio en `gold_final.csv` para comparar países (tema Green Data) | Alta | 5 | 4 | Hecho |
| PB-18 | Como analista, quiero cargar una temporada completa con consultas por fecha a la API | Media | 5 | — | Pendiente |
