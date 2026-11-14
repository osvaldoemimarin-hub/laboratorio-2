# Definition of Done

Una historia se considera terminada cuando cumple **todas** estas condiciones:

**Código y datos**
- [x] El código está en `incendios_etl/src/` y cada etapa es una función reutilizable, sin lógica dentro del DAG.
- [x] No hay rutas de Windows dentro del código. Las rutas se calculan desde `src/config.py`, así el mismo código corre en Windows y en el contenedor (`/opt/airflow/incendios_etl`).
- [x] Los parámetros (fuente, región, umbrales, semilla) están en `config/config.yaml`, no fijos en el código.
- [x] Ninguna clave o secreto se sube al repositorio (`FIRMS_MAP_KEY` va en `.env`, que está en `.gitignore`).

**Calidad**
- [x] Las pruebas pasan: `python -m pytest -q tests`.
- [x] Volver a ejecutar el pipeline no duplica datos (idempotencia).
- [x] La validación de Bronze detiene el DAG si los datos llegan mal.

**Orquestación**
- [x] `airflow dags list-import-errors` no muestra errores.
- [x] La tarea corre con `success` en Airflow y sus logs se pueden consultar desde la interfaz.

**Modelo**
- [x] Las métricas se registran en `outputs/reports/metricas_modelos.json`, con F1 macro, CV y métricas por clase.
- [x] La decisión entre modelos sigue la regla del margen mínimo de F1.

**Documentación**
- [x] El README explica cómo reproducir el proyecto, con y sin Airflow.
- [x] El PO aceptó la historia en la sprint review.
