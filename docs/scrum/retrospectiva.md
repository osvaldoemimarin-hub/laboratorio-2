# Retrospectiva

## ¿Qué salió bien?
- **Separar procesamiento y orquestación.** Las etapas viven en `src/pipeline.py` y el DAG solo las llama. Esto permitió desarrollar y probar con `python main.py` en segundos antes de tocar Airflow.
- **Las capas Medallion dieron trazabilidad.** Cada descarga queda en Bronze sin modificar, y la deduplicación de Silver hizo el pipeline idempotente: 4 ejecuciones, 61.689 detecciones únicas.
- **La validación del esquema** evitó que una respuesta de error de la API (texto plano) se guardara como si fueran datos.
- **El análisis exploratorio previo al modelo** reveló que la confianza "alta" equivale a saturación del sensor. Así interpretamos bien el resultado, sin presentar un 0,94 de F1 como un logro mayor de lo que es.

## ¿Qué se puede mejorar?
- **Ventana corta:** el feed público solo cubre 7 días, así que el histórico depende de que el DAG corra a diario. Conviene solicitar la `MAP_KEY` y descargar más días desde el inicio.
- **Elección del objetivo:** se eligió una variable objetivo que el algoritmo de NASA ya calcula. Un objetivo de negocio (persistencia del fuego, PB-13) habría aportado más.
- **Advertencias de entorno:** al usar Git Bash en Windows, `docker compose exec` convierte las rutas (`/opt/...` → `C:/Program Files/Git/opt/...`). Se resolvió con `MSYS_NO_PATHCONV=1`. Conviene documentarlo para el equipo.
- **Versiones:** scikit-learn difiere entre el entorno local y la imagen de Airflow, y eso cambia levemente las métricas (F1 del RF 0,932 local vs 0,936 en Airflow). Hay que fijar versiones en ambos `requirements`.

## Acciones para el próximo sprint
1. Fijar versiones exactas de dependencias en `requirements.txt` y `requirements-airflow.txt`.
2. Solicitar la `FIRMS_MAP_KEY` y ampliar la ventana histórica.
3. Implementar PB-13 (persistencia del fuego) usando el resumen diario por celda de la capa Gold.
