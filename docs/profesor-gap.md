# GAP nuevo (solo deltas) — Profesor vs Alumno

## Base de comparación
- Versión de referencia del profesor: `ce-ia-big-data-main` pinneada en `docs/profesor-version.md`.
- Requisitos re-ingeridos en: `docs/profesor-requisitos.md`.
- Este GAP incluye solo diferencias vigentes (no rehace cerrados).

## Matriz de deltas vigentes

| Requisito (profesor) | Estado actual | Acción concreta mínima | Archivo(s) a tocar | Criterio de aceptación (comando + output esperado) |
|---|---|---|---|---|
| Implementar carga batch canónica Bronze vía `api_to_bronze.py` con tablas `historical_prices` y `fear_greed` | Parcial | Añadir ruta batch del profesor sin reemplazar flujo streaming actual | `src/processing/batch/api_to_bronze.py`, `Makefile` | `docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/work/src/processing/batch/api_to_bronze.py` → salida con cargas en `cryptolake.bronze.historical_prices` y `cryptolake.bronze.fear_greed` |
| Make medallion canónico (`bronze-load`, `silver-transform`, `gold-transform`, `pipeline`) alineado al batch del profesor | Parcial | Agregar/normalizar targets manteniendo targets actuales | `Makefile` | `make pipeline` → ejecuta secuencialmente `bronze-load`, `silver-transform`, `gold-transform` sin error |
| Bronze→Silver batch con lógica de limpieza/dedup del guion del profesor sobre su dominio | Parcial | Incorporar script batch canónico compatible con tablas del profesor (en paralelo al 1m existente) | `src/processing/batch/bronze_to_silver.py` (o variante dedicada) | `docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/work/src/processing/batch/bronze_to_silver.py` → muestra `MERGE INTO` exitoso y registros en `cryptolake.silver.daily_prices` |

## Conclusión de delta
- No hay deltas nuevos en fases 5-6.
- Los deltas vigentes se concentran en la implementación batch canónica de fases 3-4 del profesor.
