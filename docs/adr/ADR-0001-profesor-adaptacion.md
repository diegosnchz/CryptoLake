# ADR-0001: Adaptacion de requisitos del profesor al dominio actual

## Estado
Aceptado

## Contexto
- El material del profesor (fases 1-6) usa tablas tipo `daily_prices` y `fear_greed`.
- Este repositorio ya esta avanzado en dominio de futuros en tiempo real:
  - Bronze: `cryptolake.bronze.futures_trades`
  - Silver: `cryptolake.silver.ohlcv_1m`
  - Gold PySpark existente: `cryptolake.gold.futures_daily_stats`
- Se pidio integrar faltantes sin romper arquitectura actual ni renombrar por capricho.

## Decision
1. Mantener nombres y pipeline existente de streaming a Bronze y batch a Silver.
2. Implementar la ruta profesor de dbt sobre tablas reales del alumno:
   - Source dbt: `silver.ohlcv_1m`
   - Gold dbt nuevo: `gold.fact_ohlcv_1m`, `gold.dim_symbols`, `gold.dim_dates`
3. Conservar el Gold PySpark existente (`silver_to_gold_daily.py`) por compatibilidad hacia atras.
4. Crear DAG maestro `cryptolake_full_pipeline` con paso Bronze opcional por variable:
   - `RUN_BRONZE_AVAILABLE_NOW=true` para ejecutar `available-now`
   - Default `false` para evitar interferir con stream continuo.
5. Agregar Spark Thrift (`spark-thrift`, puerto 10000) como capa adicional, no sustitutiva.

## Consecuencias
- Cumple evaluacion de fases 5-6 sin romper comandos y flujos actuales.
- Conviven dos rutas Gold:
  - Ruta actual: PySpark (`futures_daily_stats`)
  - Ruta profesor: dbt star schema sobre OHLCV (`fact_ohlcv_1m` + dimensiones)
- El README documenta ambas rutas para uso normal y para evaluacion.
