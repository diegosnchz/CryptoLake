# ADR 0001: Kafka mode para evaluacion (KRaft vs ZooKeeper)

## Estado
Aceptado

## Contexto
- En `profesor_ref/cryptolake-fases-1-2.md` la referencia usa Kafka en modo KRaft (sin ZooKeeper).
- Este repositorio del alumno ya estaba funcionando con `cp-kafka` + `cp-zookeeper` y pipeline streaming estable.
- El objetivo de esta entrega fue cerrar fases 5-6 (Spark Thrift + dbt + Airflow) con cambios minimos y sin romper lo existente.

## Decision
- Mantener Kafka en modo ZooKeeper en esta entrega.
- No migrar ahora a KRaft para evitar riesgo de regresion en:
  - bootstrap de broker/listeners
  - productor y consumidores ya cableados
  - validacion final de pipeline completo.

## Evidencia tecnica
- `docker-compose.yml` incluye servicio `zookeeper`.
- `docker-compose.yml` configura `KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181`.
- `docker compose ps` muestra `zookeeper` y `kafka` en ejecucion.

## Consecuencias
- Desviacion controlada respecto al ejemplo del profesor en fases 1-2.
- No bloquea la evaluacion funcional de fases 5-6 (dbt + Airflow + DAG master), que queda demostrada con evidencias de ejecucion.
- Posible mejora futura: migrar a KRaft en un cambio dedicado, con ventana de pruebas separada.
