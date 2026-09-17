# Database Hardening Roadmap

## Propósito

Este documento registra los problemas de diseño e integridad identificados en
la capa de persistencia. No prescribe cambios inmediatos: cada iniciativa debe
pasar primero por análisis, decisión arquitectónica, plan de migración y pruebas.

Regla de alcance: **DB-001 es la única iniciativa activa. No comenzar DB-002 ni
las siguientes hasta recibir una indicación explícita del propietario del
proyecto.**

Estados usados:

- `ACTIVE`: problema actualmente en estudio.
- `DEFERRED`: reconocido, pero fuera del alcance actual.
- `BLOCKED`: requiere una decisión o información externa.
- `DONE`: implementado, migrado y verificado en PostgreSQL.

## DB-001 — Semántica temporal inequívoca

**Estado:** `ACTIVE — contrato global implementado y migrado; pruebas operacionales ampliadas aún pendientes`

**Problema:** La aplicación mezcla instantes UTC, horas locales de Ciudad de
México y objetos `datetime` sin `tzinfo`. Varias columnas llamadas `_utc` son
`TIMESTAMP WITHOUT TIME ZONE`, y distintos consumidores interpretan el mismo
valor naive de forma diferente.

**Invariante objetivo:** Todo instante que represente un momento real en la
línea de tiempo se mantiene timezone-aware y se persiste como `timestamptz`.
UTC es la representación canónica para cálculos e intercambio. La zona de
negocio se usa solamente para reglas de calendario local y presentación.

**Decisiones de alcance confirmadas por el propietario:**

- Todos los valores existentes en columnas `timestamp without time zone`
  siguen una única convención: son horas civiles de `America/Mexico_City`.
- `events.start_time_utc` se renombra a `events.starts_at`; el nombre describe
  el concepto y no promete un formato visual u offset determinado.
- Todas las columnas que representan instantes se convierten in-place a
  `timestamptz`; no se mantienen cohortes ni adaptadores legacy.
- La aplicación intercambia instantes UTC-aware y reserva
  `Config.TIMEZONE` para calendario local y presentación.

### Estado desplegado verificado — 2026-09-17

| Campo | Tipo real en PostgreSQL | Contrato actual |
|---|---|---|
| Todas las columnas temporales de tablas base | `timestamp with time zone` | Instantes; ORM y dominio entregan UTC aware. |
| `events.starts_at` | `timestamp with time zone` | Inicio canónico del evento; reemplaza `start_time_utc`. |
| `event_source_resolution_queue.source_starts_at` | `timestamp with time zone` | Inicio informado por el proveedor. |
| `market_choice_snapshots.collected_at` | `timestamp with time zone` | Instante de observación normalizado a UTC. |
| `market_choice_snapshots.source_collected_at` | `timestamp with time zone` | Instante informado por el proveedor, normalizado a UTC. |

La vista retirada `basketball_results_season_year` ya no existe. Las consultas
de trayectoria operan directamente entre columnas `timestamptz`; P4 solo
calcula con instantes aware normalizados a UTC. PostgreSQL fue verificado con
cero columnas `timestamp without time zone`, sesión en UTC y ausencia del
nombre antiguo `start_time_utc`.

### DB-001.1 — Especificar el lenguaje temporal del sistema

- [x] Definir formalmente `Instant`, `LocalDate`, `LocalTime` y
  `ZonedDateTime` en una decisión arquitectónica (ADR).
- [x] Clasificar cada campo temporal: instante, fecha civil, hora civil,
  duración o timestamp recibido de proveedor.
- [x] Renombrar `start_time_utc` a `starts_at` y definir su contrato como
  instante UTC timezone-aware.
- [x] Establecer que un `datetime` naive no puede cruzar límites de dominio,
  repositorio o integración sin una zona de origen explícita.
- [x] Documentar que cambiar `Config.TIMEZONE` altera presentación y reglas de
  calendario, pero nunca el instante persistido.

### DB-001.2 — Inventariar productores, persistencia y consumidores

- [x] Inventariar todas las columnas `DateTime`, su nulabilidad, default y
  significado real.
- [x] Inventariar los productores que escriben en persistencia: Unix
  timestamps, strings ISO-8601,
  reloj del sistema, SQL `CURRENT_TIMESTAMP` y valores derivados.
- [ ] Inventariar todos los consumidores: queries por ventanas, pre-start,
  T-1, alertas, pilares, matching entre proveedores, vistas, materialized
  views, scripts y serialización.
- [x] Clasificar las referencias modificadas como cálculo de instante,
  calendario local,
  presentación o auditoría.
- [x] Localizar cada uso de `datetime.now()`, `datetime.fromtimestamp()`,
  `.timestamp()`, `replace(tzinfo=...)`, `localize()` y eliminación de
  `tzinfo`.

### DB-001.3 — Confirmar las precondiciones operacionales

- [x] Registrar como precondición de negocio que todo valor temporal existente
  representa una hora civil naive de
  `America/Mexico_City`; no se realizará una auditoría por cohortes.
- [x] Confirmar que no queda ninguna columna `timestamp without time zone` y
  que las conexiones de aplicación se configuran en UTC.
- [ ] Medir tamaño de `events`, índices dependientes y duración estimada del
  `ALTER COLUMN TYPE`, porque la conversión in-place puede requerir reescritura
  y un lock exclusivo aunque no cambie el nombre.
- [ ] Registrar conteo, nulos, mínimo y máximo antes de la migración.
- [x] Calcular antes de migrar una huella/reconciliación de los epochs esperados
  interpretando cada valor con la zona IANA `America/Mexico_City`.
- [x] Ejecutar una consulta post-migración que demuestre que cada nuevo
  `timestamptz` representa exactamente el epoch esperado.

### DB-001.4 — Diseñar la representación canónica

- [x] Usar `UTCDateTime`/PostgreSQL `timestamptz` para todos los instantes.
- [x] Generar `now` persistido como UTC aware.
- [ ] Evaluar `server_default` y actualización administrada por la base para
  timestamps de auditoría.
- [ ] Usar `DATE` para fechas de negocio y `TIME` más una zona IANA para
  horarios civiles recurrentes.
- [x] Centralizar parsing y conversiones en una API temporal pequeña basada en
  `datetime.timezone.utc` y `zoneinfo.ZoneInfo`.
- [x] Hacer que esa API rechace inputs naive salvo en una función de adaptación
  legacy que exija declarar la zona de origen.

### DB-001.5 — Corregir los límites de entrada

- [x] Convertir Unix timestamps de inicio de evento con `datetime.fromtimestamp(value,
  tz=timezone.utc)`.
- [x] Exigir offset o `Z` en timestamps ISO que cruzan el límite de
  persistencia; documentar la
  excepción si un proveedor entrega hora civil.
- [x] Normalizar los inicios de evento a UTC aware una sola vez al ingresar al sistema.
- [x] Evitar crear simultáneamente copias naive UTC y naive local del mismo
  instante.
- [ ] Conservar el payload/origen necesario para auditar correcciones de hora
  hechas por un proveedor.

### DB-001.6 — Migrar PostgreSQL de forma segura

- [x] Implementar una migración enfocada e idempotente, separada del
  sincronizador genérico, y registrarla en el runner existente. Adoptar Alembic
  para todo el esquema permanece en DB-002 y no se mezcla con este cambio.
- [x] Cambiar el modelo ORM a `UTCDateTime` y renombrar los conceptos
  `start_time_utc`/`source_start_time_utc`.
- [x] Interpretar cada valor legacy mediante la zona IANA, conceptualmente
  `starts_at AT TIME ZONE 'America/Mexico_City'`; no aplicar un offset fijo
  como `-06:00`, porque las reglas históricas dependen de la fecha.
- [x] Inventariar y recrear de forma controlada vistas, materialized views e
  índices que dependan de la columna y puedan impedir el cambio de tipo.
- [ ] Elegir una ventana de mantenimiento: detener writers legacy, aplicar la
  migración, desplegar productores/consumidores aware y reanudar. La estrategia
  in-place no ofrece la compatibilidad gradual de una columna paralela.
- [x] Configurar las conexiones de aplicación en UTC y normalizar
  explícitamente las lecturas a UTC.
- [ ] Definir y ensayar el downgrade antes del despliegue, entendiendo que
  volver a un tipo naive pierde nuevamente la información inequívoca del
  instante.
- [x] Validar tipos, ausencia de columnas naive y epochs fila por fila dentro
  de la transacción. Queda pendiente medir planes/rendimiento de las
  queries críticas antes de reanudar los jobs.
- [ ] Documentar el resultado, duración del lock y reconciliación postflight.

### DB-001.7 — Corregir queries y lógica de negocio

- [x] Calcular `minutes_until_start` mediante `starts_at - now_utc`, sin
  conversión a Ciudad de México.
- [x] Usar intervalos semiabiertos `[inicio, fin)` para las nuevas ventanas y
  las consultas de calendario corregidas.
- [x] Para “eventos del día local”, construir medianoche y fin del día en la
  zona configurada, convertir ambos límites a UTC y filtrar en PostgreSQL.
- [x] Revisar pre-start, correcciones de timestamp, T-1, NBA 4Q, alertas duales,
  Oddspapi, OddsPortal y todos los pilares que llaman `.timestamp()`.
- [x] Separar formateo local para mensajes de los cálculos del dominio.
- [x] Asegurar en los límites de inicio de evento que comparaciones y restas
  nunca mezclen aware y naive.
- [x] Migrar los timestamps de `market_choice_snapshots` y entregar a P4
  instantes UTC-aware sin adaptadores legacy.
- [x] Corregir la aritmética SQL de trayectoria para operar directamente entre
  columnas `timestamptz`.
- [x] Diseñar y ejecutar la migración de
  `market_choice_snapshots.collected_at` y `source_collected_at` a
  `timestamptz`; retirar después la adaptación legacy.

### DB-001.8 — Separar scheduler de semántica de eventos

- [x] Documentar qué jobs representan intervalos absolutos y cuáles horarios
  civiles de negocio.
- [x] No asumir que `Config.TIMEZONE` configura automáticamente la librería
  `schedule` ni el timezone del host/contenedor.
- [x] Fijar explícitamente la zona IANA para jobs diarios y definir el
  comportamiento ante cambios DST.
- [ ] Usar reloj monotónico para medir duración y detectar solapamientos; usar
  instantes UTC para persistir ejecuciones.
- [ ] Probar reinicios, ejecución tardía, cambio de fecha UTC/local y slots
  perdidos.

### DB-001.9 — Pruebas y criterio de terminado

- [x] Ejecutar la migración y verificaciones de esquema sobre PostgreSQL
  configurado. Queda pendiente automatizar una prueba desechable sobre la misma versión mayor de
  PostgreSQL usada en producción.
- [ ] Cubrir inputs Unix, ISO `Z`, offsets positivos/negativos y rechazo de
  naive inesperado.
- [x] Cubrir límites de medianoche local/UTC y transiciones DST en al menos una
  zona que sí tenga DST, aunque Ciudad de México actualmente no lo aplique.
- [ ] Verificar pre-start en T-30, T-5, T-1, T+1 y eventos reprogramados.
- [x] Añadir una regresión P4 que rechaza snapshots naive y demuestra que el
  dominio solo recibe instantes UTC-aware.
- [ ] Verificar que cambiar `Config.TIMEZONE` no cambia qué eventos son
  seleccionados por instante, solo calendario/presentación cuando corresponda.
- [ ] Verificar que API, ORM, SQL directo, vistas y materialized views presentan
  el mismo instante.
- [x] Eliminar helpers legacy y búsquedas residuales después de que las
  pruebas y reconciliaciones sean verdes.

## Backlog diferido

### DB-002 — Una sola fuente de verdad para el esquema

**Estado:** `DEFERRED`

- [ ] Adoptar Alembic como historial autoritativo y reproducible.
- [ ] Inventariar y convertir las migraciones imperativas de `database.py`.
- [ ] Evitar DDL automático durante el arranque normal de la aplicación.
- [ ] Hacer fallar el despliegue si una migración crítica no se aplica.
- [ ] Reservar `Base.metadata.create_all()` para escenarios claramente
  delimitados, principalmente pruebas.
- [ ] Añadir prueba de upgrade desde cada versión soportada y prueba de esquema
  nuevo desde cero.

### DB-003 — Identidad canónica multi-proveedor

**Estado:** `DEFERRED`

- [ ] Definir identidad canónica y externa para Participant, Competition y
  Season.
- [ ] Evaluar tablas `*_source_mappings` siguiendo el patrón de Event.
- [ ] Definir cardinalidades y reglas de unicidad por proveedor.
- [ ] Diseñar deduplicación, merge, aliases y trazabilidad del matching.
- [ ] Migrar FKs sin perder procedencia ni historia.

### DB-004 — Invariantes, nulabilidad y unicidad

**Estado:** `DEFERRED`

- [ ] Catalogar invariantes de cada tabla en lenguaje de negocio.
- [ ] Añadir `NOT NULL`, `CHECK`, FK y `UNIQUE` donde la base pueda protegerlas.
- [ ] Resolver identidades que contienen columnas nullable mediante `NULLS NOT
  DISTINCT`, índices parciales o un modelo diferente.
- [ ] Validar rangos de confidence, odds, scores, attempts, exchange level y
  movement.
- [ ] Validar estados, winner, gender y consistencia home/away.
- [ ] Comprobar que un parent `PillarMiningUnit` pertenezca al mismo run.

### DB-005 — Normalización y fuentes de verdad duplicadas

**Estado:** `DEFERRED`

- [ ] Documentar la fuente autoritativa de cada campo duplicado.
- [ ] Completar y retirar los campos legacy de Event mediante backfill y
  migración de consumidores.
- [ ] Revisar duplicación de datos canónicos en MarketSourceMapping.
- [ ] Revisar `EventObservation.sport` y otros valores derivados.
- [ ] Decidir si sets/periodos de Result requieren una tabla consultable.
- [ ] Separar estados operacionales como `alert_sent` de las entidades de
  dominio cuando existan múltiples tipos/canales/intentos.

### DB-006 — Cardinalidad y versionado de predicciones

**Estado:** `DEFERRED`

- [ ] Decidir si existe una o varias predicciones por evento.
- [ ] Definir identidad por tipo, modelo/engine version, instante y scope.
- [ ] Alinear PK, constraints, nombre de relación ORM e historial.
- [ ] Separar predicción, evaluación y resultado real si tienen ciclos de vida
  diferentes.

### DB-007 — Ciclo de vida, borrado y cascadas

**Estado:** `DEFERRED`

- [ ] Clasificar entidades como owned, reference, historical u operational.
- [ ] Revisar cada `CASCADE`, `SET NULL` y `RESTRICT` contra esa clasificación.
- [ ] Evitar que borrar un Bookie destruya historia de mercados accidentalmente.
- [ ] Alinear cascadas ORM con cascadas PostgreSQL y evaluar `passive_deletes`.
- [ ] Definir cuándo corresponde soft delete, desactivación o retención.
- [ ] Probar borrados completos y concurrentes con FKs habilitadas.

### DB-008 — Estrategia de índices basada en consultas

**Estado:** `DEFERRED`

- [ ] Eliminar índices duplicados por PK/UNIQUE solo después de verificar
  dependencias y planes.
- [ ] Inventariar queries críticas, joins, filtros, orden y cardinalidad.
- [ ] Reemplazar la creación heurística por nombre de columna con decisiones
  versionadas.
- [ ] Medir con `EXPLAIN (ANALYZE, BUFFERS)` y estadísticas reales.
- [ ] Revisar orden de índices compuestos, índices parciales, GIN y FKs.
- [ ] Establecer seguimiento de índices no usados y costo de escritura.

### DB-009 — Tipos, capacidad y consistencia de nombres

**Estado:** `DEFERRED`

- [ ] Sustituir fechas/horas almacenadas como strings por tipos nativos.
- [ ] Unificar el tipo de identificadores externos, considerando IDs no
  numéricos y ceros iniciales.
- [ ] Proyectar crecimiento de tablas de quotes/snapshots y evaluar BIGINT.
- [ ] Validar precisión y escala de odds y métricas contra todos los proveedores.
- [ ] Establecer convenciones consistentes para PKs, timestamps y nombres.

### DB-010 — Límites de módulos y paridad PostgreSQL

**Estado:** `DEFERRED`

- [ ] Separar modelos ORM, definición de vistas y operaciones de mantenimiento.
- [ ] Organizar modelos por contexto sin crear dependencias circulares.
- [ ] Modernizar el estilo declarativo de SQLAlchemy 2 cuando no interfiera con
  cambios funcionales.
- [ ] Mantener pruebas unitarias rápidas en SQLite solo donde sus semánticas
  sean equivalentes.
- [ ] Probar FKs, JSONB, índices funcionales, locking, vistas y migraciones en
  PostgreSQL real.
- [ ] Añadir un schema-contract test que compare metadatos, migraciones y base
  desplegada.
