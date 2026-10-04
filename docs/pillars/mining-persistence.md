# Persistencia de minería de pilares

## 1. Propósito

La persistencia de minería guarda las observaciones producidas por los pilares
antes del inicio de un evento. Su objetivo es permitir responder preguntas como:

- ¿Qué distribución tiene una métrica por deporte, competencia o minuto?
- ¿Cuándo un pilar no tuvo datos suficientes y qué input faltó?
- ¿Qué versión de un motor produjo una señal?
- ¿Cómo se relacionó una dirección HOME/AWAY con el resultado real?
- ¿Qué módulos o componentes aportaron una señal útil?

La minería almacena observaciones, no calibraciones aprendidas ni resultados
duplicados. El resultado real continúa en `results` y se relaciona por
`event_id`.

## 1.1 Estado actual de la implementación

El contrato, el repositorio y la integración runtime están activos para P1, P2, P3, P4 y P5. El pipeline llama a la minería inmediatamente después de calcular cada output y conserva también los resultados de error donde el productor los expone. P5 proyecta cada memoria de bookmaker de forma independiente y mantiene Betfair como exposición diagnóstica sin score.

P2–P5 escriben ahora esquema 4 con selección FT y estados compartidos; sus
reglas están en [market-evaluation-v1.md](market-evaluation-v1.md). Los payloads
históricos conservan su esquema y estado. P1 no cambia en esta refactorización.

P1 se calcula en `pillar_pipeline.py` y ahora se persiste en dos runs
independientes. Su orquestador devuelve dos salidas:

- `side`: un diccionario con `pillar_id`, `value`, `raw` y los módulos M1–M7;
  cada módulo contiene sus componentes, valores, bias, strength y raw propio.
- `totals`: un `P1TotalsOutput` que el pipeline serializa con `asdict`; contiene
  las capas estructural, temporal y trend, además del composite, estados,
  ventanas y `raw`.

Si `totals` es `None`, la implementación actual persiste `side` y omite el run
de totals; el pipeline deja visible esa condición en el log de disponibilidad.

## 2. Arquitectura

El modelo tiene tres niveles:

```text
events
└── pillar_mining_runs
    └── pillar_mining_units
        └── pillar_mining_metric_values
```

`pillar_mining_runs` identifica una ejecución. `pillar_mining_units` representa
el resumen, módulos, componentes, layers o trayectorias que componen el output.
`pillar_mining_metric_values` proyecta únicamente valores escalares que deben
poder filtrarse, agruparse o agregarse con SQL.

Los pilares y sus adaptadores no conocen SQLAlchemy. El paquete
`modules/pillars/mining` define contratos y puertos; el repositorio en
`infrastructure/persistence/repositories` implementa la escritura.

## 3. Grano e identidad

### 3.1 Run

Una fila de `pillar_mining_runs` significa:

> El pilar y scope indicados evaluaron este evento en este slot de ejecución con
> esta versión del motor.

La identidad es:

```text
event_id + pillar_id + result_scope + execution_slot + engine_version
```

El slot se resuelve en este orden:

1. `evaluation:<minutes_until_start>` cuando se conoce el minuto real de ejecución.
2. `target:<target_minute>` cuando solamente se conoce el minuto objetivo.
3. `event` cuando ninguno está disponible.

`evaluation_minute` responde cuándo corrió el pipeline. `target_minute` responde
qué snapshot seleccionó el motor. No deben intercambiarse: un pipeline puede
correr en T-5 y, por configuración o disponibilidad, evaluar un target diferente.

Repetir la misma identidad actualiza el run canónico. Otro minuto u otra versión
crea una observación independiente.

### 3.2 Unit

Una unit es una parte evaluable del resultado. Los tipos iniciales son
`summary`, `module`, `component`, `layer`, `market_period`, `choice` y `signal`.
En P2–P5 esquema 4, las units `signal` referencian inputs/contratos y conservan
estado, evidencia y diagnóstico; su valor escalar vive en métricas.

La identidad dentro del run es:

```text
run_id + unit_key
```

La key debe incluir el namespace necesario, por ejemplo `module:M1` o
`component:M1:home_form`. Esta unicidad global dentro del run hace que
`parent_unit_key` sea inequívoco aunque existan units de tipos diferentes.

`parent_unit_id` expresa jerarquía. Por ejemplo, un componente de P1 pertenece a
un módulo y el módulo pertenece al resumen. Los campos `score`, `direction`,
`strength`, `is_valid` y `signal_axis` contienen la proyección común; ningún
adaptador debe inventarlos cuando el pilar no los produjo.

Dimensiones de mercado usadas frecuentemente tienen columnas propias:
`market_type_id`, `line_value`, `choice_name`, `bookie_id`, `quote_id`,
`source`, `exchange_side` y `exchange_level`. El nombre, grupo y periodo del
mercado se resuelven mediante `canonical_market_types`; no se duplican como
columnas en `pillar_mining_units`. Una dimensión nueva o poco usada va en
`dimensions` hasta que exista una consulta y volumen que justifiquen
promoverla a columna.

### 3.3 Metric value

Una métrica es escalar y usa exactamente una de estas columnas:

| `value_type` | Columna |
|---|---|
| `number` | `numeric_value` |
| `text` | `text_value` |
| `boolean` | `boolean_value` |

Diccionarios y listas no son métricas. Deben vivir en `payload`, `context`,
`inputs` o `diagnostics`. Una métrica ausente significa “el productor no la
emitió”; no se crea una fila con cero ni se inventa un valor nulo.

## 4. Estados

Se conserva el estado original en `producer_status` y se añade un estado común
en `canonical_status`:

| Productor | Canónico |
|---|---|
| `ACTIVE`, `OK` | `SUCCESS` |
| `PARTIAL` | `PARTIAL` |
| `INSUFFICIENT_DATA` | `INSUFFICIENT` |
| `ERROR` | `ERROR` |
| `IGNORE`, `SKIPPED` | `SKIPPED` |

Un estado nuevo debe añadirse explícitamente a `status_policy.py`. Fallar de
forma visible es preferible a clasificar silenciosamente un significado nuevo.

La configuración pública es:

```dotenv
PILLAR_MINING_ENABLED=true
PILLAR_MINING_STATUS_MODE=all
```

`all` conserva todos los estados para medir cobertura y fallos.
`successful_only` conserva solamente `canonical_status = 'SUCCESS'`.

En P2–P5 esquema 4, `ACTIVE` significa al menos una señal calculada, incluso
neutral. Un run `SUCCESS` puede contener señales bloqueadas o con error;
`execution_status` permite distinguirlo. Cobertura incompleta no genera
`PARTIAL` global nuevo. Las filas históricas con `PARTIAL` se conservan.

## 5. Flujo de escritura

1. El pipeline calcula el output normal del pilar.
2. `PillarMiningService` busca el adaptador registrado para `pillar_id`.
3. El adaptador traduce el output a `PillarMiningRun` con units y métricas.
4. El contrato valida identidades, parents, ciclos, estados y tipos escalares.
5. La política de configuración decide si debe persistirse.
6. El repositorio hace upsert del run y obtiene sólo su ID con `RETURNING`.
   El `ON CONFLICT DO UPDATE` conserva el bloqueo de la identidad hasta terminar
   la transacción; no se vuelve a descargar el JSON del run.
7. En la misma transacción reemplaza las units y métricas mediante lotes de
   hasta 1.000 filas. Se insertan los padres antes que sus hijos.
8. Un error se registra con contexto, pero no detiene los pilares restantes.

El reemplazo completo de hijos evita datos obsoletos. Si una repetición de P4
deja de incluir una choice, esa choice no debe sobrevivir en la observación
canónica anterior.

### 5.1 Escritura por lotes y memoria

`iter_unit_layers` comparte entre validación y escritura el recorrido del grafo:
detecta padres ausentes, claves duplicadas y ciclos en tiempo lineal. No utiliza
recursión ni búsquedas repetidas en una lista de pendientes.

En PostgreSQL con psycopg 3, las units y métricas se transfieren mediante
`COPY FROM STDIN`. Los IDs de units se reservan con la secuencia real de la tabla;
no se calculan usando `MAX(id)`. Se conservan todas las señales, incluso las
bloqueadas, y todas las restricciones de integridad. La conexión es la misma
que usa el `Session`: el transporte no hace commits ni abre transacciones aparte.
Las secuencias pueden dejar huecos al hacer rollback, como en un INSERT normal.

SQLite y los otros drivers PostgreSQL usan INSERT por lotes. Los IDs devueltos
se asocian mediante `unit_key`, sin asumir el orden de `RETURNING`.

Se mantienen un mapa de IDs y buffers acotados, sin crear un objeto ORM por fila.
El borrado previo resuelve los IDs en una subconsulta SQL. P5 conserva su captura
de muestras y propiedad del run dentro de la misma transacción.

El log de confirmación incluye `duration_ms` para la adaptación, validación y
escritura, incluyendo commit cuando el servicio administra la transacción.
El registro de la revisión está en
[la auditoría de rendimiento](../audits/2026-10-04-pillar-mining-performance.md).

## 6. Lectura y queries

### 6.1 Lectura histórica del perfil estructural de P2 (esquema menor que 4)

En esquemas históricos P2 no publica un score escalar ni una dirección global.
La unidad conserva el perfil en `payload->'P2_SIGNAL_PROFILE'`.

```sql
SELECT
    r.sport,
    r.evaluation_minute,
    u.payload->'P2_SIGNAL_PROFILE' AS signal_profile,
    count(*) AS observations
FROM pillar_mining_units u
JOIN pillar_mining_runs r ON r.id = u.run_id
WHERE r.pillar_id = 'pillar_2_side_market'
  AND r.payload_schema_version < 4
  AND u.unit_type = 'summary'
  AND r.canonical_status = 'SUCCESS'
GROUP BY r.sport, r.evaluation_minute, u.payload->'P2_SIGNAL_PROFILE';
```

### 6.2 Señales de P2–P5 (esquema 4)

El resultado vive una vez en `run.output_payload`; sus inputs en `run.inputs`.
`signal_unit_refs` enlaza las señales almacenadas como units. P2/P3/P4 no
inventan una predicción global; una evaluación debe seleccionar una métrica y
definir su outcome compatible. P5 conserva dirección e intermedios en su análisis.

```sql
SELECT r.pillar_id, r.engine_version, r.sport, r.target_minute,
       r.output_payload->>'selected_full_time_period' AS ft_period,
       u.payload->>'signal_key' AS signal_key,
       u.diagnostics->'evidence'->>'metric' AS analytical_metric,
       u.producer_status AS signal_status,
       u.diagnostics->>'reason' AS blocked_reason,
       m.numeric_value, m.text_value, m.boolean_value
FROM pillar_mining_runs r
JOIN pillar_mining_units u ON u.run_id = r.id AND u.unit_type = 'signal'
LEFT JOIN pillar_mining_metric_values m ON m.unit_id = u.id AND m.metric_name = 'value'
WHERE r.payload_schema_version = 4
  AND r.pillar_id IN ('pillar_2_side_market', 'pillar_3_totals_market_context',
                      'pillar_4_temporal_market_drift', 'pillar_5');
```

`PillarMiningRepository.get_result(run_id)` devuelve el resultado reconstruido
de esquema 4 y el payload original de esquemas anteriores. No reinterpreta
sus estados. Una señal bloqueada no tiene una fila numérica con cero.

### 6.3 Diagnóstico de cobertura

```sql
SELECT
    r.sport,
    r.producer_status,
    c->>'bookie_id' AS bookie_id,
    c->>'family' AS family,
    c->>'period' AS period,
    c->>'status' AS coverage_status,
    c->>'reason' AS reason,
    count(*) AS observations
FROM pillar_mining_runs r
CROSS JOIN LATERAL jsonb_array_elements(r.output_payload->'coverage') c
WHERE r.payload_schema_version = 4
GROUP BY r.sport, r.producer_status, c->>'bookie_id', c->>'family',
         c->>'period', c->>'status', c->>'reason'
ORDER BY observations DESC;
```

### 6.4 Lectura histórica del perfil estructural de P3 (esquema menor que 4)

En esquemas históricos, P3 sigue el mismo contrato de P2: no publica un score escalar
ni una dirección global. La unidad de minería conserva el perfil completo en
`payload->'P3_SIGNAL_PROFILE'`. Sus bloques `FT`, `1H` y `FT_1H` contienen las
lecturas individuales, relaciones entre books y representatives que existían
en el momento canónico evaluado.

```sql
SELECT
    r.sport,
    r.competition_id,
    r.target_minute,
    u.payload->'P3_SIGNAL_PROFILE' AS signal_profile,
    count(*) AS observations
FROM pillar_mining_units u
JOIN pillar_mining_runs r ON r.id = u.run_id
WHERE r.pillar_id = 'pillar_3_totals_market_context'
  AND r.payload_schema_version < 4
  AND u.unit_type = 'summary'
  AND r.canonical_status = 'SUCCESS'
GROUP BY
    r.sport,
    r.competition_id,
    r.target_minute,
    u.payload->'P3_SIGNAL_PROFILE';
```

Una evaluación posterior debe seleccionar explícitamente el branch del perfil
que se desea estudiar. No debe sintetizar un resultado global para P3 durante
la persistencia.

No se debe calcular hit rate de cualquier `direction` sin mirar `signal_axis`.
`SIDE` puede contrastarse con ganador; `IMPLIED_PROBABILITY_MOVE` describe un
movimiento de mercado y no es automáticamente una predicción del partido.

### 6.5 Muestras congeladas de P5

```sql
SELECT r.id AS run_id, r.engine_version, s.sample_id, s.cutoff,
       s.query->'key' AS query_key, s.query->'filters' AS population_filters,
       s.sample_size, s.wins_home, s.wins_draw, s.wins_away
FROM pillar_mining_runs r
JOIN p5_memory_samples s ON s.run_id = r.id
WHERE r.pillar_id = 'pillar_5' AND r.payload_schema_version = 4;
```

Leer miembros mediante `SampleAuditReader.get_sample_page(sample_id, cursor,
page_size)` implementado por `Pillar5PriceMemoryRepository`. El cursor ordena
por fecha de inicio e identificador descendentes; default 100 y máximo 1.000.
La fuente es la muestra congelada, sin consultar nuevamente la vista histórica.

## 7. Mapeo actual y futuro

| Pilar | Scope | Jerarquía | Estado de Registro |
|---|---|---|---|
| P1 Side | `side` | summary → M1–M7 → components | Registrado y activo |
| P1 Totals | `totals` | summary → structural/temporal/trend layers | Registrado y activo |
| P2 | `side_market` | summary → señales | Esquema 4, motor `p2-signal-profile-v3` |
| P3 | `totals_market_context` | summary → señales | Esquema 4, motor `p3-signal-profile-v3` |
| P4 | `temporal_market_drift` | summary → señales | Esquema 4, motor `p4-signal-profile-v3` |
| P5 | `price_memory` | summary → señales por bookmaker/contrato | Esquema 4, motor `p5_price_memory_v4_0` |

Los cinco pilares siguen registrados en `_registered_mining_adapters()`.
P2–P5 usan el escritor compartido `MarketMiningAdapter`, con inputs y análisis
una sola vez. P5 nuevo calcula para bet365/Pinnacle y mantiene Betfair diagnóstico;
las ejecuciones antiguas, incluyendo SofaScore, permanecen almacenadas.
Los adaptadores proyectan identidad y metadatos del productor sin adivinarlos.

Los resultados FT pueden evaluarse con `results`. Una evaluación de primer tiempo o de una línea totals necesita una fuente de outcome apropiada; no debe forzarse con `results.winner` si semánticamente no corresponde.

## 8. Implementación de persistencia de P1

### 8.1 Objetivo y frontera

La implementación persiste exactamente las salidas que ya devuelve el cálculo de P1, sin volver a
calcularlas ni alterar su semántica. La persistencia debe conservar el output
completo en `run.output_payload` para auditoría y proyectar a units/metrics solo
los campos que sean consultables.

P1 Side y P1 Totals deben ser dos runs canónicos, no dos tablas ni un único JSON
mezclado:

```text
event_id + pillar_id=pillar_1_team_structure + result_scope=side   + execution_slot + engine_version
event_id + pillar_id=pillar_1_team_structure + result_scope=totals + execution_slot + engine_version
```

Comparten `pillar_id` y contexto del evento, pero tienen scopes, payloads y
jerarquías independientes. Si una salida no existe, no se debe fabricar una
métrica cero.

### 8.2 Adaptador de P1 Side

`modules/pillars/mining/adapters/pillar_1.py` implementa el adaptador de side que:

1. Use `pillar_id = 'pillar_1_team_structure'`, `result_scope = 'side'` y el
   `engine_version` de `result.raw.engine_version`.
2. Cree `summary` con `value` como score escalar de evidencia, `signal_axis =
   'SIDE'` y `direction` únicamente cuando se defina explícitamente como
   `result.raw.final.p1_final_bias`. `p1_final_state`, `pressure_relation` y
   `anomalies` quedan en `payload`/`diagnostics`; no deben convertirse
   automáticamente en una predicción.
3. Cree una unit `module:M1` … `module:M7` por cada módulo, hijas de `summary`,
   proyectando `value`, `bias`, `strength` y el estado que vive en cada
   `module.raw` (`mN_status` y `mN_status_reason`).
4. Cree units `component:<module_id>:<name>` hijas del módulo, proyectando
   `edge`, `weight` y `weighted_edge` como métricas numéricas; el `raw` de cada
   componente permanece en `payload`.
5. Guarde en `summary.payload` la agregación `raw.layer_a`, `raw.layer_b`,
   `raw.final`, `raw.module_statuses`, `raw.active_modules` y
   `raw.skipped_modules`, además de mantener el resultado completo en el run.

El adaptador debe definir una política explícita para el estado del run porque
P1 Side no publica un status global: todos los módulos utilizables pueden
normalizarse a `SUCCESS`, una mezcla de módulos utilizables y no utilizables a
`PARTIAL`, y ausencia de módulos utilizables a `INSUFFICIENT`. Esa política
debe quedar cubierta por pruebas y no inferirse desde el score.

### 8.3 Adaptador de P1 Totals

El mismo archivo implementa un adaptador de totals que recibe el diccionario
producido por `asdict(P1TotalsOutput)`:

1. Use `pillar_id = 'pillar_1_team_structure'`, `result_scope = 'totals'` y
   `engine_version` del output.
2. Cree `summary` con `signal_axis = 'TOTALS'`, `score_name =
   'P1_TOTALS_DIRECTIONAL_SCORE'`, `score` con ese campo y `direction` con
   `P1_TOTALS_DIRECTION`. `P1_TOTALS_COMPOSITE` debe ser una métrica separada,
   no reemplazar el score direccional.
3. Cree units hijas `layer:STRUCTURAL`, `layer:TEMPORAL` y `layer:TREND` a partir
   de `active_layers` y `ignored_layers`, conservando `status`, señales, peso,
   señal ponderada y `ignored_reason`.
4. Proyecte como métricas las salidas escalares estables del dataclass
   (`P1_TOTALS_COMPOSITE`, `BREAKOUT_SCORE`, `TREND_DOMINANCE`, volatilidad,
   conteos y scores). Los mapas `P1_TOTALS_INTERNAL_STATE`, `WINDOWS_USED` y
   `WINDOW_COMPLETENESS_BY_WINDOW` permanecen como payload/contexto salvo que
   una consulta frecuente justifique promover una columna.
5. Conserve `raw.directional_components`, `raw.variance_components`,
   `raw.temporal`, `raw.trend`, `raw.composite_breakout` y `raw.policy_ignore`
   en payload/diagnostics.

`status = 'OK'` se normaliza explícitamente a `SUCCESS`. Si P1 Totals no está
disponible porque el orquestador lo devuelve como `None`, el pipeline persiste
side y omite el run de totals; no crea una métrica cero ni un resultado
inventado. El log `P1/P1_TOTALS Totals skipped ... unavailable` mantiene visible
la condición para una futura decisión de envelope `ERROR`.

### 8.4 Integración en el pipeline

Después de `calculate_pillar_1_team_structure` y después de completar el `raw`
de contexto de P1, el pipeline llama a la minería con las dos salidas
disponibles. La dispatch distingue side y totals de forma explícita
(claves de adaptador separadas o un scope explícito); no debe hacer que un
adaptador adivine el scope por la forma del JSON.

La persistencia de side se ejecuta aunque totals sea `None`. La persistencia
de minería sigue siendo un side effect analítico: sus excepciones se registran y
no deben impedir que continúen P4/P5. Si se elige persistir el envelope de
totals ausente, el cambio debe hacerse en el orquestador o en el pipeline para
que el motivo original llegue al adaptador.

### 8.5 Secuencia de implementación y pruebas

1. Mantener congelados los nombres de `result_scope`, `signal_axis`, engine versions y la
   política de status de ambos outputs.
2. Los dos adaptadores usan `to_json_value`, sin importar
   SQLAlchemy ni modificar los motores de P1.
3. Ambos adaptadores están registrados en `_registered_mining_adapters()` y las
   llamadas ocurren justo después del cálculo de P1.
4. Añadir pruebas de contrato para jerarquía, keys determinísticas, métricas
   escalares, serialización de dataclass, status ACTIVE/partial/insuficiente y
   reemplazo idempotente del run.
5. Añadir pruebas del pipeline para: side + totals, side sin totals, P1
   deshabilitado, falta de `streak_analysis`, excepción de minería y repetición
   en el mismo slot.
6. Validar queries de cobertura y de evaluación por separado: `SIDE` puede
   relacionarse con `results.winner`; `TOTALS` debe evaluarse contra un outcome
   de goles apropiado, no contra `results.winner`.
7. Activar primero con `PILLAR_MINING_STATUS_MODE=all`, revisar conteos y
   payloads, y solo después considerar `successful_only`.

## 9. Cómo añadir un nuevo pilar

1. Documentar el grano del output y su `result_scope`.
2. Identificar summary, módulos y unidades hijas con keys determinísticas.
3. Definir `signal_axis` y la semántica de `direction`.
4. Separar métricas escalares de payloads complejos.
5. Conservar estado original y normalizarlo explícitamente.
6. Añadir un adaptador que implemente `PillarMiningAdapter`.
7. Registrar el adaptador en el composition root del pipeline.
8. Persistir inmediatamente después del cálculo del pilar.
9. Añadir pruebas de ACTIVE/insuficiente/error, jerarquía y métricas obligatorias.
10. Añadir al menos una query de evaluación válida para ese `signal_axis`.

No se debe añadir SQLAlchemy al paquete del pilar, una tabla específica por pilar
ni métricas arbitrarias dentro de JSON cuando deban agregarse con SQL.

## 10. Migración y operación

La versión 2 elimina de forma idempotente la tabla experimental no desplegada
`pillar_mining_observations` y crea las tres tablas actuales. No existe backfill.
El borrado fue aceptado porque los datos eran exclusivamente experimentales.

Las cascadas son:

```text
delete event → delete runs → delete units → delete metrics
```

La falla de minería es un side effect analítico: debe generar un log estructurado
con pilar, evento, estado, target y versión, pero no detener P4, P5 ni el resto
del procesamiento. Para troubleshooting, revisar primero ese log, después la
validación del contrato y finalmente la conectividad/transacción del repositorio.

Para P5 esquema 4 aplicar primero `20261002_01` y después la aplicación nueva.
La migración aditiva crea las muestras y sus miembros. Captura y escritura usan
una transacción; el reemplazo del mismo run elimina únicamente sus muestras.
Volver a la aplicación anterior puede dejar estas tablas en su lugar, sin
backfill ni reinterpretación de resultados históricos.

Para la optimización de escritura aplicar `20261004_01`, posterior a
`20261003_01`. Agrega exclusivamente `idx_pillar_mining_unit_parent` sobre
`parent_unit_id`: las comprobaciones de la relación padre–hijo buscan por padre,
sin conocer el `run_id` que encabeza el índice compuesto existente.
PostgreSQL crea el índice con `CONCURRENTLY`; la migración reconoce una creación
previa válida y permite reconstruir un índice inválido dejado por un intento
interrumpido. El downgrade sólo retira ese índice. No cambia columnas ni filas.

## 11. Archivos principales

- `modules/pillars/mining/contracts.py`: contrato y validación del grafo.
- `modules/pillars/mining/service.py`: política de aplicación.
- `modules/pillars/mining/adapters/pillar_1.py`: adaptadores de P1 Side y P1
  Totals.
- `modules/pillars/mining/adapters/pillar_2.py`: traducción de P2.
- `modules/pillars/mining/adapters/pillar_3.py`: traducción estructural de P3.
- `modules/pillars/mining/adapters/pillar_4.py`: traducción del perfil temporal de P4.
- `infrastructure/persistence/models.py`: esquema SQLAlchemy.
- `infrastructure/persistence/repositories/pillar_mining_repository.py`: transacción y escritura por lotes.
- `infrastructure/persistence/alembic/versions/`: migraciones de esquema.
- `modules/jobs/pre_start_check_job/pillar_pipeline.py`: integración runtime.
