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

Al 2026-09-02, el contrato, el repositorio y la integración runtime están
activos para P1, P2 y P3. El pipeline llama a la minería inmediatamente después
de calcular cada output y conserva también los resultados de error donde el
productor los expone.

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
`summary`, `module`, `component`, `layer`, `market_period` y `choice`.

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
`market_group`, `market_period`, `market_name`, `choice_group`, `choice_name`,
`bookie_id`, `quote_id`, `source`, `exchange_side` y `exchange_level`. Una
dimensión nueva o poco usada va en `dimensions` hasta que exista una consulta y
volumen que justifiquen promoverla a columna.

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

## 5. Flujo de escritura

1. El pipeline calcula el output normal del pilar.
2. `PillarMiningService` busca el adaptador registrado para `pillar_id`.
3. El adaptador traduce el output a `PillarMiningRun` con units y métricas.
4. El contrato valida identidades, parents, ciclos, estados y tipos escalares.
5. La política de configuración decide si debe persistirse.
6. El repositorio hace upsert del run y bloquea su identidad canónica.
7. En la misma transacción reemplaza todas las units y métricas del run.
8. Un error se registra con contexto, pero no detiene los pilares restantes.

El reemplazo completo de hijos evita datos obsoletos. Si una repetición de P4
deja de incluir una choice, esa choice no debe sobrevivir en la observación
canónica anterior.

## 6. Lectura y queries

### 6.1 Lectura del perfil estructural de P2

P2 no publica un score escalar ni una dirección global. La unidad de minería
conserva el perfil estructural completo en `payload->'P2_SIGNAL_PROFILE'`.

```sql
SELECT
    r.sport,
    r.evaluation_minute,
    u.payload->'P2_SIGNAL_PROFILE' AS signal_profile,
    count(*) AS observations
FROM pillar_mining_units u
JOIN pillar_mining_runs r ON r.id = u.run_id
WHERE r.pillar_id = 'pillar_2_side_market'
  AND u.unit_type = 'summary'
  AND r.canonical_status = 'SUCCESS'
GROUP BY r.sport, r.evaluation_minute, u.payload->'P2_SIGNAL_PROFILE';
```

### 6.2 Evaluación posterior de P2

P2 no debe evaluarse mediante un `hit_rate` directo de la unidad resumen,
porque no produce una predicción global. Cualquier evaluación futura debe
seleccionar explícitamente una señal y relación dentro de
`P2_SIGNAL_PROFILE`, y definir primero el contrato de outcome correspondiente.

### 6.3 Diagnóstico de cobertura

```sql
SELECT
    r.sport,
    r.producer_status,
    r.diagnostics->>'reason' AS reason,
    count(*) AS observations
FROM pillar_mining_runs r
WHERE r.pillar_id = 'pillar_2_side_market'
GROUP BY r.sport, r.producer_status, r.diagnostics->>'reason'
ORDER BY observations DESC;
```

### 6.4 Lectura del perfil estructural de P3

P3 sigue el mismo contrato de persistencia que P2: no publica un score escalar
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

## 7. Mapeo actual y futuro

| Pilar | Scope | Jerarquía |
|---|---|---|
| P1 Side | `side` | summary → M1–M7 → components |
| P1 Totals | `totals` | summary → structural/temporal/trend layers |
| P2 | `side_market` | summary → `p2_signal_engine` |
| P3 | `totals_market_context` | summary → `p3_signal_engine` |
| P4 | `temporal_drift` | summary → market periods → choices |
| P5 | `exact_price_memory` | summary → exact price memory module |

P1, P2 y P3 están registrados para escritura actualmente. P4 y P5 quedan
pendientes de escritura. P1 no necesita `bookie_id`: su output es un perfil de
equipos y no una observación de book. P4 y P5 sí deben exponer `bookie_id` en
sus outputs antes de activar sus adaptadores. El adaptador no debe resolver IDs
por nombre. La misma regla aplica a cualquier identidad canónica: si no viene
del productor o del contexto, no se adivina.

Los resultados FT pueden evaluarse con `results`. Una evaluación de primer
tiempo o de una línea totals necesita una fuente de outcome apropiada; no debe
forzarse con `results.winner` si semánticamente no corresponde.

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

## 11. Archivos principales

- `modules/pillars/mining/contracts.py`: contrato y validación del grafo.
- `modules/pillars/mining/service.py`: política de aplicación.
- `modules/pillars/mining/adapters/pillar_1.py`: adaptadores de P1 Side y P1
  Totals.
- `modules/pillars/mining/adapters/pillar_2.py`: traducción de P2.
- `modules/pillars/mining/adapters/pillar_3.py`: traducción estructural de P3.
- `infrastructure/persistence/models.py`: esquema SQLAlchemy.
- `infrastructure/persistence/repositories/pillar_mining_repository.py`: transacción.
- `infrastructure/persistence/database.py`: migración de esquema.
- `modules/jobs/pre_start_check_job/pillar_pipeline.py`: integración runtime.
