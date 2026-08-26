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

### 6.1 Distribución de `SIDE_MARKET_EDGE`

```sql
SELECT
    r.sport,
    r.evaluation_minute,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY m.numeric_value) AS median_edge,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY m.numeric_value) AS p90_edge,
    count(*) AS observations
FROM pillar_mining_metric_values m
JOIN pillar_mining_units u ON u.id = m.unit_id
JOIN pillar_mining_runs r ON r.id = u.run_id
WHERE r.pillar_id = 'pillar_2_side_market'
  AND u.unit_type = 'module'
  AND m.metric_name = 'SIDE_MARKET_EDGE'
  AND r.canonical_status = 'SUCCESS'
GROUP BY r.sport, r.evaluation_minute;
```

### 6.2 Hit rate HOME/AWAY de P2

```sql
SELECT
    r.engine_version,
    count(*) FILTER (
        WHERE (u.direction = 'HOME' AND result.winner = '1')
           OR (u.direction = 'AWAY' AND result.winner = '2')
    )::numeric / NULLIF(count(*), 0) AS hit_rate,
    count(*) AS evaluated
FROM pillar_mining_runs r
JOIN pillar_mining_units u
  ON u.run_id = r.id AND u.unit_type = 'summary'
JOIN results result ON result.event_id = r.event_id
WHERE r.pillar_id = 'pillar_2_side_market'
  AND r.canonical_status = 'SUCCESS'
  AND u.direction IN ('HOME', 'AWAY')
GROUP BY r.engine_version;
```

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

No se debe calcular hit rate de cualquier `direction` sin mirar `signal_axis`.
`SIDE` puede contrastarse con ganador; `IMPLIED_PROBABILITY_MOVE` describe un
movimiento de mercado y no es automáticamente una predicción del partido.

## 7. Mapeo actual y futuro

| Pilar | Scope | Jerarquía |
|---|---|---|
| P1 Side | `side` | summary → M1–M7 → components |
| P1 Totals | `totals` | summary → structural/temporal/trend layers |
| P2 | `side_market` | summary → `p2_raw_engine` |
| P4 | `temporal_drift` | summary → market periods → choices |
| P5 | `exact_price_memory` | summary → exact price memory module |

Solamente P2 está registrado para escritura actualmente. P4 y P5 deben exponer
`bookie_id` en sus outputs antes de activar sus adaptadores. El adaptador no debe
resolver IDs por nombre. La misma regla aplica a cualquier identidad canónica:
si no viene del productor o del contexto, no se adivina.

Los resultados FT pueden evaluarse con `results`. Una evaluación de primer
tiempo o de una línea totals necesita una fuente de outcome apropiada; no debe
forzarse con `results.winner` si semánticamente no corresponde.

## 8. Cómo añadir un nuevo pilar

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

## 9. Migración y operación

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

## 10. Archivos principales

- `modules/pillars/mining/contracts.py`: contrato y validación del grafo.
- `modules/pillars/mining/service.py`: política de aplicación.
- `modules/pillars/mining/adapters/pillar_2.py`: traducción de P2.
- `infrastructure/persistence/models.py`: esquema SQLAlchemy.
- `infrastructure/persistence/repositories/pillar_mining_repository.py`: transacción.
- `infrastructure/persistence/database.py`: migración de esquema.
- `modules/jobs/pre_start_check_job/pillar_pipeline.py`: integración runtime.
