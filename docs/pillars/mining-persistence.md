# Instrumentación inmutable de P1–P5

## Contrato vigente: schema 5

Cada ejecución de evento genera un UUID `evaluation_id` y una frontera UTC `evaluation_as_of`.
Los seis ámbitos son independientes: P1 Side, P1 Totals, P2, P3, P4 y P5.
Una ejecución solamente termina después de confirmar su persistencia completa.

Estados de señal: `COMPUTED`, `BLOCKED`, `ERROR`.
Estados globales: `ACTIVE`, `INSUFFICIENT_DATA`, `ERROR`, `SKIPPED`.
Las clasificaciones internas (por ejemplo `OK`, `DEGRADED`, `IGNORE`, `TIE`) se conservan como datos del cálculo.
P1 conserva su clasificación original en `calculation_status` cuando aplica.
`BLOCKED` requiere un motivo del catálogo de `modules/pillars/instrumentation.py`.
Los diagnostics contienen `signals.total_signals`, `computed_signals`, `blocked_signals`,
`error_signals` y `reasons`, además de los detalles de cobertura.

## Interpretación oficial

P2–P5 agregan `INTERPRETATION` a su resultado canónico. Sus engines construyen
la lectura determinista antes de persistirla. El adaptador conserva el bloque
en `pillar_mining_runs.output_payload`; no lo convierte en nuevas señales ni
repite sus datos como métricas. No requiere tablas, columnas ni migraciones.
Los registros anteriores permanecen intactos y pueden no tener este bloque.

- El `status` exterior conserva el estado global del pilar. Las lecturas usan
  `COMPUTED`, `BLOCKED` o `ERROR`; `state` describe su significado.
- Las direcciones explícitas, `comparability`, `interpretation_code` y campos contractuales son
  la información estructurada. `summary` es una plantilla legible en español.
- `evidence` referencia el análisis original y, cuando corresponde, los inputs
  y muestras. No duplica RAW, trayectorias completas o poblaciones históricas.
- P2 presenta `scopes`, periodos, mercados, exchange y comparación entre fuentes.
  Sus representantes y componentes no cuentan como votos independientes.
  Un periodo `PARTIAL_STRUCTURE` conserva la dirección de las ramas disponibles
  cuando coinciden; una divergencia o neutralidad parcial no se oculta como consenso.
- P3 agrega `period_readings.FT` y `period_readings.1H`, con líneas y direcciones
  individuales, `books_direction` y `representative_direction`. Reúne lecturas
  repetidas en varios ámbitos sin recalcular el representante. Si hay varios
  contratos válidos candidatos, informa `AMBIGUOUS_CANDIDATE` y los conserva en
  `contract_readings`; no elige arbitrariamente una línea principal.
  `scopes` y `line_structures` conservan todo el detalle. Líneas distintas no
  implican conflicto direccional.
- P4 usa interpretación versión **2** y conserva `series` más resúmenes `contracts`
  por mercado, periodo, línea y vista. `endpoint_direction` expresa la posición
  final; `net_move_direction`, el movimiento neto; `final_run_direction`, el tramo
  final; `path_pattern`, el patrón existente. Una serie estable puede terminar
  AWAY con movimiento `NO_MOVEMENT` y tramo final nulo. Se retiraron de esta
  interpretación los campos ambiguos `direction` y `direction_at_checkpoint`.
  El resumen contractual usa los representantes existentes, indicando fuente y
  serie de origen. Si hay varias fuentes o selecciones sin un representante
  conjunto, conserva `source_readings` separados y no fabrica una dirección global.
- P5 conserva interpretaciones por bookmaker. Un DRAW dominante no es lo mismo
  que un empate entre las cuentas históricas. Solo P5 expone intensidad aquí.

P2, P3 y P5 mantienen interpretación versión 1; P4 usa versión 2. Los engines
de P2–P5 registran versión `v4.2` para incluir la síntesis ejecutiva.

## Resumen ejecutivo

Cada resultado nuevo incluye `EXECUTIVE_SUMMARY` junto a `INTERPRETATION`.
El primero selecciona unas pocas conclusiones que ya existen; no ejecuta fórmulas
ni produce una predicción adicional. El segundo conserva la lectura detallada
por contrato, periodo, fuente o serie. Ambos se construyen antes de persistir y
quedan guardados dentro de `pillar_mining_runs.output_payload`.

- P1 Side resume dirección, intensidad estructural y estado final. P1 Totals
  mantiene separados el score/dirección/intensidad direccionales y los del
  composite.
- P2 presenta FT, 1H (o First Five Innings) y su relación entre periodos; las
  ramas que sustentan cada lectura provienen de `INTERPRETATION`.
- P3 presenta FT y 1H por separado, conserva las líneas y deja la dirección
  conjunta vacía cuando los contratos no son comparables.
- P4 selecciona los contratos principales de `CHECKPOINT_VIEW`: 1X2, Asian
  Handicap y Over/Under. Para cada uno conserva endpoint, movimiento neto,
  tramo final y patrón. Las series completas siguen en `INTERPRETATION`.
- P5 resume las muestras históricas 1X2 de tiempo completo por bookmaker,
  incluyendo el tamaño y las cuentas HOME/DRAW/AWAY cuando existen.

La cadena es engine del pilar → interpretación → resumen ejecutivo → persistencia.
Los periodos de P2/P3 incluyen `source_readings`, agrupado en `books`,
`exchange` y `book_exchange`. Conserva una copia de cada lectura y contrato,
con sus líneas, dirección, estado, comparabilidad y motivo. La dirección principal
continúa siendo la de books; exchange se presenta por separado incluso cuando
books no tiene datos. En P3 una lectura de bookmaker único conserva su dirección
y línea sin promoverla a representante ni consenso.
El adaptador solo guarda ambos bloques. El logger imprime el resumen y, con
diagnóstico activado, agrega `INTERPRETATION_DETAIL` después. Los resúmenes se
emiten tras confirmar la transacción y solo para pilares habilitados. Los
resultados omitidos o fallidos reciben el mismo contrato con su estado actual.
Los motores P1 son `side_engine_multilayer_v3_2_executive_summary` y
`p1_totals_directional_vol_separated_v2_6_executive_summary`.

Consulta compacta de una evaluación (reemplazar el identificador):

```sql
SELECT pillar_id, result_scope, status,
       output_payload #>> '{EXECUTIVE_SUMMARY,summary}' AS executive_summary,
       output_payload #>> '{INTERPRETATION,summary}' AS summary,
       output_payload #>> '{EXECUTIVE_SUMMARY,version}' AS executive_summary_version,
       output_payload #>> '{INTERPRETATION,version}' AS interpretation_version
FROM pillar_mining_runs
WHERE evaluation_id = 'TU_EVALUATION_ID'
ORDER BY pillar_id, result_scope;
```

Para recuperar la lectura ejecutiva, seleccionar
`output_payload -> 'EXECUTIVE_SUMMARY'`. Para auditar el resultado completo,
seleccionar `output_payload -> 'INTERPRETATION'`. Los resúmenes ejecutivos
compactos de P3/P4 se pueden consultar así:

```sql
SELECT pillar_id,
       CASE pillar_id
         WHEN 'pillar_3_totals_market_context'
           THEN output_payload #> '{EXECUTIVE_SUMMARY}'
         WHEN 'pillar_4_temporal_market_drift'
           THEN output_payload #> '{EXECUTIVE_SUMMARY}'
       END AS executive_summary
FROM pillar_mining_runs
WHERE evaluation_id = 'TU_EVALUATION_ID'
  AND pillar_id IN ('pillar_3_totals_market_context', 'pillar_4_temporal_market_drift');
```

Para exportar el detalle de las series P4 sin sus inputs grandes:

```sql
SELECT x.key AS series_id,
       x.value ->> 'status' AS status,
       x.value ->> 'state' AS state,
       x.value ->> 'endpoint_direction' AS endpoint_direction,
       x.value ->> 'net_move_direction' AS net_move_direction,
       x.value ->> 'final_run_direction' AS final_run_direction,
       x.value ->> 'path_pattern' AS path_pattern,
       x.value ->> 'summary' AS summary
FROM pillar_mining_runs r
CROSS JOIN LATERAL jsonb_each(r.output_payload #> '{INTERPRETATION,series}') x
WHERE r.evaluation_id = 'TU_EVALUATION_ID'
  AND r.pillar_id = 'pillar_4_temporal_market_drift'
ORDER BY x.key;
```

Los logs `🏁` muestran, en orden, estado, conteos de señales, minuto objetivo y
`EXECUTIVE_SUMMARY`. Con diagnóstico activado añaden `INTERPRETATION_DETAIL`.
Por defecto no enumeran ámbitos, contratos, series, métricas analíticas ni
referencias de auditoría. Se emiten después del commit y solamente para pilares
habilitados.

## Preparación y frontera de conocimiento

1. Cargar trayectorias y contexto operativo; completar los enriquecimientos HTTP de P1.
2. Preparar mercados, candidatos y muestras P5 usando la vista actual.
3. Fijar `evaluation_as_of` después de la adquisición.
4. Calcular exclusivamente con esos inputs; escribir snapshot, seis ámbitos, unidades, métricas y muestras en una transacción.

No se ofrece un parámetro para etiquetar una adquisición nueva como una ejecución histórica.
La reconstrucción histórica lee los datos persistidos de la ejecución original.

En cuotas hay dos timestamps: `collected_at` es el instante UTC real del lote
observado/persistido por el sistema; `source_collected_at` es el instante informado
por el proveedor y queda NULL cuando falta (también en SofaScore). No se agregan
columnas de disponibilidad o escritura adicionales.

Los checkpoints se reconstruyen por el último timestamp de fuente <= objetivo,
sin tolerancia de antigüedad del cambio. Sin tick admisible con fuente se utiliza
una observación sin fuente dentro de la tolerancia actual y hasta la evaluación.
El contexto y P4 comparten un selector. El corte exige `collected_at <= evaluation_as_of`
y registros visibles capturados para la ejecución antes de cerrar la frontera.

Los snapshots anteriores con fuente válida participan sin límite por ID ni
reescritura. Sus fechas originales se conservan, pero un `collected_at` antiguo
no demuestra cuándo se adquirieron realmente. Esta reconstrucción nueva describe
el mercado con ingredientes disponibles hoy: no completa una evaluación original
ya persistida. No se requiere configuración de transición temporal.

P4 CHECKPOINT_VIEW usa el objetivo para los puntos reconstruidos; ADAPTIVE_VIEW
mantiene los cambios originales y añade un cierre lógico cuando corresponde.
`SOURCE_COLLECTED_AT`, `COLLECTED_AT`, `TARGET_MINUTE`, `TIME_BASIS` y
`OBSERVATION_KIND` distinguen los conceptos sin adquisiciones ficticias.
Cada punto en `inputs` guarda además `effective_at` y `time_basis`: varias
proyecciones pueden referenciar un mismo snapshot sin compartir su hora de punto.
Las versiones `pillar-instrumentation-v3-source-reconstruction` y
`market-evaluation-v3-source-reconstruction` identifican esta política.

Los inputs originales compartidos, incluido el contexto deportivo de P1, se conservan
una vez en `pillar_evaluations.inputs`. Los resultados referencian ese conjunto y guardan
sus selecciones y transformaciones propias con identificadores de observación/contrato.
Las selecciones compartidas se normalizan en `inputs.selected_inputs`, por hash de contenido;
los runs guardan referencias y los lectores las resuelven.
No se repite el payload completo en cada unidad ni en diagnostics.

## Información conservada

| Ámbito | Datos |
|---|---|
| P1 Side | Módulos, componentes, RAW, ajustes, pesos, clasificaciones y agregación final. |
| P1 Totals | Ventanas, cobertura, baselines, escalas, capas, directional score, breakout, trend dominance, composite continuo, pesos, alignment, counts, thresholds, intensidad y dirección. Las salidas tempranas tienen reason/evidence. |
| P2 y P3 | Inputs seleccionados, trazas contractuales, operaciones aritméticas, edges, diferencias, comparaciones y clasificaciones. |
| P4 | Observaciones, checkpoints reconstruidos o próximos, huecos, tiempos originales, movimientos, transformaciones, patrones y lecturas finales. No se interpolan precios. |
| P5 | Vector, filtros, muestra exacta, exclusiones, counts, componentes, factores y perfil por bookmaker. |

Los escalares numéricos se proyectan a métricas tipadas para consultas por rangos;
la evidencia estructurada conserva su forma original. `Decimal` usa `numeric` sin escala
impuesta en PostgreSQL y cadenas decimales exactas en JSON. SQLite conserva su texto
exacto para pruebas. No se introduce una conversión a float en la frontera de persistencia.

Cada snapshot registra configuración efectiva, hash de configuración, contrato/schema,
commit SHA cuando Git está disponible, hash de los archivos efectivos (incluidas modificaciones locales), versión de
Python y hashes de dependencias. Cada ámbito registra además su versión de engine.
En imágenes sin Git, `commit_sha` es nulo y `code_sha256` identifica siempre el código efectivo.

## P5

La población productiva continúa en `mv_p5_price_memory`, con sus filtros contractuales existentes.
La definición vigente utiliza `market_choice_quotes.current_odds` y su hora real
de actualización, no el último snapshot prepartido. No se reconstruye esa vista.
No hay comprobación adicional de `result_available_at`, estado deportivo ni consulta temporal sustitutiva.
La captura usa `INSERT … SELECT`; los counts se obtienen de los miembros capturados.
La transacción PostgreSQL usa `REPEATABLE READ` para que los bookmakers compartan una
misma versión de la población. El scoring consume ese resumen después de cerrar la frontera.
Cada muestra registra `acquired_at` y queda ligada a su run incluso si el scoring falla.
Los empates válidos y Betfair `role = DIAGNOSTIC` conservan su significado. P5 no es probabilidad calibrada.

## Inmutabilidad y fallos

`append_evaluation` y `append_run` insertan registros nuevos.
La identidad del run es `(evaluation_id, pillar_id, result_scope)`.
Un reenvío idéntico es idempotente; un contenido distinto con la misma identidad produce error.
Las unidades, métricas y muestras anteriores nunca se borran para reemplazarlas.
Triggers de base de datos impiden UPDATE/DELETE de las evaluaciones y sus resultados,
y la modificación de muestras ligadas a un run. No hay foreign keys históricas hacia
entidades operativas que permitan borrados en cascada.

Los errores de cálculo se conservan como resultados `ERROR`. Los fallos de infraestructura
se propagan al coordinador y revierten toda la evaluación; no generan una señal artificial de P5.
Se conservan `SKIPPED`, `BLOCKED` y cobertura insuficiente. No existe bypass de persistencia.
Las escrituras usan lotes y COPY con buffers acotados. Las poblaciones P5 no se cargan en Python.

## Lectores y outcomes

- `PillarMiningRepository.get_evaluation(evaluation_id)`: snapshot, versiones, configuración y seis resultados.
- `get_result(run_id)`: salida, señales y primera página de unidades/metrics.
- `get_units_page(run_id, after_id=…, page_size=…)`: recorrido paginado de toda la jerarquía.
- `Pillar5PriceMemoryRepository.get_sample_page(sample_id, cursor=…, page_size=…)`: miembros congelados.
- `get_mining_case(evaluation_id, observation_id=…)`: evaluación original y observación
  exacta del mismo evento, sin recalcular pilares. Un ID inexistente o de otro evento
  produce `LookupError`; no se sustituye por otra observación. Si se omite el ID,
  mantiene la lectura exploratoria del outcome más reciente, que puede cambiar.
- `get_outcomes_page(event_id, …)`: observaciones y correcciones posteriores.

Los outcomes se anexan a `event_outcome_observations` con fuente, ID externo, tiempos y
payload deportivo original (incluidos periodos/sets cuando están disponibles).
Una corrección crea otra observación. Los outcomes no actualizan el snapshot y no participan
en la selección productiva de P5. La resolución de mercados y las métricas predictivas
pertenecen a minería/investigación futura.

La lectura y paginación de observaciones pertenecen a
`outcome_observation_repository.py`; minería delega en ese repositorio.
`append_outcome_observation` devuelve el ID insertado para conservar su identidad.
El outcome del lector incluye también `event_id`. Estos lectores aún no normalizan
FT/1H ni resuelven contratos. El lector completo carga los resultados de los pilares;
el futuro resolver debe consultar únicamente la identidad de la evaluación y su
observación. Ver el [plan LAB + Outcome Resolver](../plans/lab-outcome-resolver.md).

## Migración

Aplicar Alembic `20261012_01` durante el despliegue, antes de iniciar workers con este código.
La revisión descarta únicamente el histórico minero anterior y crea el contrato nuevo,
sin backfill ni lectores de compatibilidad. Conserva eventos, resultados y la vista P5.
No agrega columnas a snapshots operativos ni reescribe sus filas. Antes de habilitar
el pipeline, configurar la frontera de IDs descrita arriba.
La transición no tiene downgrade de datos: una reversión requiere restaurar un respaldo.

## Validación PostgreSQL

`tests/test_immutable_instrumentation_postgres.py` valida la migración, COPY, precisión,
inmutabilidad, rollback y reconstrucción en PostgreSQL real. Requiere
`MINING_TEST_DATABASE_URL` y utiliza un schema aislado con UUID, que elimina al terminar.
Sin esa variable la prueba se omite. El resto de la suite utiliza SQLite aislado.
