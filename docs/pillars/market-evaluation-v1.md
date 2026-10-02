# Política común de mercados de P2–P5

P2, P3, P4 y P5 utilizan `market-evaluation-v1` y el contrato de resultados
`payload_schema_version = 4`. P1 conserva su motor, versiones y semántica.
El éxito indica que se calculó una señal; no certifica cobertura completa ni
confianza estadística.

## Responsabilidades y dependencias

```mermaid
flowchart LR
    A[Cuotas y snapshots del evento] --> B[Normalización e índice]
    B --> C[Checkpoint causal común]
    C --> D[Selección FT común]
    D --> E[Dependencias por señal]
    E --> F[Fórmulas de cada pilar]
    F --> G[Resultado v4]
    G --> H[Mining: run y señales]
    E --> I[P5: agregado SQL y muestra congelada]
    I --> F
```

- `market_evaluation.py`: registro inmutable de bookmakers, contratos reconocidos,
  capacidades, selección FT y cobertura. Agregar un bookmaker al registro exige
  declarar sus capacidades antes de habilitar cálculos.
- `odds_trajectory_context.py` y `market_snapshot_extractor.py`: identidad,
  procedencia, indexación y extracción causal. El índice vive durante el evento.
- `signal_dependencies.py` y `profile_evaluation.py`: dependencias y referencias
  de P2/P3. Separan disponibilidad y diagnóstico de las fórmulas.
- Los motores específicos reciben snapshots o series acotados. Las primitivas
  numéricas comunes viven en `market_math.py`; P4 no importa matemática de P2/P3.
- `evaluation_contracts.py`: estado global, estado de ejecución y señales tipadas.
- `mining/adapters/market.py`: persiste un resultado canónico, con units por señal
  y métricas escalares. P5 depende del puerto `PriceMemoryReader`, inyectado por
  el pipeline; SQL y transacciones quedan en infraestructura.

Estas fronteras permiten modificar selección, fórmulas o almacenamiento por
separado. Las diferencias legítimas de los pilares se mantienen en capacidades
y dependencias; no hay un motor anterior activo ni un sistema de plugins.

## Selección temporal y FT

El pipeline resuelve una sola instancia de `TargetMinuteSelection`, con la
tolerancia configurada en `PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES`, y construye
un `EventMarketEvaluation` compartido por los cuatro motores. Los candidatos
son contratos canónicos reconocidos y no live.

1. Se examina `Full Time Including Overtime` en el checkpoint causal elegido.
2. Se elige overtime si permite alguna lectura actual soportada: par de precios,
   separación de líneas o movimiento temporal con sus observaciones requeridas.
3. Si overtime no es utilizable, se evalúa `Full Time` de regulación.
4. Si ninguno permite una lectura, FT queda sin seleccionar. Otros periodos
   soportados pueden seguir aportando señales independientes.

La prioridad no maximiza cobertura: una lectura OT válida prevalece sobre una
regulación más completa. Cada pilar recibe únicamente el FT elegido; ningún
bookmaker sustituye ese periodo. El historial de P5 no participa en esta decisión.
`selection` conserva candidatos, contratos utilizables, bloqueos, checkpoint y
motivo: `OVERTIME_PRIORITY`, `REGULATION_FALLBACK` o `NO_USABLE_FULL_TIME`.

## Identidad y completitud local

Un contrato conserva clave/tipo canónico, familia, periodo, línea y condición
live. Sus observaciones conservan mercado del proveedor, bookmaker, source,
outcome, lado y nivel de exchange, quote/snapshot y timestamps. Dos mercados del
proveedor no aportan fragmentos para fabricar un outcome vector completo.
El registro usa únicamente tipos que existen en el catálogo actual.

Una cuota válida es decimal finita y mayor que uno. Tamaños y límites son
opcionales cuando la fórmula usa solo precios. Los niveles BACK/LAY se conservan
por outcome; una ambigüedad de LAY no invalida una lectura BACK independiente.
Distintos contratos legítimos, como 1X2 y Home/Away, se calculan por separado.

| Pilar | Lectura mínima y relaciones conservadas |
|---|---|
| P2 | Home y Away habilitan el edge de una familia. Si el contrato es 1X2, la falta de Draw mantiene la cobertura incompleta y se identifica la lectura `HOME_AWAY_PRICES_FROM_1X2`. Las comparaciones entre bookmakers, moneyline/handicap, exchange y FT/periodo secundario exigen sus dependencias y compatibilidad respectivas. First To Fifth Inning conserva su periodo y etiquetas propias. |
| P3 | Over y Under habilitan el edge del contrato. La separación de líneas necesita las líneas; las comparaciones de precios en una misma línea exigen igualdad de periodo y línea. FT y First Half aportan lecturas independientes. BACK y LAY solo se exigen juntos cuando la fórmula usa ambos. |
| P4 | Una métrica de movimiento necesita al menos dos observaciones válidas y endpoint operativo. La continuidad se exige por métrica: un gap puede permitir net move y bloquear velocidad/path. Un contrato terminado conserva observaciones y diagnósticos, sin exponer movimiento cero. Los cambios de línea se representan explícitamente; no se unen trayectorias de precios de contratos distintos. Conserva moneyline, Asian Handicap, totals y sus periodos ya soportados, incluido OU First Quarter. |
| P5 | Vector actual moneyline FT por Pinnacle/bet365 y contrato. 1X2 exige Home/Draw/Away; Home/Away excluye Draw. El mínimo histórico sigue siendo tres eventos elegibles, con las mismas fórmulas y cuantización de precios a tres decimales. Betfair permanece diagnóstico y SofaScore no participa en cálculos nuevos. |

No hay bookmaker obligatorio para el éxito global. Betfair ausente nunca bloquea
globalmente un pilar. Un resultado neutral, incluyendo cero, cuenta como cálculo
válido; un valor ausente se mantiene ausente.

## Contrato y estados

El resultado contiene evento, pilar, versiones, checkpoint, selección FT,
`signals`, `coverage`, `inputs`, `contracts`, `analysis`, evidencia y diagnósticos.
Las entradas se guardan una vez; señales y series apuntan a referencias.
Los análisis específicos conservan los intermedios útiles para explicar fórmulas.

| Estado global | Condición |
|---|---|
| `ACTIVE` | Al menos una señal `COMPUTED`, aunque otras estén bloqueadas o hayan fallado. |
| `INSUFFICIENT_DATA` | Ningún cálculo válido y ningún error de ejecución. |
| `ERROR` | Ningún cálculo válido y al menos un error de ejecución. |
| `SKIPPED` | Pilar omitido por configuración. |

`execution_status` distingue `COMPLETED`, `COMPLETED_WITH_ERRORS`, `FAILED` y
`SKIPPED`. Un fallo de persistencia conserva los cálculos disponibles, registra
`PERSISTENCE_ERROR` y elimina referencias a muestras no persistidas.
Cada señal usa `COMPUTED`, `BLOCKED` o `ERROR`, valor opcional, referencias y
códigos estables: `MISSING_INPUT`, `INVALID_VALUE`, `AMBIGUOUS_CANDIDATE`,
`INCOMPATIBLE_CONTRACT`, `INSUFFICIENT_OBSERVATIONS`, `MISSING_ENDPOINT`,
`NON_CONTIGUOUS_GAP`, `INSUFFICIENT_HISTORY` o error de lookup/cálculo.

La cobertura enumera combinaciones esperadas de bookmaker, familia, periodo,
línea y lado de exchange, incluso ausentes. Usa `COMPLETE`, `INCOMPLETE`,
`MISSING`, `INVALID`, `AMBIGUOUS`, `EXCLUDED` y `NOT_APPLICABLE`.
`observed_status` conserva disponibilidad cuando la política excluye un periodo.
No introduce porcentaje de confianza ni pesos de cobertura.

## Auditoría reproducible de P5

La misma definición SQL de elegibilidad aplica precios exactos, shape, bookmaker,
sport, filtros configurados de competición/temporada/país, corte anterior al
evento actual, exclusión del evento actual, resultado compatible, scores y
deduplicación por evento. Se usa toda la población; no se aplica un límite de
coincidencias ni se materializa en listas Python.

Con mining habilitado, `INSERT … SELECT` congela miembros en
`p5_memory_sample_members`. El header `p5_memory_samples` conserva query, filtros,
cutoff, política, versiones, conteos y run propietario. Los conteos principales
provienen de los miembros capturados. Captura y persistencia del resultado usan
la misma transacción, con savepoint por lookup independiente.

Repetir la identidad `event + pillar + scope + slot + engine_version` reemplaza
units y muestras dependientes dentro de la transacción. Otras versiones quedan
intactas. Un rollback conserva la ejecución anterior y no deja muestras nuevas
huérfanas. Las claves foráneas eliminan muestras al eliminar su run.

El puerto `SampleAuditReader.get_sample_page(sample_id, cursor, page_size)`
devuelve miembros congelados, metadatos y `next_cursor`. El tamaño predeterminado
es 100 y el máximo 1.000. El orden es `starts_at DESC, event_id DESC`; el cursor
usa ambas columnas. Los miembros no dependen de modificaciones posteriores de
cuotas, resultados o de la vista materializada. No se agrega una interfaz web.

Con mining deshabilitado, P5 consulta agregados SQL y calcula el mismo resumen;
`sample_id` es nulo y `audit_persisted` es falso. Si `successful_only` descarta
una ejecución, su captura también se revierte.

## Persistencia y compatibilidad

| Pilar | Motor nuevo | Esquema |
|---|---|---|
| P2 | `p2-signal-profile-v3` | 4 |
| P3 | `p3-signal-profile-v3` | 4 |
| P4 | `p4-signal-profile-v3` | 4 |
| P5 | `p5_price_memory_v4_0` | 4 |

Mining normaliza `ACTIVE` a `SUCCESS`. La cobertura incompleta no produce
`PARTIAL` en resultados nuevos. Los lectores conservan estados/payloads de
esquemas históricos 1–3; `PillarMiningRepository.get_result(run_id)` reconstruye
señales v4 desde units/metrics sin reinterpretar datos antiguos. Consultas de
ejemplo están en `mining-persistence.md`.

El legacy retirado incluye los gates globales, selectores/DTOs de P5 anteriores,
serializadores repetidos, auditoría expandida de P4 y primitivas matemáticas
duplicadas en P2/P3. Se conservan componentes compartidos utilizados por P1 y
la lectura de resultados antiguos; no hay backfill automático.

## Despliegue y reversión

Aplicar primero la migración aditiva `20261002_01` y luego el código:

```powershell
.venv/Scripts/python.exe -m alembic upgrade head
```

La migración requiere el esquema existente y tiene como padre `20261001_01`.
Crea las dos tablas de muestras y sus índices/cascadas.
La ejecución online también admite tablas creadas previamente fuera de Alembic:
valida columnas, tipos, nulabilidad, claves primarias, claves foráneas con
`ON DELETE CASCADE` e índices antes de reconocerlas. Conserva sus filas y crea
únicamente las tablas o índices faltantes. Una estructura incompatible detiene
la migración antes del DDL; no se debe resolver con un `stamp` a ciegas ni borrando
las muestras. La generación SQL offline presupone que las tablas nuevas no existen.

La reversión operativa consiste en volver a la aplicación anterior dejando las tablas aditivas en su
lugar. Su downgrade elimina únicamente las tablas nuevas; no es necesario para
revertir la aplicación. Esta implementación valida migración en SQLite y DDL
PostgreSQL; su aplicación sobre la base de destino se realiza mediante Alembic.

## Verificación y memoria

Pruebas de fórmulas, selección, contratos, causalidad, adaptadores, auditoría,
migración, lectores y memoria:

```powershell
.venv/Scripts/python.exe -m pytest tests/pillars --ignore=tests/pillars/pillar_1_team_structure tests/test_p5_audit_repository.py tests/test_p5_audit_migration.py tests/test_pillar_mining_repository.py tests/test_pre_start_memory_limits.py tests/test_p5_price_memory_view.py tests/test_odds_trajectory_repository.py -q
.venv/Scripts/python.exe -m pytest tests -q
.venv/Scripts/python.exe tests/benchmarks/market_evaluation_memory.py --observations 500
```

La comparación offline de P4 usa el mismo fixture de 2.000 filas, incluyendo
normalización y ejecución, medido con `tracemalloc`: el checkout anterior obtuvo
15.693.547 bytes de pico y 21.350.641 bytes de payload; esta implementación obtuvo
14.162.600 bytes y 9.803.444 bytes, respectivamente. Es una medición de ese fixture,
no una garantía universal del consumo del proceso.

P4 comparte puntos originales y usa vistas pequeñas para derivaciones. P5 tiene
una prueba con 1.000 y 50.000 miembros generados dentro de SQL que verifica
conteos exhaustivos y memoria Python acotada; la lectura paginada solo carga
una página. El pipeline libera las cuotas por evento y conserva los límites de
lote existentes, sin caches globales de trayectorias ni muestras.

Verificación del 2 de octubre de 2026: la batería centrada en esta refactorización
pasó sus 177 pruebas. La suite completa registró 993 aprobadas, 4 omitidas y 61
fallos. Los 61 identificadores de fallo ya estaban entre los 68 del checkout
original; no apareció un identificador nuevo. Los ocho fallos de P1 coinciden
con los originales y sus archivos de motor/pruebas no fueron modificados.
Los fallos restantes pertenecen a proveedores, resolución de eventos,
normalización, adquisición y filtros fuera de esta refactorización. Los cambios
simultáneos en descubrimiento/borrado de eventos se conservaron separados.
