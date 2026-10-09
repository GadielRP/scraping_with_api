# Entradas y expedientes del flujo de pilares

[Guía funcional del flujo](guia-funcional/00-flujo-principal.md) · [Evaluación de mercados](market-evaluation-v1.md) · [Conservación de resultados](mining-persistence.md)

Documento contrastado con el código el 8 de octubre de 2026. Describe los datos que viajan entre la preparación de un partido y sus pilares. Los nombres del código se conservan para reconocerlos; cada campo se explica en español.

Un **objeto** es un expediente con campos relacionados. Un **contrato de entrada** define qué contiene ese expediente y cómo debe interpretarse. Un **diccionario** permite localizar datos por una clave, por ejemplo el identificador del partido. `None` o `null` significa «dato no disponible»; una lista vacía significa «ningún elemento». Ninguno de ellos equivale a una puntuación cero.

## 1. Recorrido y responsabilidad de cada expediente

El coordinador está en [pillar_pipeline.py](../../modules/jobs/pre_start_check_job/pillar_pipeline.py). El [recorrido de alertas](../../modules/jobs/pre_start_check_job/alert_pipeline.py) comparte el expediente del evento y puede preparar el historial deportivo antes de P1.

| Expediente | Función |
|---|---|
| `EventContext` | Partido completo, participantes, competición, observaciones y análisis deportivo. P1 lo utiliza directamente. |
| `OddsTrajectoryPoint` | Una fila de cuota histórica leída de la base de datos. |
| `EventIdentity` | Ficha breve de identidad del partido. P2–P5 y la minería de mercados la utilizan. |
| `OddsTrajectoryContext` | Cuotas del partido organizadas por mercado, periodo, línea, casa y resultado. |
| `TargetMinuteSelection` | Un minuto de mercado elegido para la evaluación compartida. |
| `EventMarketEvaluation` | Contratos admitidos, periodo de tiempo completo y evidencia de selección para P2–P5. |

El flujo actual hace lo siguiente:

1. Construye y completa `EventContext`. En este recorrido, `odds_trajectory` comienza vacío.
2. Carga por lote un diccionario `trajectories_by_event_id`: identificador de evento → lista de `OddsTrajectoryPoint`.
3. Después de esa lectura fija `evaluation_as_of`, el corte UTC compartido de la evaluación.
4. Al comenzar un partido, su procesador retira la lista del diccionario del lote y construye un `OddsTrajectoryContext` local. Utiliza inicio, minutos restantes y corte.
5. Obtiene `EventIdentity` mediante `to_identity()`, elige un `TargetMinuteSelection` y prepara un `EventMarketEvaluation`.
6. P2, P3, P4 y P5 reciben los mismos objetos compartidos. Cada uno calcula sus lecturas.
7. Antes de P1 libera las referencias al historial crudo de cuotas. P1 continúa con `EventContext` y `streak_analysis`.

Se organiza el historial una vez por partido. Las vistas de los pilares conservan referencias a los datos; no reconstruyen cuatro historias independientes. El contexto de mercado queda local al procesador y no se instala en `EventContext` en este recorrido de producción.

## 2. EventContext: expediente completo del encuentro

Definido en [context.py](../../modules/pillars/context.py). Puede completarse durante el recorrido, por ejemplo al obtener metadatos de competición o historia deportiva.

### Identidad, participantes y tiempo

| Campo | Significado |
|---|---|
| `event_id` | Identificador interno del partido. Debe corresponder al de sus cuotas. |
| `custom_id` | Identidad auxiliar utilizada para consultar enfrentamientos. |
| `sport` | Deporte. |
| `season_id`, `season_name`, `season_year` | Identificador, nombre y año de temporada. |
| `starts_at` | Inicio con zona horaria, normalizado a UTC. |
| `minutes_until_start` | Minutos restantes en la evaluación. Un valor negativo indica un instante posterior al inicio. |
| `discovery_source` | Fuente con la que se descubrió el encuentro. |
| `home`, `away` | Expedientes del participante local y visitante. |
| `competition` | Expediente de competición. |
| `participants_label` | Texto de presentación, por ejemplo «A vs B». |
| `context_status` | Procedencia de preparación: `normalized`, `mixed` o `legacy_compat`. Describe datos normalizados, mezclados o recuperados por compatibilidad. |
| `slug` | Nombre preparado para identificar el encuentro en rutas o fuentes. |
| `gender`, `country`, `round` | Categoría de género, país y fase deportiva. |
| `created_at`, `updated_at` | Fechas de creación y actualización del registro. |

### Historia, observaciones y continuidad

| Campo | Significado |
|---|---|
| `observations` | Datos adicionales del encuentro, como superficie o recinto. |
| `odds_trajectory` | Historial alternativo para llamadores anteriores. El recorrido por lotes actual pasa las filas por separado. |
| `odds_trajectory_context`, `ft_1x2_odds_trajectory_context` | Campos de compatibilidad para contextos de cuotas. El recorrido actual mantiene su contexto de mercado local y no los completa. |
| `streak_analysis` | Historia deportiva preparada: resultados, enfrentamientos, clasificación y referencias de liga. Es la entrada deportiva de P1. |
| `should_send_streak_alert` | Decisión del recorrido de alertas sobre participación de una alerta de rachas. |
| `dual_report` | Reporte del análisis de alertas de doble proceso, cuando existe. |
| `competition_metadata_resolved` | Indica si ya se intentó completar metadatos de competición. |
| `success` | Si se pudo preparar el expediente para continuar. |
| `alert_sent` | Si el recorrido correspondiente envió una alerta. |

Las banderas de alertas y preparación no representan probabilidades ni pesos deportivos. El expediente actual **no contiene `odds_response`**: no debe construirse una segunda entrada de cuotas basada en ese campo antiguo.

## 3. ParticipantContext y CompetitionContext

### Participante

`ParticipantContext` se utiliza tanto en `home` como en `away`.

| Campo | Significado |
|---|---|
| `participant_id` | Identidad interna del participante. |
| `source`, `source_participant_id` | Fuente e identidad del participante en ella. |
| `name` | Nombre principal. |
| `slug`, `short_name`, `code_name` | Nombre para rutas, nombre corto y código de presentación. |
| `source_status` | Estado de resolución de la identidad de fuente. |
| `snapshot_ranking` | Posición recogida en la preparación del encuentro, cuando existe. |
| `created_at`, `updated_at` | Fechas del registro. |

### Competición

`CompetitionContext` reúne identidad de torneo, estructura de temporada y procedencia.

| Campo | Significado |
|---|---|
| `competition_id` | Identidad interna de competición. |
| `source` | Fuente de los datos. |
| `source_tournament_id`, `source_unique_tournament_id` | Identidades de torneo y agrupación de torneo en la fuente. |
| `canonical_name`, `display_name` | Nombre común normalizado y nombre presentado. |
| `slug`, `unique_slug` | Identidades textuales de torneo y agrupación. |
| `category_id`, `category_name` | Identificador y nombre de categoría. |
| `number_of_teams` | Cantidad de equipos. |
| `number_of_teams_source` | De dónde proviene esa cantidad. |
| `total_regular_season_games` | Partidos previstos por equipo en temporada regular. P1 Totales construye sus ventanas con este dato. |
| `standings_grouping` | Organización de clasificación, por ejemplo grupos o tabla conjunta. |
| `league_config_source` | Procedencia de las reglas descriptivas de la liga. |
| `has_standings_source_endpoint` | Si la fuente dispone de una consulta de clasificación conocida. |
| `source_status` | Estado de resolución de la identidad de fuente. |
| `standings_response` | Clasificación disponible para construir el contexto deportivo. |
| `source_tournament_name`, `source_unique_tournament_name` | Nombres originales del torneo y su agrupación. |
| `created_at`, `updated_at` | Fechas del registro. |

`NumberOfTeamsSummary` es un resumen informativo adicional: `unique_team_count` cuenta equipos únicos y `inferred_number_of_teams` expresa una posible cantidad inferida. En el pipeline este resumen se conserva para explicación; no sustituye automáticamente el dato oficial de competición ni se presenta como una cantidad inferida aplicada a todo P1.

## 4. EventIdentity: ficha breve para P2–P5

`EventContext.to_identity()` proyecta una ficha inmutable: no se modifica después de crearla. Conserva exactamente estos campos:

| Campo | Contenido |
|---|---|
| `event_id`, `participants_label` | Identidad y texto del encuentro. |
| `starts_at`, `minutes_until_start` | Inicio y minutos de evaluación. |
| `sport`, `round` | Deporte y fase. |
| `competition_id`, `competition_name` | Identidad y nombre de competición. Para el nombre prioriza el presentado y después el canónico. |
| `season_id`, `country` | Temporada y país. |
| `context_status` | Procedencia copiada del expediente completo. |

El valor predeterminado del campo `context_status` en una ficha creada directamente es `VALID`; la proyección del pipeline copia el estado de su `EventContext`.

Esta ficha no transporta objetos completos de participantes, historia deportiva ni cuotas. P2–P5 también pueden aceptar `EventContext` en sus entradas independientes, pero el procesador actual les entrega `EventIdentity`. P1 necesita el expediente completo.

## 5. OddsTrajectoryPoint: fila original del historial

Definido en el [repositorio de trayectorias](../../infrastructure/persistence/repositories/odds_trajectory_repository.py). El repositorio agrupa sus objetos por evento; el constructor de contexto puede leer sus atributos directamente y admite diccionarios de llamadores anteriores.

| Campo | Significado |
|---|---|
| `event_id` | Encuentro de la observación. |
| `market_id` | Mercado identificado en la fuente. |
| `canonical_market_key`, `market_type_id` | Clave e identificador del tipo canónico. |
| `market_family`, `market_group` | Familia y agrupación de mercado. |
| `market_display_order`, `market_name` | Orden de presentación y nombre del mercado. |
| `market_period` | Periodo deportivo. |
| `line_value` | Línea numérica del contrato; puede faltar en ganador. |
| `is_live` | Si el contrato corresponde a juego en curso; el valor predeterminado es falso. |
| `bookie_id`, `bookie_name` | Casa que publica el precio. |
| `choice_id`, `choice_name`, `choice_display_order` | Identidad, nombre y orden del resultado del contrato. |
| `quote_id` | Cotización de la que procede la observación. |
| `source` | Proveedor. |
| `exchange_side`, `exchange_level` | BACK/LAY y nivel del exchange, cuando corresponde. |
| `initial_odds` | Precio inicial disponible; no garantiza por sí solo un momento específico como T−120. |
| `odds_value` | Precio de esta observación. |
| `snapshot_id` | Identidad de la observación guardada. |
| `source_collected_at` | Momento informado por el proveedor. |
| `collected_at` | Momento de recogida asignado a la observación guardada. |
| `observed_minutes_before_start` | Minutos restantes redondeados para proyección y consumidores anteriores. |
| `trajectory_minutes_before_start` | Minutos restantes con precisión decimal en el eje del proveedor, usando recogida como alternativa. |
| `main_line` | Si la cotización fue identificada como línea principal. Puede faltar. |
| `source_limit` | Límite publicado por la fuente, si existe. |
| `exchange_size` | Cantidad publicada disponible en ese nivel del exchange. |

`Decimal` representa cantidades decimales, como cuotas, líneas o minutos precisos. No es un porcentaje por ser decimal.

Los minutos precisos de la fila y los del contexto preparado no deben intercambiarse sin considerar sus relojes: el contexto acota el momento del proveedor por la recogida, como se explica en la sección 7.

## 6. OddsTrajectoryContext: historial organizado y compartido

Definido en [odds_trajectory_context.py](../../modules/pillars/odds_trajectory_context.py).

| Campo | Significado |
|---|---|
| `available` | Existe alguna observación utilizable en el contexto; no certifica todos los momentos ni todas las lecturas. |
| `event_id` | Encuentro al que pertenece. |
| `target_minutes_expected` | Momentos esperados. |
| `target_minutes_present` | Momentos que tienen alguna lectura proyectada. |
| `missing_target_minutes` | Esperados sin una lectura proyectada. |
| `markets` | Árbol de cuotas organizado. |
| `evaluation_as_of` | Corte de disponibilidad de esta evaluación. |
| `market_index` | Índice interno para localizar mercados por familia y periodo; no añade información deportiva. |

La estructura es:

```text
familia → periodo → nombre de mercado → línea
    → casa y procedencia → resultado
        → precios por minuto y observaciones históricas
```

Cuando no hay línea, la clave de agrupación general es `__default__`. Las vistas específicas de evaluación pueden emplear claves más detalladas para mantener separados contratos y mercados de origen.

### 6.1. Mercado y casa

`MarketLineOddsTrajectory` contiene `market_id`, `market_name`, `market_group`, `market_period`, `line_value`, `bookies`, `canonical_market_key`, `market_type_id` e `is_live`: mercado, familia, periodo, línea, casas, tipo canónico y condición de juego.

`BookieOddsTrajectory` contiene `bookie_id`, `bookie_name`, `source`, `exchange_side`, `exchange_level`, `choices` y `market_id`. El último campo identifica el mercado de origen de esa casa dentro del agrupamiento.

El valor predeterminado de `source` en ese objeto es `sofascore`, pero cada observación preparada puede aportar otra fuente. Ese valor predeterminado no convierte a SofaScore en una casa admitida por P2–P5. `exchange_level` comienza en 0; para una casa ordinaria, `exchange_side` queda ausente.

### 6.2. Resultado del contrato y dos vistas de sus cuotas

`ChoiceOddsTrajectory` contiene:

| Campo | Contenido |
|---|---|
| `choice_name`, `choice_id` | Resultado e identidad. |
| `initial_odds`, `quote_id`, `main_line` | Precio inicial disponible, cotización y condición de línea principal. |
| `odds_values` | Minuto objetivo → precio elegido. |
| `meta_by_minute` | Minuto objetivo → procedencia del precio elegido. |
| `snapshots` | Observaciones del historial de ese resultado. |

Los mapas por minuto representan checkpoints, por ejemplo 120, 30 y 5. `snapshots` conserva el recorrido disponible, incluidos datos intermedios. P2, P3 y P5 leen la proyección del momento seleccionado; P4 utiliza también el historial temporal.

Los mapas se conservan con minutos descendentes. Las observaciones se ordenan por momento efectivo, recogida e identificador de observación. Este orden facilita una salida determinista: los mismos datos producen el mismo orden.

### 6.3. Procedencia del checkpoint y observación histórica

`OddsPointMeta` conserva:

| Campo | Significado |
|---|---|
| `snapshot_id`, `quote_id` | Observación y cotización elegidas. |
| `collected_at`, `changed_at` | Recogida y momento de cambio informado por el proveedor. |
| `minutes_before_start` | Minutos observados redondeados. |
| `target_minute` | Checkpoint representado. |
| `distance_from_target` | Distancia con precisión decimal al checkpoint, en minutos. |
| `exchange_size` | Cantidad del exchange de esa lectura, si existe. |

La distancia actual es decimal; no debe documentarse como un entero.

`OddsSnapshotPoint` conserva `snapshot_id`, `quote_id`, `odds_value`, `collected_at`, `source_collected_at`, `minutes_before_start`, `source_limit` y `exchange_size`: valor histórico, tiempo preciso y procedencia, con cantidad y límite opcionales.

Los filtros `filter_by_market_groups`, `filter_by_market_period` y `filter_by_bookie_ids` obtienen vistas por familia, periodo o casa sin modificar el historial de origen.

## 7. Momento elegido y disponibilidad temporal

La [política común](market-evaluation-v1.md#4-selección-temporal-compartida) explica la regla completa. Sus operaciones esenciales son:

**instante nominal = starts_at − target_minute minutos.**

**momento efectivo = mínimo(source_collected_at, collected_at)**, cuando hay momento de proveedor; en otro caso, `collected_at`.

**minutos precisos del contexto = segundos entre inicio y momento efectivo ÷ 60.**

`collected_at` define disponibilidad y cercanía al checkpoint. `source_collected_at` sitúa el cambio de mercado, acotado por esa recogida. Una observación sin recogida no se puede situar en el eje compartido.

`TargetMinuteSelection` reúne `target_minute`, `reason` y `diagnostics`. Entre minutos presentes y permitidos elige el menor que sea mayor o igual a `evaluation_minute`. A T−5 puede elegir 5 o un momento anterior, como 30; no utiliza T−1.

`SnapshotTargetWindow` reúne `target_minute`, `nominal_at`, `earliest_at` y `latest_at`: etiqueta de momento, instante nominal e inicio/fin de ventana. La ventana incluye la tolerancia y se limita por lo que podía conocerse al evaluar.

## 8. EventMarketEvaluation: plan compartido de mercados

| Campo | Significado |
|---|---|
| `context` | Historial organizado de origen. |
| `target_selection` | Momento común. |
| `lines` | Contratos reconocidos con casas admitidas. |
| `selected_full_time_period` | Tiempo completo seleccionado, con prórroga o reglamentario. |
| `selection` | Candidatos, lecturas utilizables, motivos y checkpoint. |
| `diagnostics` | Explicación de lo que no pudo admitirse. |
| `rejected_lines` | Contratos no admitidos, conservados para explicar su exclusión. |

`view(pillar)` entrega la vista aplicable a un pilar; `contracts(pillar)`, las identidades que puede referenciar; `coverage(pillar)`, la disponibilidad por combinación.

La selección prioriza tiempo completo con prórroga si permite alguna lectura soportada y actual; en otro caso considera el reglamentario. Todos reciben el elegido. Otros periodos permanecen independientes. El número de antecedentes de P5 no interviene en esta decisión.

## 9. Objetos de extracción de una lectura

El [extractor común](../../modules/pillars/market_snapshot_extractor.py) obtiene ingredientes de checkpoints sin consultar otra vez el repositorio.

| Objeto | Campos y función |
|---|---|
| `MarketIdentity` | `market_group`, `market_period`, `market_name`: familia, periodo y nombre solicitado. |
| `ChoiceRequest` | `key`, `choice_name`, `input_name`, `exchange_size_input_name`: resultado solicitado y nombres de sus ingredientes. |
| `MarketSnapshotRequest` | `identities`, `bookie_id`, `choices`, `line_input_name`, `exchange_side`, `exchange_level`: lectura que se pide. |
| `QuotePoint` | `odds_price`, `exchange_size`, `trace`: precio, cantidad opcional y procedencia. |
| `MarketCandidate` | `market_line`, `bookie`, `line`, `choices`: contrato y casa candidatos con ingredientes disponibles. |
| `MarketSnapshotExtraction` | `target_minute`, `candidates`, `missing_inputs`, `invalid_inputs`, `ambiguous_inputs`, `container_ambiguities`: candidatos y explicación de disponibilidad. |

`QuoteTrace`, el expediente de procedencia, contiene `target_minute`, `snapshot_id`, `collected_at`, `changed_at`, `minutes_before_start`, `quote_id`, `market_group`, `market_period`, `market_name`, `line_value`, `bookie_id`, `bookie_name`, `source`, `exchange_side`, `exchange_level`, `choice_name`, `canonical_market_key`, `market_type_id`, `is_live`, `market_id` y `source_collected_at`.

Son los mismos conceptos de identidad y tiempo descritos arriba, unidos al precio elegido. Permiten explicar de qué observación salió cada ingrediente.

## 10. Entradas temporales de P4

El [muestreador compartido](../../modules/pillars/trajectory_sampling.py) define:

| Objeto | Contenido |
|---|---|
| `TrajectoryPoint` | Observación temporal con valor, dos relojes, identidades y metadatos opcionales. |
| `TrajectoryPointValue` | Valor derivado con `original`, `value` y `value_type`; comparte procedencia con el punto original. |
| `TrajectorySample` | Puntos válidos, checkpoints, vista adaptativa, endpoint y diagnóstico. |
| `TrajectorySamplingPolicy` | Inicio, hora de evaluación, minuto elegido y ventanas comunes. |

Los campos de `TrajectoryPoint` son:

| Campos | Significado |
|---|---|
| `point_id`, `value` | Identidad del punto y cantidad seguida, como cuota o inverso de cuota. |
| `effective_at`, `availability_at` | Momento del cambio y momento en que la observación estaba disponible. |
| `minutes_before_start` | Posición temporal precisa frente al inicio. |
| `snapshot_id`, `quote_id` | Observación y cotización originales. |
| `collected_at`, `source_collected_at` | Relojes originales conservados. |
| `source_limit`, `exchange_size` | Límite y cantidad opcionales. |
| `observation_kind` | Clase de observación; inicialmente `PERSISTED_SNAPSHOT`, una observación guardada. |
| `target_minute`, `distance_from_target_minutes` | Checkpoint y distancia añadidos al proyectar ese punto. |

`TrajectorySample` reúne `points`, `checkpoints`, `adaptive`, `endpoint`, `invalid`, `excluded_future_count` y `first_after_cutoff_at`: observaciones ordenadas, selección por momento, recorrido hasta el endpoint, explicación de inválidas y datos posteriores al corte. `has_movement` exige endpoint y al menos dos observaciones en una vista temporal disponible.

`TrajectorySamplingPolicy` conserva `event_start`, `evaluation_as_of`, `target_minute` y `windows`. La vista adaptativa termina en el endpoint seleccionado, manteniendo puntos cuyo momento efectivo no lo supera y cuya disponibilidad no supera la evaluación.

Ejemplo: partido a las 03:10:00, checkpoint T−5 a las 03:05:00 y evaluación a las 03:05:16. Una observación recogida a las 03:05:15 puede representar el endpoint dentro de tolerancia; una recogida a las 03:05:45 queda fuera de lo conocido. Un cambio más antiguo recogido después del endpoint puede entrar si ya estaba disponible antes de las 03:05:16.

El resultado distingue `nominal_target_as_of`, `evaluation_as_of` y `operative_as_of`: nominal, evaluación y límite operativo. Si falta endpoint o solo hay un punto, no se inventa movimiento cero.

Los [objetos propios de P4](../../modules/pillars/pillar_4/models.py) añaden `P4SeriesInput`, identidad de la serie y sus puntos/ingredientes, y `P4ExtractionResult`, series y diagnósticos. Que existan series para inspeccionar no garantiza movimiento calculable.

## 11. Qué recibe y utiliza cada pilar

| Pilar | Identidad y datos |
|---|---|
| P1 | EventContext completo y su streak_analysis. Las cuotas crudas ya se liberaron. |
| P2 y P3 | EventIdentity, historial organizado, selección temporal y evaluación de mercados. Extraen los ingredientes del checkpoint seleccionado. |
| P4 | Los mismos objetos compartidos; además transforma las observaciones en series de checkpoints y adaptativas acotadas. |
| P5 | Los objetos de mercados y un `PriceMemoryReader`, interfaz para consultar memoria histórica de otros encuentros. |

P5 utiliza un vector propio por casa: 1X2 exige local, empate y visitante; Home/Away exige local y visitante. Betfair puede aportar precios, cantidades y procedencia con `role = DIAGNOSTIC` y `participates_in_score = false`; no produce una señal de memoria ni vuelve exitoso el pilar.

P5 puede consultar otros partidos para su memoria exacta. Esa consulta no reconstruye ni sustituye la trayectoria actual del encuentro.

## 12. Ausencias, estados y lectura posterior

Las entradas pueden ser:

- **Ausentes:** falta mercado, resultado, línea, momento o cantidad requerida.
- **Inválidas:** existe un valor que no cumple su contrato, como precio no finito o menor o igual a 1.
- **Ambiguas:** más de un candidato impide identificar una lectura única.

El resultado conserva esos motivos y el minuto elegido. Las señales mantienen `input_refs` y `contract_refs` para recorrer su procedencia. P4 conserva también las referencias de series constituyentes.

P2–P5 usan `EvaluationResult` formato 4. ACTIVE significa alguna señal calculada, incluido cero; INSUFFICIENT_DATA, ninguna calculada sin errores; ERROR, ninguna calculada con errores; SKIPPED, ausencia de participación. La [política de mercados](market-evaluation-v1.md#7-cobertura-y-estados) desarrolla estos estados y la cobertura.

La conservación mantiene ingredientes, contratos, señales y explicaciones. Consulte [mining-persistence.md](mining-persistence.md) para entender cómo se guardan y se vuelven a leer.

## 13. Fuentes y lectura relacionada

Fuentes principales: [context.py](../../modules/pillars/context.py), [key_moment_evaluation.py](../../modules/jobs/pre_start_check_job/key_moment_evaluation.py), [pillar_pipeline.py](../../modules/jobs/pre_start_check_job/pillar_pipeline.py), [repositorio de trayectorias](../../infrastructure/persistence/repositories/odds_trajectory_repository.py), [odds_trajectory_context.py](../../modules/pillars/odds_trajectory_context.py), [trajectory_selection.py](../../modules/pillars/trajectory_selection.py), [trajectory_sampling.py](../../modules/pillars/trajectory_sampling.py), [market_evaluation.py](../../modules/pillars/market_evaluation.py) y [market_snapshot_extractor.py](../../modules/pillars/market_snapshot_extractor.py).

La [guía principal](guia-funcional/00-flujo-principal.md) explica el recorrido completo; [P1](guia-funcional/01-pilar-1-estructura-deportiva.md) desarrolla las entradas deportivas. La [recopilación de cuotas](../providers/pre_start_odds_ingestion.md) explica qué historia entregan los proveedores y por qué no debe suponerse una sucesión completa de todas las líneas del pasado.
