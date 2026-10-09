# Pilar 4: movimiento temporal del mercado

[Volver a la guía principal](00-flujo-principal.md) · [P1: estructura](01-pilar-1-estructura-deportiva.md) · [P2: lado](02-pilar-2-mercado-de-lado.md) · [P3: totales](03-pilar-3-mercado-de-totales.md) · [P5: memoria](05-pilar-5-memoria-de-precios.md)

P4, motor `p4-signal-profile-v3` revisado el 9 de octubre de 2026, estudia **cómo se movieron precios, líneas y lecturas derivadas hasta el momento evaluado**. Conserva magnitud, dirección, recorrido, cambios de sentido y concentración temporal. No crea un único score de «dinero informado» ni demuestra quién originó un movimiento.

Para seguir cómo se utilizan conjuntamente las definiciones y fórmulas, lea el [recorrido integrado del dato al resultado](#recorrido-integrado-del-dato-al-resultado), con pasos, ejemplo y lectura final.

## 1. De una foto a una película

P2 y P3 estudian una lectura de mercado en un momento. P4 construye **series**: varios valores del mismo dato ordenados en el tiempo.

Un **punto** es una observación con precio u otro valor, hora y procedencia. Un **tramo**, `leg`, une dos puntos consecutivos. Una **racha direccional**, `directional run`, reúne tramos continuos del mismo sentido.

P4 admite 1X2, Home/Away, Asian Handicap y Over/Under en sus periodos soportados. Incluye tiempo completo seleccionado, primera mitad y, para totales, primer cuarto. El hándicap estándar no pertenece a su dominio actual. Las casas son Pinnacle, bet365 y Betfair.

Mantiene separadas las series por mercado, periodo, línea, casa, proveedor, resultado, lado y nivel del exchange. Una cuota local de línea −0.5 no se añade a una serie de precios local de línea −1.5.

## 2. Entrada, momento y dos vistas

Recibe identidad del evento, historial organizado, momento elegido y selección común. El instante nominal es el inicio menos el minuto seleccionado. El límite operativo admite la tolerancia y se acota por los datos disponibles en la ejecución. Es la misma política de momentos utilizada por P2, P3 y P5.

Hay dos vistas:

| Vista | Significado |
|---|---|
| `CHECKPOINT_VIEW` | Una observación seleccionada alrededor de cada momento esperado: por ejemplo 120, 30 y 5 minutos. |
| `ADAPTIVE_VIEW` | Observaciones guardadas disponibles hasta el endpoint efectivo, incluidas las intermedias. |

Un **endpoint operativo** es la observación que representa el último momento evaluado para esa serie. Sin endpoint no se informa movimiento calculado. Se necesitan al menos dos observaciones para una métrica temporal calculable.

La selección utiliza cuándo se recogieron los datos. Las duraciones conservan cuándo fueron efectivos en el mercado. Si el proveedor informa un momento posterior a la recogida, no se coloca el precio después de esa recogida. Esto distingue la hora del cambio de la hora en que el programa pudo conocerlo.

En la vista adaptativa, el endpoint delimita hasta qué estado del mercado se observa; la hora de evaluación delimita qué datos recogidos estaban disponibles.

## 3. Qué valores convierte en series

| `VALUE_TYPE` | Valor seguido |
|---|---|
| `ODDS_PRICE` | Cuota decimal original. |
| `IMPLIED_PROBABILITY_RAW` | Inverso de la cuota: 1 ÷ precio. Es un peso implícito bruto, sin ajuste de margen. |
| `SOURCE_LIMIT` | Límite publicado por la fuente, si existe y es válido. |
| `EXCHANGE_SIZE` | Cantidad disponible en el nivel del exchange. |
| `LINE` | Línea del mercado observada en checkpoints. |
| `SIDE_EDGE` | Diferencia relativa de inversos local/visitante. |
| `OU_EDGE` | Diferencia relativa de inversos Over/Under. |
| `BOOK_REP_EDGE` | Media de edges de Pinnacle y bet365. |
| `BOOK_INTERNAL_GAP` | Separación absoluta entre esos edges. |
| `EXCHANGE_REP_EDGE` | Media de edges BACK y LAY. |
| `EXCHANGE_INTERNAL_GAP` | Separación absoluta de edges BACK y LAY. |
| `BOOK_EXCHANGE_GAP` | Separación absoluta entre representantes de casas y exchange. |
| `BACK_LAY_RELATIVE_SPREAD` | (precio LAY − precio BACK) ÷ media de ambos precios. |

Las series derivadas se construyen en checkpoints comunes de sus ingredientes y conservan referencias a ellos. No se fabrican puntos de una casa para completar otra.

Los tamaños del exchange conservan cambios, pero el motor deja sin valor sus velocidades, aceleración y desaceleración. No presenta un cambio de cantidad como si fuera velocidad de precio.

## 4. Fórmulas de cada tramo

Sean valor inicial A, valor final B y sus horas efectivas:

**DELTA_RAW = B − A.**

**ABS_DELTA_RAW = valor absoluto de DELTA_RAW.**

**ELAPSED_MINUTES_ACTUAL = segundos entre las horas ÷ 60.**

**VELOCITY_RAW = DELTA_RAW ÷ minutos transcurridos**, cuando el tiempo es positivo y el tramo es continuo.

La dirección es POSITIVE si el delta es positivo, NEGATIVE si es negativo y ZERO si es cero. `DIRECTION_BY_SIGN` representa lo mismo como +1, −1 o 0.

En checkpoints, continuidad significa que se conectan momentos esperados consecutivos. Si se esperaba 120 → 30 → 5 y falta 30, el salto 120 → 5 no se trata como un tramo continuo para velocidad y recorrido completo.

## 5. Cambio neto, recorrido y eficiencia

| Campo | Fórmula o significado |
|---|---|
| `OBSERVATION_COUNT` | Número de puntos disponibles. |
| `LEG_COUNT` | Número de pares consecutivos. |
| `NET_MOVE_RAW` | Último valor menos primer valor. |
| `PATH_LENGTH_RAW` | Suma de cambios absolutos de los tramos continuos observados. |
| `PATH_EFFICIENCY_RAW` | Valor absoluto del cambio neto ÷ recorrido, solo con recorrido completo y distinto de cero. |
| `NO_MOVEMENT_RAW` | Verdadero si el recorrido es completo y su longitud es cero. |
| `ELAPSED_MINUTES_ACTUAL` | Tiempo efectivo entre primer y último punto. |
| `VELOCITY_RAW` global | Cambio neto ÷ duración, solo con recorrido completo y duración positiva. |
| `GAP_PRESENT_RAW` | Si faltan checkpoints o continuidad. |
| `SIGN_SEQUENCE_RAW` | Signos de todos los tramos observados. |

Ejemplo: cuota 2.00 → 1.90 → 1.95, con 90 minutos en el primer tramo y 25 en el segundo.

- Deltas: −0.10 y +0.05.
- Velocidades: −0.10 ÷ 90 ≈ −0.001111 por minuto y +0.05 ÷ 25 = 0.002 por minuto.
- Cambio neto: −0.05.
- Recorrido: 0.10 + 0.05 = 0.15.
- Eficiencia: 0.05 ÷ 0.15 ≈ 0.3333.
- Velocidad global: −0.05 ÷ 115 ≈ −0.000435 por minuto.

El precio bajó globalmente, pero parte del movimiento inicial se corrigió. Si la serie estudiada fuera el inverso del precio, una caída del precio produciría un aumento de ese inverso. Hay que leer el tipo de valor antes de interpretar el signo.

La eficiencia se acerca a 1 cuando el movimiento mantiene el sentido. Si ida y vuelta se cancelan y el recorrido es positivo, puede ser 0. Si el recorrido es cero, la eficiencia queda ausente: no se divide entre cero.

Cuando existe un hueco, P4 puede conservar el cambio neto entre extremos y la suma de tramos continuos observados, pero no certifica un recorrido completo ni calcula su eficiencia o velocidad global.

## 6. Patrones, rachas y puntos de giro

`PATH_PATTERN_RAW` ignora los tramos de cambio cero para contar cambios de signo:

| Etiqueta | Significado |
|---|---|
| `NO_MOVEMENT` | Ningún tramo con movimiento. |
| `UNIDIRECTIONAL` | Todos los movimientos no nulos tienen el mismo sentido. |
| `REVERSAL` | Un cambio de sentido. |
| `MULTI_REVERSAL` | Más de un cambio de sentido. |

Si el recorrido no es completo, el patrón global queda ausente.

`DIRECTIONAL_RUNS` reúne las rachas observadas por segmento continuo. Cada una conserva identificador, dirección, cambio total, tramos, puntos y tiempos de inicio/fin. Un tramo plano puede quedar dentro de una racha ya iniciada.

`NET_DIRECTION_RAW` describe el signo del cambio entre extremos. `FINAL_RUN_DIRECTION_RAW` describe el sentido de la última racha. Pueden diferir: un descenso grande seguido de una subida pequeña termina con cambio neto negativo y última racha positiva.

`TURNING_STRUCTURE_RAW` conserva:

- `SIGN_CHANGE_COUNT_RAW`: cantidad de giros con recorrido completo.
- `ZERO_BRIDGED_SIGN_CHANGE`: si un giro atravesó una zona plana.
- `ZONES`: picos (`PEAK`) o valles (`TROUGH`), como punto (`POINT`) o meseta (`PLATEAU`), con sus referencias.

Con huecos conserva los giros observados en segmentos continuos, pero no los presenta como conteo global completo.

## 7. Corrección, retención y sobrepaso

Se toma la primera racha y la primera posterior de dirección contraria:

**CORRECTION_RATIO_RAW = magnitud de la corrección ÷ magnitud de la racha inicial.**

**MOVE_RETENTION_RAW = máximo entre 0 y (1 − ratio de corrección).**

**OVERSHOOT_RAW = verdadero cuando el ratio es mayor que 1.**

**OVERSHOOT_MAGNITUDE_RAW = magnitud de corrección − magnitud inicial**, solo si hubo sobrepaso; en otro caso es cero.

En el ejemplo de la sección 5, la caída inicial fue 0.10 y la recuperación 0.05. El ratio es 0.5 y la retención 0.5. Si la recuperación fuera 0.15, el ratio sería 1.5 y el sobrepaso 0.05.

`CORRECTION_RAW` incluye primera racha, corrección y fases posteriores. No suma todas las fases posteriores para redefinir esa primera corrección.

Sus estados son `NO_DIRECTIONAL_RUN` cuando no hubo rachas, `NO_CORRECTION` cuando no hubo racha contraria, `CORRECTION` o `OVERSHOOT`. Sin recorrido completo, el estado queda ausente.

## 8. Aceleración y desaceleración

Entre velocidades de tramos continuos:

**VELOCITY_CHANGE_RAW = velocidad actual − velocidad anterior.**

**ABS_VELOCITY_CHANGE_RAW = magnitud de velocidad actual − magnitud de velocidad anterior.**

Aceleración significa mismo sentido no nulo y aumento de magnitud. Desaceleración significa mismo sentido y reducción de magnitud. Una inversión de signos se marca como `REVERSAL_RAW`.

Esto compara velocidades de tramos; no vuelve a dividir la diferencia entre otra duración. Por tanto no es una aceleración física en unidades por minuto al cuadrado.

Las banderas globales indican si hubo alguna aceleración o desaceleración observada. Si no existen pares de velocidades comparables, quedan ausentes.

## 9. Dónde se concentró el movimiento

Cada tramo continuo se asigna según su punto final:

| Ventana | Minutos restantes del punto final |
|---|---|
| `EARLY_WINDOW` | Desde 360 hasta 120, ambos incluidos. |
| `DEVELOPMENT_WINDOW` | Desde 120 excluido hasta 30 incluido. |
| `LATE_WINDOW` | Menos de 30, hasta el momento operativo incluido. |

`WINDOWS` conserva valor inicial/final, cambio neto, recorrido, puntos, tramos, giros, duración, dirección, patrón y referencias por ventana.

**MOVE_SHARE_BY_WINDOW = recorrido de la ventana ÷ recorrido observado.**

**MOVE_SHARE_RAW de un tramo = magnitud de ese tramo ÷ recorrido observado.**

Si el recorrido es cero, estas participaciones quedan ausentes. Un tramo que atraviesa ventanas se asigna a la de su punto final; no reparte su movimiento por duración entre ventanas.

`DOMINANT_SEGMENTS_RAW` son los tramos con mayor cambio absoluto y `DOMINANT_WINDOWS_RAW`, las ventanas con mayor recorrido. Si empatan, conserva todas las dominantes. Estas etiquetas no identifican participantes del mercado.

## 10. Cómo conserva cambios de línea

P4 construye una serie LINE cuando puede identificar una línea única en cada checkpoint. Si hay dos líneas candidatas incompatibles para la misma lectura, registra la ambigüedad.

Una transición observada de 2.5 a 3.0 es un movimiento de línea +0.5. Las cuotas de 2.5 y de 3.0 continúan en series de precio separadas.

Si una línea anterior no llega al endpoint y existe una lectura final de otra línea para ese resultado, la serie anterior puede clasificarse como `CONTRACT_ENDED`. Conserva antecedentes, pero no comunica movimiento cero. Si tampoco hay una transición final identificable, se registra endpoint ausente.

El historial recibido refleja los resultados de la línea principal que la recopilación logró reconstruir. No observar una transición no demuestra que la línea real nunca cambiara.

## 11. Relaciones casas–exchange a lo largo del tiempo

Se construyen representantes de casas y exchange en checkpoints comunes del mismo contrato. La comparación utiliza el primer y último checkpoint comunes:

**INITIAL_GAP_RAW = diferencia absoluta inicial de representantes.**

**FINAL_GAP_RAW = diferencia absoluta final.**

**GAP_CHANGE_RAW = gap final − gap inicial.**

Una diferencia positiva significa que aumentó la separación; negativa, que disminuyó.

Las relaciones son `ALIGNED_POSITIVE`, `ALIGNED_NEGATIVE` o `ALIGNED_ZERO` si comparten signo; `ONE_SIDE_ZERO` si solo uno es cero; `OPPOSED` si son opuestos. `STATE_CHANGED_RAW` indica si la etiqueta inicial y final difieren.

Es una comparación de edges: POSITIVE representa local en SIDE y Over en TOTALS. No debe interpretarse como «precio subió» sin leer el tipo de serie.

## 12. Objetos, procedencia y paquete de salida

| Campo u objeto | Significado |
|---|---|
| `TrajectoryPoint` | Valor con momento efectivo, disponibilidad, momento del proveedor, recogida, quote/snapshot, tamaño y límite opcionales. |
| `point_id`, `snapshot_id`, `quote_id` | Identidades para localizar observación y cotización. |
| `target_minute`, `distance_from_target_minutes` | Momento que representa y distancia al instante nominal. |
| `observation_kind` | Si es observación, checkpoint o endpoint. |
| `TrajectoryPointValue` | Valor derivado que conserva la procedencia del punto original. |
| `TrajectorySample` | Puntos, checkpoints, vista adaptativa, endpoint y observaciones no utilizables. |
| `P4SeriesInput` | Identidad del dato seguido, vista, tipo de valor, puntos, checkpoints esperados/ausentes y series constituyentes. |
| `P4ExtractionResult` | Series extraídas y explicación de disponibilidad temporal. |
| `P4SeriesResult` | Resultado de una serie: mercado, puntos, tramos, métricas, señales, estado y trazabilidad. |
| `BASE_SERIES_ID`, `CONSTITUENT_SERIES_IDS` | Serie de origen e ingredientes de series derivadas. |
| `EXPECTED_TARGET_MINUTES`, `MISSING_TARGET_MINUTES` | Momentos previstos y faltantes. |
| `OPERATIVE_ENDPOINT_PRESENT` | Si existe la última observación necesaria. |
| `market` | Dominio SIDE/TOTALS, mercado, familia, periodo, línea, resultado, casa, proveedor, lado/nivel, vista y tipo de valor. |
| `nominal_target_as_of`, `operative_as_of` | Instante nominal del checkpoint y límite operativo disponible. |
| `inputs` | Observaciones, valores y referencias de las series. |
| `analysis` | Métricas, estructura temporal, tramos y trazabilidad por serie. |
| `signals` | Métricas calculadas o explicación de su indisponibilidad. |

En un tramo, `FROM_POINT_ID` y `TO_POINT_ID` identifican extremos, `ORDINAL` su posición, `CONTIGUOUS_RAW` continuidad y `LEG_ID` el tramo. Los campos START/END indican valores, horas o referencias según su nombre; no contienen otro score.

Los motivos habituales se entienden así: `MISSING_ENDPOINT`, falta la lectura final; `INSUFFICIENT_OBSERVATIONS`, faltan puntos; `NON_CONTIGUOUS_GAP`, el recorrido tiene un hueco; `ZERO_DENOMINATOR`, no se puede dividir; `NOT_APPLICABLE`, la métrica no corresponde a ese valor. Son explicaciones locales, no una política que obligue a invalidar todo P4.

## Recorrido integrado: del dato al resultado

Este recorrido enlaza selección temporal, series, tramos, métricas, patrones, corrección, ventanas, líneas y relaciones. El ejemplo principal utiliza la trayectoria de cuota 2.00 → 1.90 → 1.95 de la sección 5.

### Paso 1. Identificar qué dato cambia y hasta cuándo se conoce

Supongamos un partido a las 20:00 y una evaluación a las 19:55:16, T−5. Se elige la última lectura alrededor de las 19:55, dentro de la ventana admitida. Para el ejemplo, sus horas efectivas y de recogida coinciden.

El extractor reconoce un contrato y resultado concretos, por ejemplo local de Pinnacle en 1X2 de tiempo completo. Conserva mercado, periodo, línea cuando existe, casa, proveedor, resultado y lado/nivel del exchange. Esos datos identifican la serie; no se mezclan precios de contratos diferentes para añadir observaciones.

`TrajectoryPoint` conserva valor, hora efectiva y disponibilidad. La primera sitúa el cambio; la segunda comprueba si ya podía conocerse al evaluar. `TrajectorySample` selecciona checkpoints, recorrido adaptativo y endpoint.

### Paso 2. Formar las dos vistas y los tipos de valor

Para CHECKPOINT_VIEW, tomemos tres puntos:

| Checkpoint | Hora efectiva | Cuota `ODDS_PRICE` |
|---|---|---|
| T−120 | 18:00 | 2.00 |
| T−30 | 19:30 | 1.90 |
| T−5 | 19:55 | 1.95 |

El punto T−5 es el endpoint operativo. `P4SeriesInput` conserva los puntos, sus identidades, tipo de valor, vista y checkpoints esperados. En este ejemplo los tres checkpoints forman la secuencia esperada y ninguno falta entre ellos.

ADAPTIVE_VIEW puede contener además observaciones intermedias hasta ese endpoint, siempre que estuvieran disponibles al evaluar. Con más puntos puede revelar correcciones que tres checkpoints no muestran. Cada vista produce sus propias métricas.

A partir de las cuotas se puede construir otra serie con `IMPLIED_PROBABILITY_RAW = 1/cuota`. Los límites de fuente y cantidades disponibles generan SOURCE_LIMIT y EXCHANGE_SIZE cuando existen. Sus valores no se suman a la cuota original.

### Paso 3. Convertir puntos en tramos

El motor une puntos consecutivos y obtiene dos tramos:

| Tramo | Delta | Minutos efectivos | Velocidad |
|---|---|---|---|
| T−120 → T−30 | 1.90 − 2.00 = −0.10. | 90. | Aproximadamente −0.001111 por minuto. |
| T−30 → T−5 | 1.95 − 1.90 = +0.05. | 25. | +0.002 por minuto. |

`FROM_POINT_ID` y `TO_POINT_ID` vinculan los extremos; `LEG_ID` identifica el tramo; `ORDINAL` conserva su orden; `CONTIGUOUS_RAW` indica continuidad. Los signos son −1 y +1.

Si se esperaba T−30 pero faltara, el salto T−120 → T−5 no demostraría continuidad. Podría existir cambio entre extremos, pero no la velocidad y eficiencia de un recorrido completo.

### Paso 4. Obtener las métricas del recorrido

Con tres observaciones, dos tramos continuos y endpoint:

- `NET_MOVE_RAW` = 1.95 − 2.00 = **−0.05**.
- `PATH_LENGTH_RAW` = 0.10 + 0.05 = **0.15**.
- `PATH_EFFICIENCY_RAW` = 0.05/0.15 ≈ **0.333333**.
- `ELAPSED_MINUTES_ACTUAL` = **115**.
- `VELOCITY_RAW` global = −0.05/115 ≈ **−0.000435** por minuto.
- `NO_MOVEMENT_RAW` = falso y `GAP_PRESENT_RAW` = falso.

La diferencia entre cambio neto y recorrido muestra que hubo ida y vuelta. En la serie de inversos, los mismos precios producen aproximadamente 0.500000 → 0.526316 → 0.512821: su cambio neto es positivo. El signo se interpreta después de leer `VALUE_TYPE`.

Una serie constante completa daría cambio cero y NO_MOVEMENT verdadero; su eficiencia quedaría ausente por denominador cero. Una serie sin endpoint o con un solo punto no se presenta como movimiento cero.

### Paso 5. Reconocer rachas, giro y corrección

El primer tramo forma una racha NEGATIVE y el segundo una POSITIVE. `DIRECTIONAL_RUNS` conserva ambas, con tiempos, puntos, tramos y magnitud. `PATH_PATTERN_RAW` es REVERSAL.

`NET_DIRECTION_RAW` es NEGATIVE, pero `FINAL_RUN_DIRECTION_RAW` es POSITIVE. El punto de cuota 1.90 es un valle, TROUGH, de tipo POINT en `TURNING_STRUCTURE_RAW`. Hay un cambio de signo; no atraviesa una meseta de puntos planos.

La primera racha movió 0.10 y la primera contraria corrigió 0.05. Por tanto, ratio de corrección **0.50**, retención **0.50**, estado CORRECTION, sobrepaso falso y magnitud de sobrepaso cero. Las referencias de `CORRECTION_RAW` permiten volver a esas rachas.

Si la corrección fuera 0.15, el ratio sería 1.50 y habría sobrepaso de 0.05. Esa es otra rama del mismo cálculo, no el resultado del ejemplo principal.

Las velocidades pasan de negativa a positiva. Se registra inversión; aunque aumente su magnitud absoluta, no se clasifica como aceleración en el mismo sentido. Los tramos de igual dirección sí permiten estudiar aceleración o desaceleración según la magnitud de sus velocidades. Para EXCHANGE_SIZE se conservan cambios de cantidad, pero estas medidas de velocidad y aceleración quedan sin valor.

### Paso 6. Localizar dónde ocurrió el movimiento

La asignación utiliza el punto final de cada tramo. El que termina en T−30 pertenece a DEVELOPMENT_WINDOW; el que termina en T−5, a LATE_WINDOW.

El recorrido de desarrollo es 0.10 y su participación **0.10/0.15 = 2/3**. El tardío es 0.05 y su participación **1/3**. No se asigna recorrido a EARLY_WINDOW solo porque el primer punto esté en T−120.

`WINDOWS` conserva cambios, recorrido, puntos, tramos, duración, patrón y referencias por ventana. El primer tramo es dominante por magnitud y la ventana de desarrollo es dominante por recorrido. Si hubiera empate, se conservarían todos los dominantes.

### Paso 7. Añadir líneas y series derivadas comparables

En hándicap o totales, una serie LINE puede seguir una línea única por checkpoint, por ejemplo 2.5 → 3.0. La diferencia +0.5 describe la condición del mercado. Los precios de 2.5 y 3.0 permanecen en series distintas. Una serie antigua sin endpoint puede quedar CONTRACT_ENDED si se reconoce su relevo.

Cuando existen ingredientes en checkpoints comunes, el motor construye SIDE_EDGE u OU_EDGE. Después puede formar BOOK_REP_EDGE, BOOK_INTERNAL_GAP, EXCHANGE_REP_EDGE, EXCHANGE_INTERNAL_GAP y BOOK_EXCHANGE_GAP. Para precios BACK/LAY compatibles también construye BACK_LAY_RELATIVE_SPREAD.

Cada serie derivada conserva `CONSTITUENT_SERIES_IDS` y las referencias originales. Si falta un ingrediente, no inventa una observación. Estas series usan las fórmulas de pares, medias, diferencias y spreads de [P2](02-pilar-2-mercado-de-lado.md) y [P3](03-pilar-3-mercado-de-totales.md), y luego el mismo análisis temporal.

Como ejemplo complementario de relación, si en checkpoints comunes el representante de casas pasa de +0.04 a +0.08 y el del exchange de +0.06 a −0.02, el gap pasa de **0.02 a 0.10**, con cambio **+0.08**. La relación cambia de ALIGNED_POSITIVE a OPPOSED. `STATE_CHANGED_RAW` es verdadero. Son edges comparables; no se compara una cuota con un edge.

### Paso 8. Publicar señales y reconstruir su significado

`P4ExtractionResult` reúne las series y el diagnóstico temporal. Cada `P4SeriesResult` devuelve mercado, puntos, tramos, métricas, señales estructurales, estado y trazabilidad.

El paquete general guarda ingredientes en `inputs`, cálculos por serie en `analysis` y métricas calculadas o motivos locales en `signals`. Añade contratos, cobertura, diagnósticos y límites nominal y operativo. Una métrica imposible puede quedar BLOCKED mientras otras son COMPUTED.

En el ejemplo principal existe movimiento calculable y el resultado general es ACTIVE. La lectura es: **«Hasta T−5, esta cuota bajó 0.10 y recuperó 0.05. Termina por debajo del inicio, con una corrección de la mitad del descenso inicial. Dos tercios del recorrido observado se asignan a desarrollo y un tercio al tramo tardío»**.

Para comprobarlo se sigue **señal → serie y tipo de valor → métrica → tramos → puntos y relojes → contrato original**. En una serie derivada se recorren también sus constituyentes. Los conteos de observaciones, checkpoints faltantes y presencia del endpoint permiten distinguir un patrón observado de una historia incompleta.

## 13. Fuentes y lectura complementaria

[Entrada de P4](../../../modules/pillars/pillar_4/run_pillar_4.py), [selección de series](../../../modules/pillars/pillar_4/trajectory_policy.py), [motor de series](../../../modules/pillars/pillar_4/trajectory_engine.py), [fórmulas temporales](../../../modules/pillars/pillar_4/metrics.py), [series derivadas](../../../modules/pillars/pillar_4/semantic_metrics.py), [relaciones](../../../modules/pillars/pillar_4/relations.py), [objetos](../../../modules/pillars/pillar_4/models.py) y [muestreo temporal común](../../../modules/pillars/trajectory_sampling.py).

La [guía principal](00-flujo-principal.md) explica el origen de los datos y los estados globales. [P2](02-pilar-2-mercado-de-lado.md) y [P3](03-pilar-3-mercado-de-totales.md) explican las inclinaciones cuyo movimiento también observa P4.

