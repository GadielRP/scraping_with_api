# Pilar 3: lectura del mercado de totales

[Volver a la guía principal](00-flujo-principal.md) · [P1: estructura deportiva](01-pilar-1-estructura-deportiva.md) · [P2: lado](02-pilar-2-mercado-de-lado.md) · [P4: movimiento](04-pilar-4-movimiento-temporal.md) · [P5: memoria](05-pilar-5-memoria-de-precios.md)

El motor `p3-signal-profile-v3`, revisado el 8 de octubre de 2026, estudia **cómo el mercado de totales se inclina hacia mayor o menor anotación y cómo coinciden o discrepan sus lecturas**.

No calcula los goles o puntos esperados desde resultados deportivos. Esa tarea pertenece a la rama de [totales de P1](01-pilar-1-estructura-deportiva.md). P3 utiliza precios y líneas del mercado.

## 1. Qué significa un total

**Over** significa superar la línea; **Under**, quedar por debajo según las condiciones del contrato. Una **línea** como 2.5 es el umbral del mercado. El precio Over 2.5 y el precio Under 2.5 son dos resultados del mismo contrato.

La línea no es la cuota. «Línea 2.5 con cuota 1.90» contiene dos números diferentes: umbral de anotación y precio para ese resultado.

El **periodo** importa: Over 2.5 de tiempo completo y Over 2.5 de primera mitad no describen la misma parte del encuentro. P3 utiliza el tiempo completo seleccionado en común, incluyendo prórroga o reglamentario, y puede leer primera mitad por separado.

Las casas son Pinnacle y bet365; Betfair aporta las lecturas de exchange BACK y LAY. La cantidad disponible en un precio se conserva como contexto, sin convertirse en un peso del cálculo.

## 2. Entradas y recorrido

P3 recibe la identidad del evento, el historial de cuotas, el momento elegido y la selección común de mercados. Su función de entrada es `calculate_pillar_3`.

1. Selecciona contratos Over/Under soportados.
2. Extrae línea, precio Over y precio Under por casa y periodo en el momento común.
3. Conserva separadas variantes con distintas líneas.
4. Calcula inclinación individual por casa.
5. Calcula separación de líneas y, cuando los contratos coinciden, compara precios y representantes.
6. Lee BACK y LAY del exchange y los compara entre sí o con las casas cuando procede.
7. Compara direcciones de tiempo completo y primera mitad.
8. Devuelve señales, intermedios y referencias a sus ingredientes.

Una lectura individual necesita Over y Under válidos del mismo mercado. Para medir separación de líneas bastan las líneas. Por ello puede existir una señal de estructura de línea sin una señal de precios y viceversa.

## 3. Fórmula de inclinación Over/Under

**EDGE = (1 ÷ cuota Over − 1 ÷ cuota Under) ÷ (1 ÷ cuota Over + 1 ÷ cuota Under).**

| Variable | Significado |
|---|---|
| `over_odds` / `OVER_ODDS` | Precio del resultado Over. |
| `under_odds` / `UNDER_ODDS` | Precio del resultado Under. |
| Inversos de las cuotas | Pesos implícitos brutos de los dos resultados. |
| `edge` / `EDGE` | Diferencia relativa de esos pesos. |
| `direction` / `DIRECTION` | Etiqueta obtenida del signo del edge. |

Con Over 1.80 y Under 2.20, el resultado es aproximadamente 0.10: inclinación OVER. Con Over 2.20 y Under 1.80, es −0.10: inclinación UNDER. Con precios iguales vale cero: NEUTRAL.

No se incorpora un umbral de intensidad para estas direcciones. La fórmula compara el par de precios, no predice una cantidad exacta de goles o puntos ni garantiza superar la línea.

## 4. Lectura individual de cada casa

`PINNACLE` y `BET365` contienen cinco datos:

| Campo | Significado |
|---|---|
| `LINE` | Línea del contrato. |
| `OVER_ODDS` | Cuota Over. |
| `UNDER_ODDS` | Cuota Under. |
| `EDGE` | Resultado de la fórmula del par. |
| `DIRECTION` | OVER, UNDER o NEUTRAL. |

Si la línea falta pero el par de precios existe y pertenece a una lectura válida, puede calcularse el edge individual. Las comparaciones que necesitan demostrar igualdad de línea permanecen ausentes.

## 5. Estructura de líneas

El bloque `LINE_STRUCTURE` conserva:

**LINE_DIFF_RAW = línea Pinnacle − línea bet365.**

**LINE_GAP = valor absoluto de LINE_DIFF_RAW.**

Si Pinnacle tiene 2.5 y bet365 3.0, la diferencia es −0.5 y la separación 0.5. El primer dato conserva cuál casa publicó la línea mayor; el segundo mide cuánto difieren.

No se transforma ese −0.5 en una dirección UNDER del par de precios. Diferencia de líneas y edge de precios son magnitudes distintas.

## 6. Relación entre casas y representante

Para comparar precios de las casas se exige el mismo periodo y la misma línea, además de ambos edges disponibles.

**GAP = valor absoluto de (EDGE Pinnacle − EDGE bet365).**

**EDGE representativo = (EDGE Pinnacle + EDGE bet365) ÷ 2.**

Los resultados se almacenan como `BOOK_RELATION` y `REPRESENTATIVE`.

| Direcciones de las dos lecturas | `RELATION` |
|---|---|
| OVER y OVER | `CONVERGENCE_OVER`: coincidencia hacia mayor anotación. |
| UNDER y UNDER | `CONVERGENCE_UNDER`: coincidencia hacia menor anotación. |
| OVER y UNDER | `DIVERGENCE`: oposición. |
| Alguna NEUTRAL | `NEUTRAL`: una de las lecturas no tiene dirección. |

Ejemplo: ambos contratos tienen línea 2.5 y los edges son 0.10 y 0.04. La media es 0.07, el gap 0.06 y la relación CONVERGENCE_OVER.

Si las líneas son 2.5 y 3.0, los edges individuales y la separación de líneas pueden existir. La media de precios y su comparación quedan ausentes porque describen umbrales distintos.

La etiqueta `CONTEXT_DIRECTION_RAW` traduce la dirección representativa:

| Dirección | Contexto |
|---|---|
| OVER | `OPEN_BIAS`: inclinación de mercado hacia un entorno de mayor anotación. |
| UNDER | `CLOSED_BIAS`: inclinación hacia menor anotación. |
| NEUTRAL | `NEUTRAL_BIAS`: sin inclinación del representante. |
| Representante ausente | Contexto ausente. |

La etiqueta describe el mercado; no mide el estilo deportivo real de los equipos.

## 7. Exchange de totales

P3 conserva lecturas de Betfair para tiempo completo y primera mitad cuando están disponibles.

Cada lado BACK o LAY guarda `OVER_ODDS`, `UNDER_ODDS`, `OVER_SIZE`, `UNDER_SIZE`, `EDGE` y `DIRECTION`. El edge utiliza la misma fórmula Over/Under. Las cantidades no intervienen en ella.

Si BACK y LAY tienen líneas compatibles y los dos edges existen:

- `BACK_LAY_RELATION` compara sus direcciones.
- `EXCHANGE_INTERNAL_GAP` es la diferencia absoluta de los edges.
- `REPRESENTATIVE.EDGE` es su media.
- `REPRESENTATIVE.DIRECTION` es la dirección de esa media.

Una lectura BACK calculable se conserva aunque falte LAY. No se presenta la ausencia como un edge neutral.

Para comparar exchange y casas, primero ambas casas deben tener la misma línea. Esa es la línea de casas utilizada:

**LINE_DIFF_RAW = línea de casas − línea del exchange.**

**LINE_GAP = valor absoluto de LINE_DIFF_RAW.**

Solo si la diferencia de líneas es cero y existen ambos representantes se calculan:

**GAP = valor absoluto de (representante de casas − representante del exchange).**

`RELATION` compara sus direcciones con las reglas de la sección 6. Los bloques son `BOOK_EXCHANGE_OU` y `BOOK_EXCHANGE_1H_OU`.

Ejemplo: casas a 2.5 y exchange a 3.0 permiten medir una separación de 0.5, pero no una comparación de precios como si el contrato fuera igual.

## 8. Tiempo completo frente a primera mitad

Cuando cada periodo dispone de su representante y su relación entre casas, se calcula:

**FT_1H_OU_GAP = valor absoluto de (representante tiempo completo − representante primera mitad).**

`FT_1H_OU_RELATION` conserva coincidencia, oposición o neutralidad. `FT_1H_OU_STRUCTURE` guarda las relaciones y direcciones originales de cada periodo.

Aquí se comparan orientaciones relativas, no cantidades de anotación. La línea de primera mitad no necesita ser numéricamente igual a la de tiempo completo para esta lectura: cada representante ya fue construido dentro de su propio periodo y línea compatible.

Ejemplo: tiempo completo apunta a OVER y primera mitad a UNDER. La relación es DIVERGENCE. Esto no basta para afirmar que la segunda mitad producirá una cantidad determinada; el motor no hace esa deducción.

## 9. Objetos y nombres de entrada

| Objeto o campo | Qué reúne |
|---|---|
| `TotalsBookSnapshot` | Línea y observaciones Over/Under de una casa. |
| `TotalsExchangeSnapshot` | Lecturas BACK/LAY con sus líneas e identidad. |
| `P3PeriodSnapshot` | Casas del mismo periodo. |
| `P3MarketSnapshot` | Tiempo completo, primera mitad y exchange disponible. |
| `BookOUReading` | Línea, precios, edge y dirección individuales. |
| `LineStructureSignal` | Diferencia de línea con signo y separación absoluta. |
| `BookRelationSignal` | Relación direccional y separación de precios entre casas. |
| `RepresentativeSignal` | Edge medio y dirección. |
| `PeriodOUSignal` | Lecturas de casas, estructura, relación, representante y contexto. |
| `ExchangeOUReading`, `ExchangeOUSignal` | Datos por lado del exchange y comparación de ambos lados. |
| `BookExchangeOUSignal` | Diferencias de línea y relación de precios frente a las casas. |
| `FT1HSignal` | Relación de tiempo completo con primera mitad. |
| `P3SignalProfile` | Perfil que reúne todas las lecturas anteriores. |

Los nombres de entrada se interpretan por partes. `PIN_FT_OU_LINE` es la línea de totales de Pinnacle para tiempo completo; `B365_1H_UNDER_ODDS` es la cuota Under de bet365 para primera mitad; `BF_OU_LAY_OVER_FULL_TIME_EXCHANGE_SIZE` es la cantidad disponible en LAY Over del exchange para tiempo completo.

`trace` conserva evento, mercado, periodo, línea, casa, proveedor, resultado, quote/snapshot, momentos y lado/nivel del exchange. Es la procedencia de un ingrediente, no otro indicador.

## 10. Paquete de salida e interpretación

`analysis` conserva perfiles separados por periodo y variantes de contrato. Cada perfil incluye `FT`, `1H`, `FT_1H`, `BETFAIR_FT_OU`, `BOOK_EXCHANGE_OU`, `BETFAIR_1H_OU` y `BOOK_EXCHANGE_1H_OU`, cuando hay datos.

`signals` contiene lecturas individuales de EDGE, diferencias de línea, gaps y relaciones. `inputs` conserva sus ingredientes; `contracts`, sus mercados; `coverage`, qué partes estuvieron disponibles; `diagnostics`, por qué alguna lectura faltó.

Cero significa un cálculo neutral válido. `null` significa que no se pudo producir el dato. El pilar puede estar activo por una separación de líneas calculada, aunque no haya representante de precios. «Activo» no equivale a consenso de todas las fuentes.

Lea el perfil deportivo de [P1](01-pilar-1-estructura-deportiva.md) si quiere entender anotación histórica, y [P4](04-pilar-4-movimiento-temporal.md) si quiere entender cambios de precios y líneas. Ninguno reemplaza automáticamente al otro en el coordinador.

## 11. Fuentes

[Entrada de P3](../../../modules/pillars/pillar_3_totals_market_context/run_pillar_3.py), [extracción](../../../modules/pillars/pillar_3_totals_market_context/snapshot_policy.py), [fórmulas](../../../modules/pillars/pillar_3_totals_market_context/signal_engine.py), [objetos](../../../modules/pillars/pillar_3_totals_market_context/signal_models.py), [relaciones](../../../modules/pillars/pillar_3_totals_market_context/relations.py), [periodos](../../../modules/pillars/pillar_3_totals_market_context/periods.py), [matemática común](../../../modules/pillars/market_math.py) y [evaluación de perfiles](../../../modules/pillars/profile_evaluation.py). Para los estados comunes, consulte la [guía principal](00-flujo-principal.md#8-lectura-de-resultados-de-p2p5).

