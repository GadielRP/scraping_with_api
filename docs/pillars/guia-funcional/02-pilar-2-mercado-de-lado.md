# Pilar 2: lectura del mercado de lado

[Volver a la guía principal](00-flujo-principal.md) · [P1: estructura deportiva](01-pilar-1-estructura-deportiva.md) · [P3: totales](03-pilar-3-mercado-de-totales.md) · [P4: movimiento](04-pilar-4-movimiento-temporal.md) · [P5: memoria](05-pilar-5-memoria-de-precios.md)

Este documento describe el motor `p2-signal-profile-v3` revisado el 8 de octubre de 2026. P2 pregunta **cómo se inclinan los precios hacia el local o el visitante y qué coincidencias o diferencias existen entre lecturas**. No combina resultados deportivos de P1 ni produce un único porcentaje de victoria.

## 1. Vocabulario de los mercados

**Home / local** y **Away / visitante** son los dos lados del encuentro. **1X2** es un contrato de tres resultados: local, empate y visitante. **Home/Away** es un contrato de dos resultados. P2 puede leer el par local/visitante de ambos, pero conserva su identidad: una lectura del par de 1X2 no se convierte en una probabilidad completa de tres resultados.

Un **hándicap** modifica el marcador usado por el mercado mediante una línea. Por ejemplo, una línea local −1.5 representa una condición distinta de la línea −0.5. **Asian Handicap**, abreviado AH, y **Handicap** estándar son familias distintas.

Un **periodo** delimita la parte del encuentro: tiempo completo, primera mitad o las primeras cinco entradas de béisbol. En el código hay estructuras internas reutilizadas para estas lecturas; el reporte conserva explícitamente `FIRST_FIVE_INNINGS` cuando se trata de entradas y no las presenta como una mitad.

**Pinnacle** (`PIN`) y **bet365** (`B365`) son casas. **Betfair** (`BF`) es el exchange estudiado. En un exchange, **BACK** es el precio para apoyar un resultado y **LAY** para tomar la posición contraria. **Size** es la cantidad publicada disponible en ese nivel de precio; no mide por sí sola toda la actividad del mercado.

## 2. Entradas y recorrido

P2 recibe la ficha del evento (`event_context`), el historial organizado de cuotas (`odds_trajectory_context`), el momento elegido (`target_selection`) y la evaluación de mercados común (`market_evaluation`). Las decisiones de momento y tiempo completo se explican en las [secciones 5 y 6 de la guía](00-flujo-principal.md#5-elección-del-momento-de-mercado).

El recorrido es:

1. Conserva las familias y periodos soportados.
2. Trata 1X2 y Home/Away por separado.
3. Extrae las lecturas del momento seleccionado para cada casa y lado del exchange.
4. Mantiene separados los contratos con diferentes líneas; no junta fragmentos de distintos mercados para fabricar una lectura completa.
5. Calcula inclinaciones individuales, coincidencias entre casas y relaciones entre familias o periodos.
6. Convierte cada métrica analítica en una señal con referencias a sus entradas.
7. Devuelve señales, intermedios, cobertura y explicaciones.

Una cuota debe ser un número finito mayor que 1. Para una inclinación individual bastan los precios de local y visitante del mismo contrato. El empate se conserva cuando está disponible, pero no interviene en la fórmula de ese par. Por eso un 1X2 sin empate puede permitir una lectura local/visitante y seguir teniendo cobertura incompleta.

## 3. Cómo obtiene la inclinación de un par de precios

La fórmula base transforma cada cuota en su inverso. Una cuota más pequeña tiene un inverso más grande.

| Variable | Significado |
|---|---|
| `home_price` | Cuota del local. |
| `away_price` | Cuota del visitante. |
| `1 / home_price` | Peso implícito bruto del local. |
| `1 / away_price` | Peso implícito bruto del visitante. |
| `edge` | Diferencia relativa entre ambos pesos. |

**Edge = (inverso local − inverso visitante) ÷ (inverso local + inverso visitante).**

Si el local cotiza 1.80 y el visitante 2.20, sus inversos son aproximadamente 0.5556 y 0.4545. El resultado es 0.10. Si se intercambian los precios, resulta −0.10.

| Valor del edge | `DIRECTION` | Lectura |
|---|---|---|
| Mayor que cero | `HOME` | El par de precios se inclina hacia el local. |
| Menor que cero | `AWAY` | Se inclina hacia el visitante. |
| Igual a cero | `NEUTRAL` | Los dos precios producen el mismo peso. |

No se añade un umbral mínimo para cambiar estas etiquetas. Un valor positivo pequeño sigue siendo HOME. Tampoco existe una tabla de intensidad de P2 equivalente a la de P1.

El edge expresa un contraste del par. En un 1X2 no utiliza el precio del empate ni estima la probabilidad total de local o visitante. Una ventaja de precio tampoco significa una ventaja deportiva demostrada.

## 4. Dos casas: inclinaciones, separación y representante

Pinnacle y bet365 se leen individualmente:

- `PIN_EDGE` y `PIN_DIRECTION`: inclinación y dirección de Pinnacle.
- `B365_EDGE` y `B365_DIRECTION`: lo mismo para bet365.

Para combinar las dos lecturas deben pertenecer al mismo periodo y familia. Entonces:

**REP_EDGE = (PIN_EDGE + B365_EDGE) ÷ 2.**

**BOOK_GAP = valor absoluto de (PIN_EDGE − B365_EDGE).**

El representante es una media con pesos iguales; no presupone que una casa sea más fiable. El gap mide separación y siempre es no negativo.

Ejemplo: Pinnacle da 0.10 y bet365 0.06. El representante es 0.08 y el gap 0.04. Ambas apuntan al local. Si fueran 0.10 y −0.06, el representante sería 0.02 y el gap 0.16: la media apunta al local, pero las casas discrepan. Esa discrepancia no desaparece del resultado.

`BOOK_RELATION` usa estas reglas:

| Direcciones comparadas | Relación |
|---|---|
| HOME y HOME | `CONVERGENCE_HOME`: coincidencia hacia el local. |
| AWAY y AWAY | `CONVERGENCE_AWAY`: coincidencia hacia el visitante. |
| Una HOME y otra AWAY | `DIVERGENCE`: oposición. |
| Alguna NEUTRAL | `NEUTRAL`: no hay coincidencia direccional de las dos. |

Si solo existe una casa, su edge individual sigue siendo calculable. La media y la comparación de ambas permanecen ausentes.

## 5. Hándicap: separar línea y precio

Cada casa puede tener una línea local (`PIN_LINE`, `B365_LINE`) y precios local/visitante para esa línea. El motor aplica la misma fórmula de edge a los precios.

**LINE_GAP = valor absoluto de (línea Pinnacle − línea bet365).**

La diferencia de líneas se puede medir aunque no haya precios completos. Pero para calcular `PRICE_GAP`, `REP_EDGE` y `BOOK_RELATION` del hándicap se exige que las líneas sean iguales, además del mismo periodo y familia.

Ejemplo: Pinnacle ofrece −0.5 y bet365 −1.5. La separación es 1.0. Comparar directamente sus inclinaciones como si fueran el mismo contrato resultaría engañoso, porque los precios corresponden a condiciones distintas; esa comparación queda sin valor.

AH y Handicap estándar conservan bloques independientes. `HANDICAP` no es una abreviatura alternativa de `AH`.

## 6. Relación entre ganador y hándicap

Cuando hay representantes de ambas familias:

**CROSS_MARKET_GAP = valor absoluto de (representante ganador − representante AH).**

La relación utiliza las mismas etiquetas HOME, AWAY, NEUTRAL y sus combinaciones de la sección 4. Para el hándicap estándar se conserva una relación distinta, `CROSS_MARKET_HANDICAP`.

Esto compara la orientación de dos condiciones diferentes del encuentro. No vuelve idénticos sus requisitos ni estima cuánto marcador producirá el partido.

El bloque de tiempo completo utiliza nombres como `FT_1X2_AH_RELATION` y `FT_CROSS_MARKET_GAP`; el secundario utiliza nombres equivalentes de primera mitad o primeras cinco entradas.

## 7. Exchange: BACK y LAY

P2 puede calcular un edge de BACK y otro de LAY, cada uno con su par local/visitante:

| Campo | Operación |
|---|---|
| `BACK_EDGE`, `LAY_EDGE` | Fórmula del par de precios en ese lado. |
| `BACK_DIRECTION`, `LAY_DIRECTION` | Dirección por el signo de cada edge. |
| `BACK_LAY_RELATION` | Coincidencia, oposición o neutralidad entre direcciones. |
| `EXCHANGE_INTERNAL_GAP` | Valor absoluto de BACK_EDGE − LAY_EDGE. |
| `REP_EDGE` | Media de los dos edges cuando son comparables. |
| `HOME_SPREAD`, `AWAY_SPREAD` | Separación relativa entre precios BACK y LAY para cada resultado. |
| `SIDE_SPREAD` | Media de las dos separaciones relativas. |

**Spread relativo = (cuota LAY − cuota BACK) ÷ media de esas cuotas.**

Con BACK 2.00 y LAY 2.10, la media es 2.05 y el spread aproximadamente 0.04878. La operación conserva el signo; no aplica valor absoluto.

Se pueden conservar precios BACK aunque LAY falte. Las métricas que requieren ambos lados quedan ausentes. Las cantidades disponibles (`HOME_SIZE`, `AWAY_SIZE`) se muestran como contexto y no ponderan estos edges.

Para el mercado de ganador de tiempo completo, `BOOK_EXCHANGE` compara el representante de Pinnacle/bet365 con el representante BACK/LAY:

**GAP = valor absoluto de (representante de casas − representante del exchange).**

`BOOK_DIRECTION` y `EXCHANGE_DIRECTION` conservan sus orientaciones; `RELATION` aplica las reglas de coincidencia, oposición y neutralidad. Se necesitan ambos representantes. No compara una casa aislada con el exchange para fabricar el representante que falta.

Para AH y Handicap del exchange se conservan `LINE`, precios y cantidades BACK/LAY. El representante de esos lados requiere líneas compatibles. Además se comparan con las casas:

**LINE_DIFF_RAW = línea representativa de casas − línea del exchange.**

**LINE_GAP = valor absoluto de LINE_DIFF_RAW.**

Las casas tienen una línea representativa para esta comparación solo si Pinnacle y bet365 coinciden en ella. La comparación de edges exige además que coincida con la del exchange. Entonces `GAP` es la diferencia absoluta de representantes y `RELATION` describe sus direcciones.

Estas lecturas se mantienen separadas para tiempo completo y periodos secundarios soportados.

## 8. Tiempo completo frente al periodo secundario

Cuando existen representantes de ganador en ambos periodos:

**FT_1H_1X2_GAP = valor absoluto de (representante tiempo completo − representante secundario).**

`FT_1H_1X2_RELATION` conserva si sus direcciones coinciden. `FT_1H_STRUCTURE` añade las relaciones entre ganador y hándicap de cada periodo.

No se escala la primera mitad para predecir tiempo completo. En béisbol, los campos de esa lectura se presentan como primeras cinco entradas para conservar el significado deportivo.

## 9. Diccionario de entradas, objetos y resultados

| Campo u objeto | Explicación |
|---|---|
| `MarketIdentity` | Familia, periodo y nombre del mercado solicitado. |
| `MarketSnapshotRequest` | Petición de una lectura: mercado, casa, resultados y, si aplica, BACK o LAY. |
| `ChoiceRequest` | Resultado concreto solicitado, por ejemplo local. |
| `QuotePoint` | Un precio con su cantidad opcional y procedencia. |
| `trace` | Identidad del evento, mercado, casa, proveedor, resultado, línea, quote/snapshot, timestamps y lado/nivel del exchange. |
| `ThreeWayMarketSnapshot` | Precios local, empate y visitante con procedencia del mismo contrato. |
| `TwoWayMarketSnapshot` / variante parcial | Par local/visitante del mismo mercado, completo o incompleto. |
| `PartialAsianHandicapSnapshot` | Par de hándicap con línea, conservando ausencias. |
| `P2MarketSnapshot` | Lecturas de tiempo completo y periodo secundario, con exchange cuando aplica. |
| `BookMarketSignal` | Edges individuales, direcciones, representante y comparación entre casas. |
| `AsianHandicapSignal` | Añade líneas y distingue separación de línea y precio. |
| `PeriodSignal` | Agrupa ganador, AH, Handicap y sus relaciones en un periodo. |
| `ExchangeSignal` / lecturas de hándicap | Edges BACK/LAY, representante, relaciones y spreads aplicables. |
| `P2SignalProfile` | Perfil que reúne todos esos bloques. |
| `FT`, `1H`, `FIRST_FIVE_INNINGS` | Tiempo completo, primera mitad y primeras cinco entradas. |
| `EXCHANGE`, `BOOK_EXCHANGE` | Lectura interna del exchange y comparación con las casas. |
| `BETFAIR_FT_AH`, `BETFAIR_1H_AH` | AH del exchange por periodo. |
| `BOOK_EXCHANGE_AH`, `BOOK_EXCHANGE_1H_AH` | Comparación de AH con las casas. |
| Bloques equivalentes `HANDICAP` | Mismas operaciones para Handicap estándar, sin mezclarlo con AH. |
| `missing_inputs`, `invalid_inputs`, `ambiguous_inputs` | Entradas ausentes, presentes pero inválidas, o con más de un candidato posible. |

Los nombres de entrada se leen por partes: `PIN_AH_HOME_FULL_TIME_ODDS_PRICE` significa «cuota de Pinnacle para el local en AH de tiempo completo». `BF_AH_LAY_AWAY_FULL_TIME_EXCHANGE_SIZE` significa «cantidad de Betfair publicada en LAY del visitante para ese AH». Cambiar PIN por B365 o HOME por AWAY cambia la casa o el resultado, no la fórmula.

`analysis` organiza los perfiles por familia, periodo secundario y variante de contrato. `signals` convierte métricas como edge, gap y relation en lecturas individuales. Los campos de dirección también se conservan en el perfil explicativo. Las referencias de cada señal permiten volver a precios y contratos sin interpretar el nombre del campo como una fórmula nueva.

## 10. Cómo interpretarlo junto con otros pilares

P2 describe el estado actual. [P4](04-pilar-4-movimiento-temporal.md) puede explicar cómo llegó a él. [P1](01-pilar-1-estructura-deportiva.md) describe el rendimiento deportivo y [P5](05-pilar-5-memoria-de-precios.md) los antecedentes de precios iguales. El coordinador no decide automáticamente cuál de estas perspectivas prevalece.

Una señal neutral vale cero; una comparación imposible vale `null`. El pilar puede estar activo con una sola lectura individual aunque falte una comparación entre casas. Los estados comunes se explican en la [guía principal](00-flujo-principal.md#8-lectura-de-resultados-de-p2p5).

## 11. Fuentes de esta explicación

[Entrada de P2](../../../modules/pillars/pillar_2_side_market/run_pillar_2.py), [extracción](../../../modules/pillars/pillar_2_side_market/snapshot_policy.py), [fórmulas](../../../modules/pillars/pillar_2_side_market/signal_engine.py), [objetos de señal](../../../modules/pillars/pillar_2_side_market/signal_models.py), [relaciones](../../../modules/pillars/pillar_2_side_market/relations.py), [periodos](../../../modules/pillars/pillar_2_side_market/periods.py), [primitivas matemáticas](../../../modules/pillars/market_math.py) y [evaluación de señales independientes](../../../modules/pillars/profile_evaluation.py). La [política común existente](../market-evaluation-v1.md) amplía selección y cobertura.

