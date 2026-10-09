# Pilar 5: memoria de precios exactos

[Volver a la guía principal](00-flujo-principal.md) · [P1: estructura](01-pilar-1-estructura-deportiva.md) · [P2: lado](02-pilar-2-mercado-de-lado.md) · [P3: totales](03-pilar-3-mercado-de-totales.md) · [P4: movimiento](04-pilar-4-movimiento-temporal.md)

P5, motor `p5_price_memory_v4_0` revisado el 9 de octubre de 2026, pregunta **qué resultados tuvieron otros partidos del mismo deporte con el mismo vector de precios de una casa**. Su salida combina predominio histórico y tamaño de muestra. No estima una probabilidad futura calibrada ni analiza el recorrido de la cuota actual.

Para seguir cómo se utilizan conjuntamente las definiciones y fórmulas, lea el [recorrido integrado del dato al resultado](#recorrido-integrado-del-dato-al-resultado), con pasos, ejemplo y lectura final.

## 1. Qué es un vector de precios

Es el conjunto de cuotas de los resultados de un mercado:

| Mercado | Vector requerido |
|---|---|
| 1X2, `THREE_WAY` | Local, empate y visitante. |
| Home/Away, `TWO_WAY` | Local y visitante; el empate no pertenece al vector. |

Ejemplo 1X2: local 1.850, empate 3.600 y visitante 4.400. Ejemplo Home/Away: local 1.850 y visitante 2.050.

P5 calcula perfiles separados para **Pinnacle**, **bet365** y **SofaScore** (`bookie_id=1`), para el tiempo completo seleccionado en común. No promedia sus memorias para crear una puntuación final del evento.

A diferencia de P2, un 1X2 sin empate no permite la consulta de memoria: aquí se exige el vector completo.

## 2. Recorrido de la ejecución

1. Recibe la identidad del evento y la selección común de momento y mercado.
2. Extrae un vector completo por casa y familia.
3. Comprueba que sus precios pertenecen a la misma casa, familia, periodo y mercado.
4. Redondea precios a tres decimales y construye la clave de búsqueda.
5. Consulta todos los eventos históricos elegibles con esa clave.
6. Cuenta resultados local, empate y visitante según la forma del mercado.
7. Evalúa tamaño de muestra, empate de frecuencias, predominio y score.
8. Conserva cada perfil con sus ingredientes, fórmulas y muestra utilizada cuando esta se guarda.

No utiliza cuotas de casas distintas para completar un vector. Tampoco utiliza el historial disponible de P5 para decidir si el tiempo completo común debe incluir prórroga.

## 3. Qué significa precio exacto

Cada precio debe ser finito y mayor que 1. Se representa con tres decimales usando redondeo de mitad hacia arriba.

Por ejemplo, 1.8544 pasa a 1.854 y 1.8545 pasa a 1.855. La coincidencia es exacta **después de esta cuantización**. No hay un rango de parecido ni una distancia entre vectores.

`ODDS_QUANTUM = 0.001` indica esa unidad de precisión.

| Campo de `MemoryQueryKey` | Significado |
|---|---|
| `sport` | Mismo deporte. |
| `bookie_id` | Misma casa. |
| `market_group` | Misma familia: 1X2 o Home/Away. |
| `market_period` | Mismo periodo de tiempo completo. |
| `market_shape` / `has_draw` | Dos o tres resultados; si pertenece empate. |
| `odds_home`, `odds_draw`, `odds_away` | Cuotas a tres decimales. En dos resultados, empate queda ausente. |

El minuto actual identifica la lectura del evento presente, pero **no forma parte de la clave de coincidencia histórica**. La consulta no exige igual minuto histórico ni mismos participantes.

## 4. Qué partidos entran en la muestra

El acceso histórico es `PriceMemoryReader`, una forma de pedir un resumen a la memoria guardada. La implementación consulta `mv_p5_price_memory`.

Un evento histórico entra si:

- Coincide con deporte, casa, familia, periodo, forma y todos los precios de la clave.
- No es el evento actual.
- Su inicio es estrictamente anterior al inicio del evento actual.
- Tiene resultado compatible y ambos marcadores disponibles.
- Cumple las restricciones de competición, temporada o país que se hayan elegido para esa población.
- Cuenta una sola vez por identificador de evento.

Las restricciones de competición, temporada y país no se aplican por el solo hecho de que el evento tenga esos campos. Si una restricción está habilitada, se exige su valor; si falta, no se amplía silenciosamente la población.

Entre filas históricas válidas duplicadas del mismo evento se conserva una, priorizando las referencias más recientes según los campos del historial. La consulta usa toda la población elegible, sin límite de coincidencias.

El corte es **inicio del evento histórico anterior al inicio del actual**. La condición de esta consulta no constituye por sí misma una reconstrucción de qué resultados ya se conocían a la hora del checkpoint actual. El momento de mercado actual y el corte histórico de la consulta son datos diferentes.

| Campo de `MemorySample` | Significado |
|---|---|
| `key` | Clave de coincidencia. |
| `sample_size` | Número de eventos únicos elegibles. |
| `wins_home` | Resultados local. |
| `wins_draw` | Empates; solo en tres resultados. |
| `wins_away` | Resultados visitante. |
| `exclusions` | Conteos de resultados incompatibles, marcadores ausentes y duplicados descartados entre candidatos. |
| `sample_id` | Identidad de muestra guardada, si existe. |

Los conteos deben sumar el tamaño de muestra. En Home/Away, `wins_draw` debe ser cero.

## 5. Muestra mínima y empate

El mínimo es **tres eventos**. Con menos de tres, `P5_VALID` es falso, `P5` queda ausente y el perfil informa datos insuficientes.

Con muestra suficiente se busca la mayor frecuencia de resultado. Si dos o más resultados comparten esa frecuencia máxima, la memoria conserva:

- `memory_status = TIE`;
- `P5_VALID = true`;
- `P5_DIRECTION = NONE`;
- `P5 = 0`;
- `P5_STRENGTH = NONE`.

Es un cálculo válido y neutral, no una muestra fallida.

Si hay un único resultado dominante, `P5_DIRECTION` es HOME, AWAY o DRAW y se aplican las fórmulas siguientes. El score es no negativo; la dirección se expresa en otro campo. Un score 0.30 con dirección AWAY no se convierte en −0.30.

## 6. Fórmulas, variable por variable

### 6.1 Frecuencia dominante y referencia uniforme

**P_hist_DOMINANT = cantidad del resultado dominante ÷ tamaño de muestra.**

**BASELINE = 1 ÷ número de resultados del mercado.**

En 1X2, baseline es 1/3; en Home/Away, 1/2. Esta referencia uniforme no viene de la cuota actual: representa repartir por igual los resultados posibles.

**HIST_EDGE = (frecuencia dominante − baseline) ÷ (1 − baseline).**

Cuantifica cuánto supera la frecuencia dominante esa referencia, en una escala que llega a 1 si todos los eventos tuvieron el resultado dominante.

### 6.2 Consistencia y tamaño continuo

**CONSISTENCY = 0.5 + 0.5 × HIST_EDGE.**

Es un factor construido a partir del predominio. No es desviación estándar ni una medida independiente de estabilidad.

**SAMPLE_FACTOR = mínimo entre (tamaño de muestra ÷ 8) y 1.**

A partir de ocho eventos este factor llega a 1. Antes atenúa la señal gradualmente.

**MSRI_RAW = HIST_EDGE × CONSISTENCY × SAMPLE_FACTOR.**

Combina predominio, el factor derivado de predominio y la cantidad de antecedentes.

### 6.3 Conversión a escalones

| `MSRI_RAW` | `MSRI_SIGNAL` |
|---|---|
| Menor que 0.10 | 0 |
| Desde 0.10 hasta menos de 0.20 | 0.25 |
| Desde 0.20 hasta menos de 0.40 | 0.50 |
| Desde 0.40 hasta menos de 0.60 | 0.75 |
| Desde 0.60 | 1.00 |

La igualdad pertenece al escalón que comienza en ese número. Por ejemplo, exactamente 0.20 produce 0.50.

### 6.4 Peso discreto de muestra

| Eventos | `SAMPLE_WEIGHT` |
|---|---|
| 3–4 | 0.40 |
| 5–7 | 0.60 |
| 8–12 | 0.80 |
| 13 o más | 1.00 |

**P5 = MSRI_SIGNAL × SAMPLE_WEIGHT.**

El tamaño interviene dos veces con funciones distintas: una atenuación continua antes del escalón y un peso por tramos después. No son nombres duplicados para la misma operación.

### 6.5 Fuerza del score final

| Score P5 | `P5_STRENGTH` |
|---|---|
| Menor que 0.10 | `NONE`, sin intensidad. |
| Desde 0.10 hasta menos de 0.25 | `WEAK`, débil. |
| Desde 0.25 hasta menos de 0.50 | `MODERATE`, moderada. |
| Desde 0.50 | `STRONG`, fuerte. |

Estas categorías son internas del motor. No significan probabilidades de acierto.

## 7. Ejemplo completo

Supongamos un vector 1X2 de Pinnacle con seis eventos únicos: cuatro resultados local, un empate y uno visitante.

1. Muestra: 6, suficiente.
2. Dominante: HOME con 4.
3. Frecuencia: 4/6 = 0.6667.
4. Baseline: 1/3 = 0.3333.
5. HIST_EDGE: (0.6667 − 0.3333) / (1 − 0.3333) = 0.50.
6. CONSISTENCY: 0.5 + 0.5 × 0.50 = 0.75.
7. SAMPLE_FACTOR: 6/8 = 0.75.
8. MSRI_RAW: 0.50 × 0.75 × 0.75 = 0.28125.
9. MSRI_SIGNAL: 0.50.
10. SAMPLE_WEIGHT: 0.60.
11. P5: 0.50 × 0.60 = 0.30.
12. Dirección HOME, fuerza MODERATE.

La frecuencia observada fue 66.67%, pero el score es 0.30. Son magnitudes distintas. Ese score no debe presentarse como «30% de probabilidad».

## 8. Betfair y la información diagnóstica

P5 puede conservar los precios y cantidades BACK/LAY de Betfair en `inputs` con `role = DIAGNOSTIC`. Las entradas de diagnóstico `exchange_exposure` indican mercado, periodo, lado, disponibilidad y referencias.

`participates_in_score = false` significa que esos datos no participan en las fórmulas ni crean una señal de memoria. Pueden ayudar a revisar el contexto, pero su sola presencia no hace exitoso P5. SofaScore participa como un perfil independiente, igual que Pinnacle y bet365.

## 9. La muestra que permite explicar un resultado después

Cuando se conserva la auditoría, se guardan los miembros exactos de la población utilizada y un encabezado con clave, filtros, corte y conteos. La muestra y el resultado pertenecen a la misma conservación conjunta.

`sample_id` permite recuperar esa población. Consultarla después no vuelve a buscar en cuotas cambiantes: utiliza los miembros capturados para esa ejecución. La consulta paginada devuelve porciones de la muestra, metadatos y un cursor para continuar.

Si no se conserva la muestra, las fórmulas siguen usando los conteos históricos y `sample_id` queda ausente. `audit_persisted` indica si se guardó la auditoría. Si guardar falla, no se mantienen referencias que aparenten apuntar a una muestra inexistente.

Esto añade reproducibilidad, no un peso al score.

## 10. Diccionario del perfil completo

| Campo de `BookmakerMemoryProfile` | Lectura |
|---|---|
| `bookmaker`, `bookie_id` | Casa que aportó el vector. |
| `market_group`, `market_period`, `market_shape` | Familia, periodo y forma de dos/tres resultados. |
| `target_minute`, `current_price_vector` | Momento actual y precios utilizados para buscar. |
| `P5_STATUS`, `P5_VALID` | Estado del perfil y si su cálculo es válido. |
| `P5_DIRECTION`, `DOMINANT_RESULT` | Dirección del score y resultado dominante; pueden quedar sin dirección. |
| `P5`, `P5_STRENGTH` | Score final y categoría de intensidad. |
| `memory_status`, `reason`, `is_tie` | Estado de la memoria, explicación y si hubo empate máximo. |
| `sample_size`, `wins_home`, `wins_draw`, `wins_away` | Población y conteos. |
| `wins_dominant`, `P_hist_DOMINANT` | Cantidad y frecuencia del dominante. |
| `BASELINE`, `HIST_EDGE`, `CONSISTENCY` | Referencia uniforme y factores de predominio. |
| `SAMPLE_FACTOR`, `SAMPLE_WEIGHT` | Atenuación continua y peso discreto de muestra. |
| `MSRI_RAW`, `MSRI_SIGNAL` | Combinación previa y su escalón. |
| `sample_id`, `diagnostics` | Muestra conservada, clave y exclusiones. |

`PopulationFilters` reúne competición, temporada y país cuando participan como restricciones. `MemoryQueryKey` define la coincidencia. `MemorySample` aporta conteos. `BookmakerMemoryProfile` contiene la interpretación y el score. Esa es la secuencia de objetos.

El resultado general añade `signals`, `coverage`, `inputs`, `contracts`, `analysis` y `diagnostics`. Cada perfil aparece separado por casa, familia y periodo. Los estados comunes están en la [guía principal](00-flujo-principal.md#8-lectura-de-resultados-de-p2p5).

## Recorrido integrado: del dato al resultado

Esta sección conecta vector, cuantización, población, conteos, predominio, factores de muestra, escalones, fuerza y auditoría. Se desarrolla el ejemplo de seis eventos de la sección 7 desde la entrada actual hasta el resultado general.

### Paso 1. Identificar la lectura actual

Supongamos una evaluación T−5 con 1X2 de tiempo reglamentario seleccionado. El historial organizado contiene, para Pinnacle, precios local 1.8504, empate 3.6004 y visitante 4.4004.

La identidad del evento indica encuentro, deporte e inicio. `target_selection` indica el momento actual; `market_evaluation` aporta el periodo y los contratos. P5 comprueba que los tres precios corresponden a la misma casa y mercado y que cada uno es finito y mayor que 1. Un precio de bet365 no completa el empate que pudiera faltar en Pinnacle.

### Paso 2. Transformar el vector en una clave exacta

El redondeo de mitad hacia arriba a `ODDS_QUANTUM = 0.001` produce **1.850 / 3.600 / 4.400**. Con el deporte, casa, familia 1X2, periodo y forma THREE_WAY se construye `MemoryQueryKey`.

Si hubiera una lectura Home/Away, se construiría otra clave con forma TWO_WAY y sin empate. Si se utiliza alguna restricción de competición, temporada o país, `PopulationFilters` la añade a la población consultada.

El T−5 actual identifica de dónde salió el vector. No exige que las observaciones históricas se hayan registrado también en T−5. La coincidencia utiliza los precios cuantizados y los demás campos de la clave.

### Paso 3. Obtener la población y explicar sus conteos

`PriceMemoryReader` consulta eventos anteriores al inicio del actual, con resultado y marcadores compatibles. Retira el evento presente, aplica los filtros elegidos y deduplica por evento. `MemorySample` devuelve todos los miembros elegibles resumidos en conteos.

Supongamos que quedan **seis eventos: cuatro local, uno empate y uno visitante**. Su suma es seis, igual a `sample_size`. Las exclusiones explican qué candidatos no entraron; no se añaden como derrotas ni como ceros a esos seis.

La muestra supera el mínimo de tres. HOME tiene la mayor frecuencia sin empate con otro resultado. Por eso puede continuar el cálculo direccional.

### Paso 4. Convertir predominio en puntuación

Los conteos alimentan esta cadena completa:

| Operación | Sustitución | Resultado |
|---|---|---|
| `P_hist_DOMINANT` | 4/6 | Aproximadamente 0.666667. |
| `BASELINE` | 1/3, porque hay tres resultados. | Aproximadamente 0.333333. |
| `HIST_EDGE` | (4/6 − 1/3)/(1 − 1/3) | 0.50. |
| `CONSISTENCY` | 0.5 + 0.5 × 0.50 | 0.75. |
| `SAMPLE_FACTOR` | mínimo(6/8, 1) | 0.75. |
| `MSRI_RAW` | 0.50 × 0.75 × 0.75 | 0.28125. |
| `MSRI_SIGNAL` | Escalón desde 0.20 hasta menos de 0.40. | 0.50. |
| `SAMPLE_WEIGHT` | Tramo de cinco a siete eventos. | 0.60. |
| `P5` | 0.50 × 0.60 | **0.30**. |
| `P5_STRENGTH` | Tramo desde 0.25 hasta menos de 0.50. | **MODERATE**. |

`DOMINANT_RESULT` y `P5_DIRECTION` conservan HOME. `P5_VALID` es verdadero; el perfil queda ACTIVE. `BookmakerMemoryProfile` reúne estos valores, el vector, la clave, los conteos y su explicación.

La frecuencia observada, el contraste histórico y el score son tres magnitudes distintas. El resultado 0.30 expresa la combinación definida por el motor; la frecuencia local observada fue 4/6.

### Paso 5. Procesar las demás casas y los resultados alternativos

El recorrido se repite de forma independiente para Pinnacle, bet365 y SofaScore. Cada casa utiliza sus propios precios y su propia población. Betfair conserva su papel de diagnóstico BACK/LAY, sin aportar un score de memoria.

Para completar el ejemplo, supongamos:

| Otro perfil | Consecuencia |
|---|---|
| bet365 tiene cuatro eventos: dos HOME y dos AWAY. | Muestra suficiente con empate máximo: `memory_status = TIE`, válido, dirección NONE y P5 = 0. |
| SofaScore tiene solo dos eventos coincidentes. | Muestra insuficiente: `P5_VALID = false` y P5 ausente; la señal queda BLOCKED por historial insuficiente. |
| No hay un vector Home/Away completo. | Esa lectura queda BLOCKED por falta de entrada, sin construir un vector con otra casa o familia. |

Un empate histórico calculado y una muestra insuficiente llegan a salidas distintas. La presencia de precios diagnósticos de Betfair tampoco reemplaza a una muestra insuficiente.

### Paso 6. Vincular la evidencia y producir el resultado general

Cada perfil aparece por casa, familia y periodo en `analysis`. `signals` publica su score o el motivo de indisponibilidad; `inputs` conserva el vector actual y el diagnóstico; `contracts` identifica el mercado; `coverage` describe ingredientes disponibles.

Las referencias de señal permiten volver al vector. Cuando se guarda auditoría, `sample_id` permite volver a los eventos históricos exactos y `audit_persisted` indica esa conservación. El encabezado registra clave, filtros, corte y conteos; los miembros congelados permiten verificar que los cuatro resultados HOME, el empate y el visitante son los que se utilizaron.

Consultar después una página de cien miembros no limita a cien la muestra utilizada. En el ejemplo existen seis; en otro caso se utilizan todos los elegibles, aunque la lectura posterior requiera varias páginas.

El resultado general es **ACTIVE**, porque Pinnacle y el empate válido de bet365 produjeron señales calculadas. No se promedian 0.30, cero y un dato ausente para fabricar un score del encuentro. Si una consulta falla, su ámbito conserva ERROR; los perfiles ya calculados siguen disponibles y el estado de ejecución refleja el error.

La lectura en lenguaje natural es: **«El vector actual de Pinnacle coincide con seis eventos anteriores; cuatro tuvieron resultado local y el motor obtiene 0.30, HOME, MODERATE. bet365 conserva una memoria neutral por empate de frecuencias. SofaScore no alcanza la muestra mínima. Cada conclusión mantiene su población y procedencia»**.

Para revisar el resultado se sigue **señal → perfil → conteos y fórmulas → clave y filtros → miembros de la muestra**, si fueron conservados. Ese camino enlaza todos los conceptos de la guía sin presentar el score como una probabilidad futura.

## 11. Fuentes y relación con los demás pilares

[Entrada de P5](../../../modules/pillars/pillar_5/run_pillar_5.py), [clave y cuantización](../../../modules/pillars/pillar_5/memory_sample.py), [fórmulas](../../../modules/pillars/pillar_5/memory_score.py), [objetos](../../../modules/pillars/pillar_5/calculation_models.py), [diagnóstico del exchange](../../../modules/pillars/pillar_5/exchange_diagnostics.py) y [población histórica](../../../infrastructure/persistence/repositories/pillar_5_price_memory_repository.py).

[P2](02-pilar-2-mercado-de-lado.md) explica la orientación actual; [P4](04-pilar-4-movimiento-temporal.md), el recorrido; [P1](01-pilar-1-estructura-deportiva.md), la historia deportiva. El coordinador conserva estas perspectivas sin fusionarlas en una sola decisión.

