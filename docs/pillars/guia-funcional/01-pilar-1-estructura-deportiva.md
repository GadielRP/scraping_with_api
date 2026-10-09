# Pilar 1: estructura deportiva, contexto competitivo y perfil de totales

[Volver a la guía principal](00-flujo-principal.md) · [P2: mercado de lado](02-pilar-2-mercado-de-lado.md) · [P3: mercado de totales](03-pilar-3-mercado-de-totales.md) · [P4: movimiento](04-pilar-4-movimiento-temporal.md) · [P5: memoria](05-pilar-5-memoria-de-precios.md)

Este documento explica la implementación revisada el 8 de octubre de 2026. P1 pregunta **qué muestran los resultados deportivos de los participantes, la calidad de sus rivales y su situación en la competición**. Produce dos respuestas: un perfil de **lado**, que compara local y visitante, y un perfil de **totales**, que estudia la anotación conjunta. Cada respuesta conserva sus cálculos y reglas de interpretación.

P1 es el pilar más extenso. Su análisis de lado contiene siete módulos y una política de combinación; el de totales contiene tres capas direccionales, un análisis separado de variabilidad y una lectura compuesta. Las etiquetas describen perfiles del motor. Un valor 0.40 no equivale a 40 % de probabilidad de victoria.

Para seguir cómo se utilizan conjuntamente las definiciones y fórmulas, lea el [recorrido integrado del dato al resultado](#recorrido-integrado-del-dato-al-resultado), con pasos, ejemplo y lectura final.

## 1. Ruta de lectura y mapa del pilar

| Sección | Contenido |
|---|---|
| [2. Entradas](#2-el-expediente-deportivo-de-entrada) | De dónde viene la información y qué significa cada familia de datos. |
| [3. Convenciones](#3-convenciones-y-operaciones-compartidas) | Signos, escalas, medias, estados y objetos de resultado. |
| [4. M1](#4-m1-fuerza-base) | Resultados de temporada, diferencia de anotación, consistencia y capacidad de imponerse. |
| [5. M2](#5-m2-perfil-ofensivo) | Regularidad anotadora, techo, partidos sin anotar y frecuencia de anotación alta. |
| [6. M3](#6-m3-enfrentamiento-directo) | Historia entre los dos participantes y ajuste por cantidad de partidos. |
| [7. M4](#7-m4-estado-inmediato-ajustado-por-rival) | Últimos resultados ponderados por la posición del oponente. |
| [8. M5](#8-m5-coste-competitivo-del-contexto) | Distancia en puntos a zonas de clasificación y tiempo restante. |
| [9. M6](#9-m6-evolución-estructural) | Cambio de rendimiento a lo largo de cinco bloques cronológicos. |
| [10. M7](#10-m7-rendimiento-frente-a-la-expectativa-del-rival) | Si cada resultado supera o queda por debajo de una expectativa basada en el rival. |
| [11. Combinación de lado](#11-cómo-se-combinan-los-siete-módulos-de-lado) | Pesos, ajuste de M4, presión de M5 y todas las reglas de estado final. |
| [12–18. Totales](#12-p1-totales-entradas-y-ventanas) | Ventanas, estructura, temporalidad, tendencia, variabilidad, dirección y compuesto. |
| [19. Diccionario de salida](#19-diccionario-del-resultado-de-totales) | Significado de todos los campos del objeto de totales. |
| [20. Fuentes](#20-fuentes-y-documentación-relacionada) | Archivos responsables de cada cálculo. |

La entrada general es `run_pillar_1_team_structure`. Para lado, `calculate_p1_side` llama a M1, M2, M3, M4, M5, M6 y M7, en ese orden. Después combina sus resultados. El coordinador también llama al motor de totales. Una ausencia de resultado de totales no elimina un resultado de lado ya calculado.

## 2. El expediente deportivo de entrada

### 2.1. Identidad e historia son expedientes distintos

`EventContext` identifica el encuentro: participantes, competición, temporada, inicio y minutos restantes. Sus campos de negocio se explican en la [guía principal](00-flujo-principal.md#4-expedientes-variables-y-objetos-compartidos).

`event_context.streak_analysis` contiene un `MatchupStreakContext`: el historial deportivo preparado para ese encuentro. P1 requiere ese historial; el orquestador toma el que está asignado al expediente del evento.

Antes de P1, el sistema reutiliza un historial ya preparado o intenta construirlo en la evaluación de cinco minutos antes del inicio. Para construirlo necesita identificar correctamente los dos participantes, la competición y la temporada. El resolver obtiene los enfrentamientos previos y el constructor prepara resultados de los equipos, clasificación y referencias de liga. El expediente se guarda en memoria para que los recorridos que lo comparten no lo vuelvan a preparar innecesariamente.

El historial de enfrentamientos directos se prepara con encuentros anteriores al actual y dentro de los últimos 730 días. En deportes de equipo se comprueba la competición; en tenis se consideran superficie y modalidad individual o dobles. Los resultados se orientan hacia los participantes del encuentro actual: si hoy A es local pero fue visitante en el pasado, su marcador sigue atribuido a A.

La forma de cada equipo puede contener todos los partidos disponibles de una temporada recopilada; **no debe interpretarse siempre como «los últimos diez»**, aunque algunos comentarios históricos del código utilicen esa expresión. Cada módulo selecciona su propia muestra. Los cálculos de liga se construyen con información anterior al corte del encuentro.

### 2.2. Diccionario de las entradas deportivas

| Campo o familia | Significado |
|---|---|
| `home_team_results`, `away_team_results` | Listas de partidos del participante que hoy es local y del que hoy es visitante. |
| `team_score`, `opponent_score` | Anotación del equipo estudiado y de su rival en un partido. |
| `score_for`, `goals_for`, `gf` | Nombres alternativos de anotación a favor que determinados módulos aceptan. |
| `score_against`, `goals_against`, `ga` | Anotación recibida; el nombre concreto depende del registro y del módulo. |
| `goal_diff`, `diff`, `net_score` | Diferencia entre anotación a favor y en contra. Positiva significa que el equipo terminó por encima del rival. |
| `game_total` | Suma de la anotación de ambos participantes. |
| `team_result_code`, `team_result` | Resultado desde la perspectiva del equipo: victoria, empate o derrota. Los módulos traducen los códigos que admiten a una lectura común. |
| `opponent_name`, `opponent_ranking` | Identidad y posición del rival. Un puesto 1 es mejor que un puesto 20. |
| `event_id`, `startTimestamp` | Identidad y fecha del partido histórico. Permiten reconocerlo y ordenar la historia. |
| `home_team_current_standing`, `away_team_current_standing` | Fila de clasificación actual de cada participante. |
| `home_team_standing`, `away_team_standing` | Otros registros de clasificación disponibles en el contexto. |
| `current_standings` | Clasificación conjunta, consultable por nombre de equipo. |
| `standing`, `opponent_standing` dentro de un partido | Clasificaciones asociadas al equipo y a su rival en ese registro. |
| `wins`, `games_played`, `goal_diff` en clasificación | Victorias, partidos disputados y diferencia de anotación acumulada. No confundir esta diferencia de temporada con la de un solo encuentro. |
| `points`, `rank` o `position` | Puntos de clasificación y puesto. Los puntos de tabla son distintos de los puntos o goles del marcador. |
| `h2h_matchup_matches` | Partidos previos entre los dos participantes actuales. |
| `home_score`, `away_score` en H2H | Marcadores ya orientados al local y visitante del próximo encuentro. |
| `hist_home`, `hist_away`, `hist_home_score`, `hist_away_score` | Identidades y marcadores según la localía que existió en el encuentro histórico. |
| `current_standings_source`, `current_standings_cutoff_timestamp` | Procedencia de la clasificación y fecha de corte. Explican qué información se utilizó. |
| `league_totals_context.matches` | Partidos de la liga con sus marcadores y totales. Sirven como referencia de anotación. |
| `league_totals_context.teams` | Historias por equipo de la liga, con resultados y series de totales. Sirven para referencias de tendencia y variabilidad. |
| `number_of_teams` | Tamaño de la liga. M1, M4 y M7 lo utilizan de formas distintas. |
| `total_regular_season_games` | Partidos esperados por equipo en temporada regular. Es obligatorio para construir las ventanas de P1 Totales. |

El contexto también contiene conteos de victorias, derrotas, empates, rachas, lotes de resultados y rankings resumidos. Son antecedentes disponibles para otros recorridos; P1 vuelve a calcular las métricas que se explican aquí a partir de las entradas que cada módulo utiliza. Los precios de apertura y cierre que pueda incluir el contexto no intervienen en estas fórmulas deportivas.

### 2.3. Una misma entrada no implica una misma muestra

M1 analiza registros de temporada y ventanas acumulativas. M2 usa anotaciones válidas de cada equipo. M3 usa enfrentamientos directos. M4 toma los últimos cinco partidos válidos con posición de rival. M5 usa la tabla. M6 reparte la historia en cinco bloques. M7 retira un partido antiguo y estudia resultados frente a una expectativa. Totales equilibra las cantidades de partidos de ambos equipos y construye otras cuatro ventanas.

Por eso dos módulos pueden utilizar números distintos de encuentros sin que exista contradicción. Los campos de muestra de cada resultado explican esa diferencia.

## 3. Convenciones y operaciones compartidas

### 3.1. Signo, magnitud y límite

En los módulos de lado, un valor positivo favorece al local (`HOME`), uno negativo al visitante (`AWAY`) y cero es neutral (`NEUTRAL`); M5 expresa la ausencia de presión como `NONE`. La **magnitud** es el valor absoluto: −0.40 y +0.40 tienen la misma magnitud, pero distinta dirección.

`edge` significa contraste o ventaja relativa. `raw` identifica los datos e intermedios que explican cómo se obtuvo. `final` es el valor después de los ajustes definidos por ese cálculo. No todos los nombres contienen un ajuste adicional: en algunas capas de totales, señal original y final coinciden.

**Limitar**, llamado `clamp`, consiste en mantener un número dentro de un intervalo. Con el intervalo habitual de −1 a +1, 1.30 pasa a 1 y −1.30 pasa a −1; 0.30 se conserva. Cuando se usa de 0 a 1 o de −0.06 a +0.06 se indica expresamente.

La intensidad general de lado se calcula sobre la magnitud:

| Magnitud | Etiqueta | Lectura |
|---|---|---|
| Menor que 0.05 | `IGNORE` | Contraste demasiado pequeño para un núcleo operativo. |
| Desde 0.05 hasta menos de 0.15 | `LOW` | Bajo. |
| Desde 0.15 hasta menos de 0.30 | `MEDIUM` | Medio. |
| Desde 0.30 hasta menos de 0.60 | `HIGH` | Alto. |
| Desde 0.60 | `EXTREME` | Extremo. |

Esta tabla se usa en M1–M6 y en el núcleo de lado. M7, la presión de M5 dentro de la combinación y Totales tienen tablas propias. No deben intercambiarse.

### 3.2. Operaciones en lenguaje cotidiano

Una **media** suma valores y divide por su cantidad. Una **media ponderada** da diferente importancia a cada valor: multiplica cada uno por su peso y combina las aportaciones. Cuando se divide por la suma de los pesos se indica; algunas sumas del motor no hacen esa división.

La **desviación estándar**, representada a veces por `std` o σ, mide dispersión. El código usa la versión poblacional: obtiene la media, calcula la distancia de cada dato a esa media, eleva las distancias al cuadrado, las promedia y toma la raíz cuadrada. Una serie constante tiene desviación cero.

Un **percentil** ordena datos y busca una referencia de posición. P50 es la mediana; P75 representa la referencia del 75 %; P90, la del 90 %. M1 utiliza una posición entera para su P75. Totales utiliza interpolación entre posiciones cuando hace falta. Se detallan ambos procedimientos en sus secciones.

Una **diferencia relativa** de A frente a B suele ser:

**(A − B) ÷ (valor absoluto de A + valor absoluto de B).**

Permite comparar signos y tamaños dentro de −1 a +1. Si ambos son cero, el denominador también lo es y estos cálculos devuelven cero. Si un lado es positivo y el otro negativo, la diferencia relativa puede llegar a 1 aunque sus cantidades absolutas sean pequeñas.

### 3.3. Objeto de resultado y disponibilidad

Cada `ModuleResult` reúne:

| Campo | Interpretación |
|---|---|
| `pillar_id`, `module_id`, `module_name` | Pilar, código M1–M7 y nombre del análisis. |
| `event_id`, `participants` | Encuentro al que corresponde. |
| `value` | Contraste numérico del módulo. |
| `bias` | Dirección local, visitante o neutral. |
| `strength` | Categoría de magnitud. |
| `components` | Subcálculos que explican el módulo. |
| `raw` | Entradas utilizadas, fórmulas intermedias, muestras, fuentes y estado. |

Cada `ModuleComponentResult` contiene `name` —nombre—, `edge` —contraste—, `bias`, `strength`, `weight` —peso—, `weighted_edge` —aportación después del peso— y `raw`. Los componentes explican el cálculo; no siempre forman una suma directa, como ocurre con M5.

`ACTIVE` significa que el módulo dispone de los datos que exige para su estado completo. `DEGRADED` indica una muestra o procedencia menos completa, pero conserva su valor en la combinación. `INSUFFICIENT_DATA`, `INACTIVE` y los estados que comienzan por `INVALID` no aportan valor efectivo al núcleo. La ausencia de datos no debe leerse como un empate deportivo.

## 4. M1: fuerza base

M1 responde: **¿qué ventaja general muestra cada equipo en resultados y márgenes, y cómo de estable o dominante ha sido?** Contiene cuatro componentes.

### 4.1. Registro de temporada y procedencia

Para cada equipo construye un registro con `wins` —victorias—, `games_played` —partidos jugados— y `goal_diff` —diferencia acumulada—. Prioriza su clasificación actual directa, después la fila correspondiente en `current_standings` y después clasificaciones incorporadas a sus resultados. Si no dispone de un registro utilizable, reconstruye una aproximación desde la lista de partidos.

La reconstrucción cuenta las entradas como partidos, cuenta como victoria el código `"1"` y suma las diferencias numéricas disponibles. Su procedencia queda identificada; no se presenta como una clasificación oficial completa.

Los márgenes de cada partido se buscan en `goal_diff`, luego `diff`, después se calculan con los marcadores a favor y en contra y, como alternativa, se toma `net_score`. Solo los valores válidos forman la serie de márgenes.

### 4.2. Ventaja de resultados: peso 0.35

Primero calcula **tasa de victoria = victorias ÷ partidos disputados**. Después:

**result_edge = limitar(tasa de victoria local − tasa de victoria visitante).**

Con 6 victorias de 10 frente a 4 de 10, resulta 0.60 − 0.40 = 0.20. Aquí el empate no recibe media victoria: únicamente las victorias entran en el numerador.

### 4.3. Diferencia de anotación: peso 0.35

Para cada equipo obtiene **diferencia por partido = diferencia acumulada ÷ partidos disputados**.

Para poner ese contraste en contexto de liga, reúne las magnitudes de diferencia por partido de equipos con registros utilizables. Prioriza la clasificación conjunta; puede utilizar la respuesta de clasificación de la competición y, si hace falta, registros incluidos en los historiales. Evita contar varias veces al mismo equipo.

Ordena esas magnitudes y toma el elemento de posición **techo de 0.75 × cantidad**, contando posiciones desde 1. Ese P75 se conserva como `m1_gd_dynamic_scale` y como `dynamic_scale` dentro del detalle de ese componente. En cuatro valores ordenados 0.10, 0.20, 0.40 y 0.80, la escala es 0.40: el tercer valor.

**gd_edge = limitar((diferencia por partido local − diferencia por partido visitante) ÷ escala de liga).**

Una diferencia de 0.20 con escala 0.40 produce 0.50; con escala 1.00 produce 0.20. La misma distancia deportiva puede tener distinta importancia relativa según la distribución de la liga. La escala debe ser positiva.

### 4.4. Consistencia: peso 0.10

Toma la cantidad de márgenes que ambos equipos pueden aportar y construye ventanas acumulativas de 5, 10, 15 y así sucesivamente. Si quedan datos después del último múltiplo de cinco, añade una ventana con toda la cantidad común. Con 12 datos por lado compara ventanas de 5, 10 y 12.

Para cada ventana calcula la desviación estándar de márgenes de cada equipo:

**diferencia de consistencia = desviación visitante − desviación local.**

Promedia esas diferencias y limita el resultado. Menor dispersión local favorece al local. El módulo usa el orden recibido de las listas; las entradas deportivas se preparan normalmente con lo más reciente primero.

La consistencia mide estabilidad del margen, no su nivel. Un equipo que pierde siempre por lo mismo puede ser estable y seguir siendo débil en los otros componentes.

### 4.5. Dirección de la volatilidad: peso 0.20

Usa las mismas ventanas. Separa los márgenes positivos y negativos:

- `destroy_power`: media de los márgenes positivos.
- `collapse_power`: magnitud de la media de los negativos.
- `net_power`: capacidad de imponerse menos magnitud de las caídas.

Si no hay márgenes de una clase, su potencia vale cero. Los empates de margen cero no entran en ninguno de esos promedios. Por ventana compara `net_power` local menos visitante; promedia las diferencias de ventanas y limita el resultado.

Por ejemplo, los márgenes +3, +1, −2 y 0 dan potencia positiva 2, caída 2 y potencia neta 0. El cálculo examina el tamaño medio de victorias y derrotas; no los pondera aquí por su frecuencia.

### 4.6. Resultado y datos que lo explican

**M1 = limitar(0.35 × result_edge + 0.35 × gd_edge + 0.10 × consistency_edge + 0.20 × vol_direction_edge).**

Con componentes 0.20, 0.30, 0.10 y 0.20, las aportaciones son 0.070, 0.105, 0.010 y 0.040. M1 resulta 0.225: HOME, MEDIUM.

El resultado conserva registros de temporada, series y ventanas, escala y fuente, los cuatro contrastes, pesos y aportaciones. `m1_edge`, `m1_abs_edge`, `m1_bias`, `m1_strength`, `m1_status` y `m1_status_reason` resumen valor, magnitud, dirección, intensidad, disponibilidad y explicación.

Necesita registros utilizables y al menos cinco márgenes por lado para los componentes de ventanas. Una escala inexistente o no positiva impide el cálculo válido. La procedencia de la clasificación, su cobertura y si se conoce el número esperado de equipos distinguen ACTIVE de DEGRADED. **DEGRADED no multiplica automáticamente M1 por un descuento**: la combinación utiliza su valor calculado.

## 5. M2: perfil ofensivo

M2 pregunta: **¿quién anota con más regularidad, alcanza mejores techos y evita quedarse sin anotar?**

Extrae anotación a favor válida de `team_score`, `score_for`, `goals_for` o `gf`. Cada equipo conserva su propia cantidad de datos; M2 no iguala obligatoriamente las muestras.

| Componente | Cálculo del contraste local | Peso |
|---|---|---|
| Regularidad ofensiva | Desviación de anotación visitante − desviación local. | 0.35 |
| Techo ofensivo | Media de las cinco mayores anotaciones locales − media de las cinco mayores visitantes. | 0.30 |
| Evitar quedarse en cero | Tasa de ceros visitante − tasa de ceros local. | 0.20 |
| Explosión ofensiva | Tasa de anotaciones de al menos 2 local − la visitante. | 0.15 |

Si un equipo tiene menos de cinco anotaciones, su techo usa todas las disponibles. La tasa de ceros divide partidos con anotación exactamente cero por la cantidad válida. La tasa de explosión cuenta valores **mayores o iguales a 2.0** y divide por esa misma cantidad.

El umbral 2.0 es fijo en esta implementación; no se adapta automáticamente a la escala de cada deporte. «Explosión» es el nombre interno de esa condición, no una conclusión universal sobre qué constituye una anotación excepcional.

Los contrastes de desviación y techo conservan sus unidades y pueden superar 1 antes de la combinación. M2 limita la suma final:

**M2 = limitar(0.35 × regularidad + 0.30 × techo + 0.20 × evitar ceros + 0.15 × explosión).**

Por ejemplo, contrastes 0.20, 1.00, 0.10 y 0.20 aportan 0.070 + 0.300 + 0.020 + 0.030 = 0.420: HOME, HIGH.

Si ambos equipos tienen al menos cinco anotaciones válidas, el estado es ACTIVE. Si alguno tiene entre una y cuatro, es DEGRADED y aún calcula. Si no hay anotación válida de alguno, queda INSUFFICIENT_DATA.

El reporte incluye series, cantidades, desviaciones, mayores anotaciones, medias del techo, ceros, tasas de explosión, pesos y contribuciones. `m2_edge_raw` es la suma previa al límite y `m2_edge` el resultado. La etiqueta adicional `m2_bias_label` distingue `SLIGHT_HOME` o `SLIGHT_AWAY` cuando el valor no es cero pero su magnitud es menor que 0.05. El `bias` ordinario conserva HOME o AWAY y la intensidad general puede ser IGNORE.

## 6. M3: enfrentamiento directo

M3 responde: **¿cómo les ha ido a estos participantes cuando se enfrentaron entre sí?**

### 6.1. Orientación de los resultados

Cada `ParsedH2HMatch` representa un encuentro directo con anotación de los participantes que hoy son local y visitante, diferencia, resultado, fecha, localía histórica y registro original. Si recibe marcadores ya normalizados, los utiliza. Si recibe identidades y marcadores históricos, los reorienta para no confundir al actual local con quien era local entonces.

Ordena por fecha cuando está disponible. M3 calcula sobre todos los enfrentamientos válidos que recibe. La ventana de dos años se aplica al preparar el historial, no mediante otra ponderación por antigüedad dentro de M3.

### 6.2. Contraste de resultados y de anotación

Con `n` encuentros, asigna una unidad por victoria y media unidad por empate:

**tasa local = (victorias locales + 0.5 × empates) ÷ n.**

**tasa visitante = (victorias visitantes + 0.5 × empates) ÷ n.**

**win_edge = limitar(tasa local − tasa visitante).**

Suma también los marcadores de cada participante:

**points_edge = limitar((anotación local acumulada − anotación visitante acumulada) ÷ (anotación local acumulada + anotación visitante acumulada)).**

Si ambos acumulados son cero, ese segundo contraste es cero. Combina ambos:

**m3_raw_edge = limitar(0.70 × win_edge + 0.30 × points_edge).**

La palabra `points` aquí se refiere a unidades del marcador, no a puntos de clasificación.

### 6.3. Ajuste por muestra

**m3_sample_factor = mínimo entre n ÷ 5 y 1.**

**M3 = limitar(m3_raw_edge × m3_sample_factor).**

Con uno, dos, tres, cuatro y cinco o más encuentros, los factores son 0.20, 0.40, 0.60, 0.80 y 1.00. Este módulo sí reduce explícitamente el contraste por muestra pequeña.

Ejemplo: el local ganó los dos enfrentamientos válidos y acumula marcador 4 frente a 1. `win_edge` = 1; `points_edge` = 3 ÷ 5 = 0.60. El contraste original es 0.88. Con factor 0.40, el resultado final es 0.352: HOME, HIGH.

La confianza de muestra es otra etiqueta:

| Factor | `m3_sample_confidence` |
|---|---|
| Cero | `NONE` |
| Menor que 0.25 | `VERY LOW` |
| Desde 0.25 hasta menos de 0.50 | `LOW` |
| Desde 0.50 hasta menos de 0.75 | `MEDIUM` |
| Desde 0.75 hasta 0.90 inclusive | `HIGH` |
| Mayor que 0.90 | `VERY HIGH` |

Esta confianza describe cantidad de antecedentes, no probabilidad de acertar. M3 queda INACTIVE sin enfrentamientos válidos; con al menos uno queda ACTIVE, aunque su factor sea pequeño. El reporte conserva victorias, empates, tasas, anotación acumulada, contraste original, factor, confianza y contraste final. Las intensidades original y final permiten ver cuánto cambió por el ajuste.

## 7. M4: estado inmediato ajustado por rival

M4 pregunta: **¿qué valor tienen los resultados recientes al considerar la posición de los oponentes?** No trata igual ganarle al primero que al último.

### 7.1. Selección y tamaño de liga

Ordena los partidos del más reciente al más antiguo y busca hasta cinco válidos por equipo. Para esta selección necesita el resultado, ambos marcadores numéricos (`team_score` y `opponent_score`) y `opponent_ranking`. Calcula el margen restando los marcadores. Si un registro no permite el cálculo, continúa buscando en los anteriores; el reporte conserva los descartes y motivos.

El tamaño `n_liga` se obtiene primero de la cantidad de filas de `current_standings`; después, del número de equipos de la competición; como alternativa, de la mayor posición inferida en los registros. La procedencia queda registrada.

Con cinco válidos por lado queda ACTIVE. Con al menos tres por lado, pero menos de cinco en alguno, queda DEGRADED. Con menos de tres en alguno, queda INSUFFICIENT_DATA y no produce una aportación operativa.

### 7.2. Fuerza y debilidad del oponente

**opponent_strength = limitar entre 0 y 1 [1 − (posición del rival − 1) ÷ (n_liga − 1)].**

**opponent_weakness = 1 − opponent_strength.**

En una liga de 20, el rival primero tiene fuerza 1 y debilidad 0; el último, fuerza 0 y debilidad 1. Un rival intermedio queda entre ambos extremos.

El código traduce victoria a +1, empate a 0 y derrota a −1. El resultado ajustado se asigna así:

| Resultado | `adjusted_result` |
|---|---|
| Victoria | Fuerza del rival. |
| Empate | 0. |
| Derrota | Menos la debilidad del rival. |

Por tanto, ganarle al primero aporta +1; ganarle al último aporta 0. Perder ante el primero aporta 0; perder ante el último aporta −1. Son extremos de una política de valoración explícita.

El margen se ajusta por separado:

- Margen positivo: multiplicarlo por la fuerza del rival.
- Margen negativo: multiplicarlo por la debilidad del rival.
- Margen cero: conservar cero.

Una victoria por dos ante el primero aporta margen ajustado +2; ante el último aporta 0. Una derrota por dos ante el último aporta −2.

### 7.3. Comparación y resultado

Suma los resultados ajustados de cada equipo y compara esas sumas mediante diferencia relativa. Hace lo mismo con las sumas de márgenes ajustados:

**result_score = (suma local de resultados ajustados − suma visitante) ÷ (magnitud de suma local + magnitud de suma visitante).**

**gd_score = la misma comparación usando sumas de márgenes ajustados.**

Los denominadores cero producen contraste cero.

**M4 = limitar(0.60 × result_score + 0.40 × gd_score).**

El resultado conserva tamaño de liga y fuente; partidos utilizados y descartados; fuerza y debilidad de cada rival; resultados y márgenes ajustados; sumas por equipo y los dos contrastes. El valor completo puede alcanzar ±1. **Su efecto posterior sobre el núcleo de P1 se limita a ±0.06**, como se explica en la sección 11.

## 8. M5: coste competitivo del contexto

M5 pregunta: **¿qué presión comparativa representa la distancia a un objetivo de clasificación, dado el tiempo restante?** Modela un coste competitivo a partir de posiciones y puntos. No mide emociones de jugadores ni incorpora directamente forma, diferencia de goles o tasa de puntos por partido.

### 8.1. Tabla, partidos restantes y puntos posibles

Cuenta filas válidas de clasificación: posición positiva y puntos no negativos. Ese conteo es `n_league`. Para cada participante utiliza su clasificación directa válida o busca la fila por nombre.

La duración de temporada utilizada **dentro de M5** sigue esta regla:

- Con 18 o más equipos, presupone 38 partidos por equipo.
- Con menos de 18, presupone 2 × (n_league − 1).

Es una suposición propia del módulo. No toma en esta fórmula `total_regular_season_games`, aunque ese campo sí interviene en P1 Totales.

`games_played_reference` es el mayor número de partidos disputados entre los participantes y los registros de liga disponibles. No utiliza una cantidad restante diferente para cada participante.

**games_remaining = máximo entre partidos de temporada − referencia disputada y 0.**

**max_points_remaining = 3 × games_remaining.**

El valor 3 es fijo: representa tres puntos posibles por victoria. En una liga de 20 con referencia de 30 partidos jugados, quedan 38 − 30 = 8 y el horizonte es 24 puntos.

### 8.2. Objetivo y régimen

La selección actual del objetivo es:

| Situación | `objective_type` |
|---|---|
| Liga de al menos 18 equipos y posición entre 1 y 7 | `EUROPE`: zona superior definida por el motor. |
| Liga de al menos 18 y posición en los últimos tres puestos | `SURVIVAL`: permanencia. |
| Otras posiciones válidas | `MIDTABLE`: zona intermedia. |
| Datos que no permiten identificar objetivo | `NONE`. |

La etiqueta EUROPE expresa la zona que el código define; no verifica por sí misma las plazas internacionales oficiales de cada competición. En ligas con menos de 18 equipos, la selección actual asigna MIDTABLE a todas las posiciones válidas.

La **severidad base** asignada al objetivo es:

| Objetivo | `base_severity` |
|---|---|
| EUROPE | 0.70 |
| PLAYOFF | 0.85 |
| SURVIVAL | 1.00 |
| MIDTABLE | 0.30 |
| NONE | 0 |

PLAYOFF tiene un valor en la tabla de severidad, pero la función actual de selección no asigna ese objetivo.

El **régimen** expresa la relación con una zona:

| Régimen | `regime_multiplier` |
|---|---|
| `ON_TARGET_ZONE`: dentro del objetivo superior | 1.00 |
| `OUTSIDE_TARGET`: fuera del objetivo superior | 1.10 |
| `INSIDE_DANGER`: dentro de peligro | 1.25 |
| `SAFE_ZONE`: fuera del peligro o zona intermedia | 0.80 |
| `NONE` | 0 |

Con la selección actual, los equipos EUROPE se encuentran en ON_TARGET_ZONE, los SURVIVAL en INSIDE_DANGER y los MIDTABLE en SAFE_ZONE. Otras ramas están definidas para interpretar objetivos y regímenes, aunque esa selección no las active normalmente.

### 8.3. Punto de corte y distancia

`points_cutoff` representa los puntos de la posición de referencia:

- EUROPE compara con el puesto 7; si el equipo está exactamente séptimo, compara con el 8.
- SURVIVAL compara con el puesto `n_league − 3`, inmediatamente por encima de los tres últimos.
- MIDTABLE toma los puntos propios como referencia, pero asigna una distancia equivalente a todo el horizonte de puntos.

Si falta la fila exacta de corte, busca la posición disponible más cercana con puntos válidos; en empate de distancia prioriza el número de posición menor.

`gap` es la distancia:

- Dentro de la zona superior: puntos propios menos puntos del corte.
- Fuera de esa zona: puntos del corte menos puntos propios.
- Dentro del peligro: puntos del corte menos puntos propios.
- Fuera del peligro: puntos propios menos puntos del corte.
- MIDTABLE o NONE: horizonte completo de puntos restantes.

Esta distancia puede mostrar un colchón o el coste de alcanzar otra zona. Para la urgencia, una distancia negativa se trata como cero.

### 8.4. Urgencia, coste y comparación

Si el objetivo no existe o no quedan puntos posibles, la urgencia es cero. En otro caso:

**urgency_factor = limitar entre 0 y 1 [1 − máximo(gap, 0) ÷ max_points_remaining].**

**final_cost = base_severity × regime_multiplier × urgency_factor.**

Cuanto más pequeña es la distancia frente a los puntos todavía posibles, más cerca queda la urgencia de 1. Si la distancia alcanza o supera todo ese horizonte, queda en 0. El coste puede superar 1: SURVIVAL con multiplicador 1.25 y urgencia 1 produce 1.25. La limitación de −1 a +1 se aplica al contraste entre costes, no a cada coste individual.

En MIDTABLE, la distancia asignada coincide con todo el horizonte; con horizonte positivo, la urgencia queda en cero. El objetivo intermedio no produce presión numérica con esta política.

**M5 = limitar((coste local − coste visitante) ÷ (magnitud del coste local + magnitud del coste visitante)).**

Si ambos costes son cero, M5 es cero y `m5_pressure_side` es NONE. Si el local tiene mayor coste, HOME; si el visitante lo tiene, AWAY. Si solo uno tiene coste positivo, el contraste llega a ±1: expresa asimetría de costes, no certeza de ganar.

Ejemplo: quedan 24 puntos posibles. El local está en SURVIVAL a seis puntos del corte: urgencia 1 − 6/24 = 0.75; coste 1 × 1.25 × 0.75 = 0.9375. El visitante está en EUROPE con distancia 12: urgencia 0.50; coste 0.70 × 1 × 0.50 = 0.35. El contraste es 0.5875 ÷ 1.2875 ≈ 0.4563 hacia el local.

Si la información no permite un estado activo, el módulo deja los costes operativos en cero. Conserva, por equipo, posición, puntos, partidos, objetivo, régimen, corte, distancia, urgencia, severidad, multiplicador y coste; además, el horizonte común y la regla de temporada.

Sus componentes de coste local y visitante permiten auditar los dos lados, pero **M5 se calcula mediante la diferencia relativa**, no sumando esas fichas de componentes. La combinación de P1 asigna a M5 una escala de intensidad especial.

## 9. M6: evolución estructural

M6 pregunta: **¿cómo ha cambiado el margen deportivo del equipo a lo largo de su historia disponible y cuán estable es ese recorrido?**

### 9.1. Historia y cinco bloques

Obtiene márgenes de `net_score` o de anotación a favor menos recibida. Ordena de antiguo a reciente cuando dispone de fechas; como alternativa, invierte la lista recibida, que normalmente viene de reciente a antiguo.

Divide cada historia en cinco bloques consecutivos, sin superponerlos. Con `n` datos, las fronteras son los enteros redondeados de `i × n / 5`, para i de 0 a 5. En empates exactos el redondeo utiliza el entero par. Cada bloque produce una media de margen: el vector tiene cinco valores del tramo más antiguo al más reciente.

Esto es distinto de las ventanas acumulativas de M1. Cada encuentro ocupa un solo bloque de M6.

### 9.2. Pendiente y estabilidad

Llama `trend` a la pendiente del vector. Para las posiciones 1, 2, 3, 4 y 5, resta 3 a cada posición y resta la media general a cada media de bloque:

**pendiente = suma de [(posición − 3) × (media del bloque − media de las cinco medias)] ÷ 10.**

El 10 procede de 4 + 1 + 0 + 1 + 4, la suma de los cuadrados de las distancias de posición. Una pendiente positiva indica mejora del margen a lo largo de los bloques; una negativa, deterioro.

**trend_global_edge = limitar((pendiente local − pendiente visitante) ÷ (magnitud de ambas pendientes sumadas)).**

Calcula también la desviación estándar de las cinco medias de bloque de cada equipo:

**stability_edge = limitar((desviación visitante − desviación local) ÷ (desviación visitante + desviación local)).**

Los denominadores cero producen contraste cero. La estabilidad aquí corresponde a medias de bloques, no a todos los marcadores individuales.

**M6 = limitar(0.65 × trend_global_edge + 0.35 × stability_edge).**

Ejemplo: el local tiene medias −2, −1, 0, 1, 2 y el visitante 0, 0, 0, 0, 0. Las pendientes son 1 y 0: ventaja de tendencia +1. El visitante es más estable: contraste de estabilidad −1. M6 = 0.65 − 0.35 = 0.30, HOME y HIGH según el límite inclusivo de esa categoría.

### 9.3. Muestra y campos de auditoría

Con menos de diez márgenes válidos en cualquiera de los dos equipos queda INSUFFICIENT_DATA y su valor final es cero. Con al menos diez por lado, pero menos de veinte en alguno, queda DEGRADED. Con veinte o más en ambos, ACTIVE.

El reporte guarda márgenes cronológicos, tamaños de bloques, vectores de cinco medias, pendiente por equipo, dispersión por equipo, contrastes de tendencia y estabilidad, pesos, resultado y estado. Una mejora relativa no exige que el equipo ya tenga margen positivo: puede estar perdiendo por menos y mostrar pendiente favorable.

## 10. M7: rendimiento frente a la expectativa del rival

M7 responde: **¿el equipo consigue resultados y márgenes mejores o peores de lo que esta escala espera ante la posición del rival?**

### 10.1. Preparación de partidos y posiciones

Retira un partido, el más antiguo, de cada equipo antes de evaluar. Cuando hay fechas ordena para identificarlo; sin ellas utiliza la posición esperada del último elemento de una lista de reciente a antiguo. El registro retirado queda identificado.

El máximo de posiciones `max_rank` se obtiene, por orden, del número de equipos de la competición, cantidades de filas de clasificación, mayores posiciones disponibles y, como último valor, 20.

La posición del rival se busca primero en el campo explícito del partido, después en su clasificación incorporada, campos históricos alternativos o la clasificación actual por nombre. Debe estar entre 1 y `max_rank`. La posición propia se conserva como contexto, pero **no interviene en la expectativa que se explica a continuación**.

### 10.2. Expectativa y resultado observado

**opponent_strength = (max_rank − posición rival) ÷ (max_rank − 1).**

**expected_result = limitar(1 − 2 × opponent_strength).**

Así, contra el primero se espera −1 y contra el último +1. Es una escala de referencia determinada por el puesto, no una probabilidad de derrota o victoria.

`actual_result` es +1 con margen positivo, 0 con margen cero y −1 con margen negativo.

**roe = limitar((actual_result − expected_result) ÷ 2).**

`roe` compara resultado real con esperado. La expectativa de margen es:

**expected_gd = 1.5 × expected_result.**

**gdoe = limitar((margen real − expected_gd) ÷ 5).**

Los valores 1.5 y 5 son constantes del motor. Finalmente:

**game_context_score = limitar(0.60 × roe + 0.40 × gdoe).**

Ejemplo en liga de 20: ganar por uno al primero da fuerza rival 1, expectativa −1, resultado real +1 y `roe` = 1. El margen esperado es −1.5; `gdoe` = (1 − (−1.5))/5 = 0.50. La puntuación del partido es 0.80.

Ganar por uno al último da expectativa +1: `roe` = 0, margen esperado +1.5 y `gdoe` = −0.10. La puntuación queda −0.04. La misma victoria tiene distinto significado frente a la expectativa.

### 10.3. Etiquetas de partido

| Puntuación de contexto | Categoría |
|---|---|
| Desde +0.50 | `ELITE_OVERPERFORMANCE`: superación excepcional de la expectativa. |
| Desde +0.30 hasta menos de +0.50 | `STRONG_OVERPERFORMANCE`: superación fuerte. |
| Desde +0.15 hasta menos de +0.30 | `MODERATE_OVERPERFORMANCE`. |
| Desde +0.05 hasta menos de +0.15 | `LIGHT_OVERPERFORMANCE`. |
| Mayor que −0.05 y menor que +0.05 | `NEUTRAL_EXPECTATION`. |
| Mayor que −0.15 hasta −0.05 inclusive | `LIGHT_UNDERPERFORMANCE`. |
| Mayor que −0.30 hasta −0.15 inclusive | `MODERATE_UNDERPERFORMANCE`. |
| Mayor que −0.50 hasta −0.30 inclusive | `STRONG_UNDERPERFORMANCE`. |
| −0.50 o menos | `SEVERE_CONTEXTUAL_COLLAPSE`. |

Las denominaciones exactas se conservan para reconocerlas en los resultados. Describen la comparación matemática con la expectativa del módulo.

### 10.4. Promedios y contraste entre participantes

Para cada equipo calcula la media de puntuaciones de contexto, medias de `roe` y `gdoe`, mejor y peor puntuación, dispersión y proporciones positivas, negativas y neutrales. En esas proporciones una tolerancia de una milmillonésima alrededor de cero distingue neutralidad numérica; no es el umbral 0.05 de la tabla de categorías.

**M7 = limitar(media de contexto local − media de contexto visitante).**

Con menos de cinco partidos válidos en algún equipo, queda INSUFFICIENT_DATA y asigna valor operativo cero. Entre cinco y nueve en alguno, DEGRADED. Con diez o más en ambos, ACTIVE. Esas cantidades se evalúan después de retirar el partido y validar los registros.

M7 clasifica su magnitud con una tabla propia:

| Magnitud | Intensidad |
|---|---|
| Menor que 0.05 | `VERY_LOW` |
| Desde 0.05 hasta menos de 0.15 | `LOW` |
| Desde 0.15 hasta menos de 0.30 | `MEDIUM` |
| Desde 0.30 hasta menos de 0.50 | `HIGH` |
| Desde 0.50 | `VERY_HIGH` |

El reporte conserva el partido retirado, posiciones y sus fuentes, marcadores, margen real, expectativa de resultado y margen, `roe`, `gdoe`, puntuación y categoría de cada partido, agregados por equipo y contraste final.

## 11. Cómo se combinan los siete módulos de lado

La combinación utiliza exactamente M1–M7. Su versión es `side_engine_multilayer_v3_0`. Hay un núcleo deportivo, un ajuste inmediato y una comparación de presión competitiva.

### 11.1. Valor original y efectivo

`raw_value` conserva el valor que devuelve cada módulo. `effective_value` es el que la combinación admite: si el módulo está INSUFFICIENT_DATA, INACTIVE o en un estado INVALID, vale cero; en los demás estados conserva su valor.

Los pesos son fijos. **No se redistribuye el peso de un módulo sin datos entre los demás.** Un módulo DEGRADED sigue aportando su valor efectivo completo con el peso previsto.

### 11.2. Capa A: núcleo deportivo

| Módulo | Peso | Papel |
|---|---|---|
| M1 | 0.30 | Fuerza base. |
| M2 | 0.20 | Perfil ofensivo. |
| M3 | 0.15 | Enfrentamiento directo. |
| M6 | 0.20 | Evolución estructural. |
| M7 | 0.15 | Rendimiento frente a expectativa. |

**p1_core_side = 0.30 × M1 + 0.20 × M2 + 0.15 × M3 + 0.20 × M6 + 0.15 × M7**, usando valores efectivos.

Cada `contribution` conserva módulo, estado, motivo, valor original, valor efectivo, peso y aportación. `p1_core_side_bias` y `p1_core_side_strength` describen ese núcleo antes del ajuste siguiente.

### 11.3. M4: ajuste pequeño del núcleo

**m4_context_adj = limitar M4 entre −0.06 y +0.06.**

**p1_effective_core = p1_core_side + m4_context_adj.**

No se aplica otro límite a esa suma. `core_strength` usa la tabla general de intensidad. `core_bias` es HOME o AWAY según el signo, salvo que la intensidad sea IGNORE: entonces se asigna NONE. NONE indica que no hay lado operativo del núcleo.

M4 no puede trasladar toda su magnitud al núcleo. Si su valor es 0.80, el ajuste es 0.06. Si vale −0.03, el ajuste es −0.03.

### 11.4. M5: presión y escala especial

`m5_edge` es el valor efectivo de M5. `m5_pressure_side` conserva su lado de presión o lo obtiene del signo: HOME, AWAY o NONE.

La magnitud de presión usada **por la combinación** se clasifica así:

| Magnitud de M5 | `m5_strength` |
|---|---|
| Menor que 0.15 | `LOW` |
| Desde 0.15 hasta menos de 0.30 | `MODERATE` |
| Desde 0.30 hasta menos de 0.45 | `HIGH` |
| Desde 0.45 hasta menos de 0.60 | `SEVERE` |
| Desde 0.60 | `OVERRIDE` |

Esta tabla no coincide con la intensidad general que aparece en la ficha individual de M5.

`pressure_relation` expresa la relación:

| Condición | Etiqueta |
|---|---|
| Núcleo IGNORE | `CONTEXT_ONLY`: solo el contexto puede establecer una ventana. |
| Núcleo operativo sin lado | `INTERNAL_INCONSISTENCY`: datos de decisión incoherentes. |
| Sin lado de presión | `NO_PRESSURE`. |
| Presión y núcleo apuntan al mismo lado | `PRESSURE_SUPPORTS_CORE`. |
| Apuntan a lados opuestos | `PRESSURE_CHALLENGES_CORE`. |

### 11.5. Reglas del estado y lado finales

La decisión utiliza **categoría y lado del núcleo, categoría y lado de presión y relación entre ambos**. No utiliza únicamente el signo de una suma.

Si el núcleo es IGNORE:

| Presión | Estado final | Lado final |
|---|---|---|
| NONE, o intensidad LOW/MODERATE | `NO_BET` | NONE |
| HIGH con lado | `CONTEXT_WINDOW` | Lado de presión |
| SEVERE/OVERRIDE con lado | `STRONG_CONTEXT_WINDOW` | Lado de presión |

Si el núcleo es operativo y no hay presión, el estado es CONFIRMED y conserva el lado del núcleo.

Si presión y núcleo coinciden, conserva el lado del núcleo: queda DEGRADED cuando ambos tienen intensidad LOW; en las demás combinaciones queda CONFIRMED.

Si se oponen, aplica toda esta tabla:

| Intensidad del núcleo | LOW de M5 | MODERATE de M5 | HIGH de M5 | SEVERE de M5 | OVERRIDE de M5 |
|---|---|---|---|---|---|
| EXTREME | DEGRADED | DEGRADED | DEGRADED | CONFLICT | STRONG_CONFLICT |
| HIGH o MEDIUM | DEGRADED | DEGRADED | CONFLICT | STRONG_CONFLICT | STRONG_CONFLICT |
| LOW | DEGRADED | CONFLICT | UNDERDOG_WINDOW | STRONG_UNDERDOG_WINDOW | STRONG_UNDERDOG_WINDOW |

DEGRADED, CONFLICT y STRONG_CONFLICT conservan el lado del núcleo. UNDERDOG_WINDOW y STRONG_UNDERDOG_WINDOW adoptan el lado de presión. «Underdog» aquí nombra el lado que desafía un núcleo débil; esta regla no consulta quién tiene la cuota más alta.

Si aparece un núcleo operativo sin lado, registra la anomalía `CORE_BIAS_NONE_WITH_OPERATIVE_CORE` y devuelve NO_BET con NONE.

Las etiquetas significan:

- **CONFIRMED:** el perfil pasa estas reglas conservando su lado.
- **DEGRADED:** conserva el lado con una lectura debilitada por la relación entre componentes.
- **CONFLICT / STRONG_CONFLICT:** conserva el lado del núcleo y marca oposición relevante.
- **CONTEXT_WINDOW / STRONG_CONTEXT_WINDOW:** el contexto establece el lado cuando el núcleo no es operativo.
- **UNDERDOG_WINDOW / STRONG_UNDERDOG_WINDOW:** la presión establece el lado contrario a un núcleo de intensidad baja.
- **NO_BET:** estas reglas no producen un lado final operativo.

Son clasificaciones internas del perfil. El cálculo no envía una orden de apuesta.

### 11.6. Balance numérico y ejemplo completo

También conserva:

**p1_final_context_balance = p1_effective_core + m5_edge.**

Ese número se devuelve como `value` del pilar de lado. **Es evidencia numérica, no la regla que decide el lado final.** No se limita otra vez a ±1, por lo que puede superar ese intervalo.

Ejemplo con valores efectivos M1 = 0.20, M2 = 0.10, M3 = 0.30, M6 = 0.20 y M7 = 0.10:

1. Núcleo = 0.060 + 0.020 + 0.045 + 0.040 + 0.015 = 0.180.
2. M4 = 0.20 se convierte en ajuste +0.06.
3. Núcleo efectivo = 0.240: HOME, MEDIUM.
4. M5 = −0.35: presión AWAY, HIGH.
5. La tabla MEDIUM frente a presión HIGH opuesta produce **CONFLICT, HOME**.
6. El balance es 0.240 − 0.350 = −0.110.

El balance negativo no cambia por sí solo el lado final a AWAY. El lector debe consultar `p1_final_bias` y `p1_final_state`.

### 11.7. Dónde se conserva cada explicación

El resultado de lado reúne `pillar_id`, `pillar_name`, `event_id`, `participants`, `modules`, `value` y `raw`. Dentro de `raw`:

| Grupo | Qué explica |
|---|---|
| `layer_a` | Pesos, contribuciones y núcleo antes y después de M4. |
| `layer_b.m4` | Valor original y efectivo de M4, límite y ajuste. |
| `layer_b.m5` | Valor de M5, lado de presión e intensidad especial. |
| `final` | Balance, relación de presión, estado, lado, entradas de la decisión y anomalías. |
| `module_statuses` | Estado y motivo de M1–M7. |
| `active_modules`, `skipped_modules` | Módulos utilizables y módulos sin aportación efectiva, con sus valores y motivos. |
| `m1_raw` a `m7_raw` | Detalle de cada módulo, también disponible en su ficha individual. |
| `value_is_evidence_only` | Marca que el balance numérico es evidencia. |
| `p1_final_context_balance_is_decision` | Se conserva como falso: el balance no decide el lado. |

Varios campos también se repiten al nivel superior de `raw` para facilitar consulta. No son nuevos cálculos. No existe una única intensidad final que deba deducirse del balance: las categorías relevantes son `core_strength`, `m5_strength` y el estado final.

## 12. P1 Totales: entradas y ventanas

P1 Totales estudia **si el perfil deportivo se sitúa por encima o por debajo de referencias de anotación de su liga**. No compara directamente contra una línea de apuesta ni consume los precios de P3.

Su versión es `p1_totals_directional_vol_separated_v2_5_composite_trend_dominance` y el código de módulo es `P1_TOTALS`. Produce un objeto `P1TotalsOutput`.

### 12.1. Muestra equilibrada

De cada historial extrae partidos con `team_score` y `opponent_score` numéricos válidos. Llama `gf` a la anotación a favor y `ga` a la recibida. Para el total de un encuentro toma `game_total` si existe; como alternativa, suma ambos marcadores.

Ordena de reciente a antiguo cuando dispone de fechas. Define:

**N_AVAIL = mínimo de la cantidad válida local y visitante.**

Con 18 partidos válidos del local y 12 del visitante, conserva los 12 más recientes de cada lado. Ese equilibrio afecta las series de estructura, temporalidad, tendencia y variabilidad del encuentro.

Necesita una duración positiva de temporada regular, `TOTAL_SEASON_GAMES`, obtenida de `event_context.competition.total_regular_season_games`.

### 12.2. Las cuatro ventanas

| Ventana | Proporción de temporada | Peso base temporal | Lectura |
|---|---|---|---|
| `SHORT` | 0.15 | 0.30 | Tramo corto. |
| `RECENT` | 0.35 | 0.35 | Tramo reciente más amplio. |
| `MID` | 0.60 | 0.20 | Tramo intermedio. |
| `FULL` | 1.00 | 0.15 | Horizonte de temporada. |

**tamaño objetivo = redondear hacia arriba los empates de mitad en (partidos de temporada × proporción).**

Este redondeo es `ROUND_HALF_UP`: 6.5 pasa a 7. Es distinto del redondeo al entero par usado en minutos y bloques de M6.

**partidos usados = mínimo entre N_AVAIL y tamaño objetivo.**

**completitud = partidos usados ÷ tamaño objetivo**, limitada entre 0 y 1.

**peso efectivo = peso base × completitud.**

Con temporada de 38 y diez datos por lado:

| Ventana | Objetivo | Usados | Completitud | Peso efectivo aproximado |
|---|---|---|---|---|
| SHORT | 6 | 6 | 1.0000 | 0.3000 |
| RECENT | 13 | 10 | 0.7692 | 0.2692 |
| MID | 23 | 10 | 0.4348 | 0.0870 |
| FULL | 38 | 10 | 0.2632 | 0.0395 |

Todas comienzan en el partido más reciente; se superponen. Una ventana puede existir aunque todavía no alcance su tamaño objetivo. Su peso refleja la completitud.

La variable pública `WINDOWS_USED` guarda **los tamaños objetivo**, pese a su nombre. Las cantidades realmente utilizadas están en `raw`, dentro de la información de ventanas. `WINDOW_COMPLETENESS_BY_WINDOW` guarda las proporciones de completitud.

Si algún tamaño objetivo queda en cero, o una ventana no puede usar ningún partido, no se entrega un perfil válido de totales.

### 12.3. Referencias y percentiles de liga

El motor utiliza `league_totals_context`, preparado con resultados de liga. Sus listas de partidos permiten obtener totales; sus historias por equipo permiten obtener tendencia y dispersión.

Para sus percentiles continuos ordena los valores y calcula la posición **(cantidad − 1) × proporción**, contando desde cero. Si la posición está entre dos elementos, interpola. Con valores 0, 2, 4 y 8, el P75 está en posición 2.25 y vale 4 + 0.25 × (8 − 4) = 5.

Para crear una escala suele medir **distancias absolutas respecto de la mediana** y tomar su P75. La referencia responde «cuál es el centro»; la escala, «qué distancia es relevante en esta distribución». La escala no es un precio ni un umbral fijo de goles.

## 13. Capa estructural: capacidad de anotar y recibir

Calcula, sobre la muestra equilibrada:

- `GF_PG_HOME`, `GF_PG_AWAY`: anotación a favor por partido.
- `GA_PG_HOME`, `GA_PG_AWAY`: anotación recibida por partido.

Cruza ataque de un equipo con defensa del otro:

**entorno ofensivo local = (anotación a favor local por partido + anotación recibida visitante por partido) ÷ 2.**

**entorno ofensivo visitante = (anotación a favor visitante por partido + anotación recibida local por partido) ÷ 2.**

**EXPECTED_TOTAL_STRUCTURAL = entorno local + entorno visitante.**

Es una expectativa estructural basada en promedios; no afirma un marcador exacto.

Ejemplo: el local anota 1.6 y recibe 1.0 por partido; el visitante anota 1.2 y recibe 1.4. El entorno local es (1.6 + 1.4)/2 = 1.5; el visitante (1.2 + 1.0)/2 = 1.1. El total estructural es 2.6.

De los totales de partidos de liga obtiene:

**LEAGUE_MATCH_TOTAL_BASELINE = mediana de los totales de liga.**

**TOTAL_DYNAMIC_SCALE = P75 de las distancias absolutas entre cada total de liga y esa mediana.**

La señal estructural, llamada aquí **S**, es:

**STRUCTURAL_PROFILE_SCORE = limitar((EXPECTED_TOTAL_STRUCTURAL − LEAGUE_MATCH_TOTAL_BASELINE) ÷ TOTAL_DYNAMIC_SCALE).**

Con total esperado 2.6, referencia 2.4 y escala 0.8, S = 0.25. Un S positivo apunta hacia OVER_PROFILE relativo a esa referencia; uno negativo hacia UNDER_PROFILE.

`P1_TOTALS_STRUCTURAL_SCORE` y `STRUCTURAL_FINAL` conservan esa misma señal. También obtiene:

**STRUCTURAL_ANCHOR = 0.5 + 0.5 × magnitud de S**, dentro de 0.5–1.

El ancla describe la magnitud estructural. En esta versión se conserva como información; **no multiplica las otras capas ni la señal direccional**.

## 14. Capa temporal: nivel anotador de las ventanas

Para cada equipo y ventana calcula:

**TEAM_TOTAL_WINDOW = media de anotación a favor + media de anotación recibida**, usando los partidos de esa ventana.

Después obtiene el total temporal del equipo:

**TEAM_TOTAL_WEIGHTED = suma de (total de ventana × peso efectivo) ÷ suma de pesos efectivos utilizados.**

Los pesos efectivos ya incluyen la completitud de la sección 12. Si faltan partidos para una ventana extensa, su importancia disminuye. El divisor normaliza la combinación para conservar la unidad de anotación.

**MATCHUP_TEMPORAL_TOTAL = (total temporal local + total temporal visitante) ÷ 2.**

La señal temporal, llamada **T**, es:

**TEMPORAL_PROFILE_SCORE = limitar((MATCHUP_TEMPORAL_TOTAL − referencia de total de liga) ÷ escala de total de liga).**

Usa la misma referencia y escala que la capa estructural. `TEMPORAL_FINAL` conserva T.

S y T responden preguntas relacionadas pero distintas. S cruza promedios completos de ataque y defensa; T da mayor o menor importancia a tramos recientes. La diferencia entre ambas permite detectar que la historia más reciente cuenta algo distinto de la muestra completa.

## 15. Capa de tendencia: cambio reciente frente al largo plazo

Para cada equipo toma las medias de total de las cuatro ventanas y crea dos perfiles:

**perfil corto = 0.60 × SHORT + 0.40 × RECENT.**

**perfil largo = 0.60 × MID + 0.40 × FULL.**

**delta del equipo = perfil corto − perfil largo.**

**MATCHUP_TREND_DELTA = (delta local + delta visitante) ÷ 2.**

Un delta positivo indica que los tramos recientes tienen mayor anotación que los amplios. No requiere que su nivel absoluto ya sea alto.

Calcula deltas equivalentes para las historias disponibles de equipos de liga, con los mismos tamaños objetivo. Cada equipo de referencia usa la cantidad de datos que tenga hasta esos tamaños.

**TREND_BASELINE = mediana de deltas de equipos de liga.**

**TREND_DYNAMIC_SCALE = P75 de las distancias absolutas de esos deltas a su mediana.**

La señal de tendencia, llamada **R**, es:

**TREND_SIGNAL = limitar((MATCHUP_TREND_DELTA − TREND_BASELINE) ÷ TREND_DYNAMIC_SCALE).**

`TREND_FINAL` conserva R. Una subida de 0.20 no tiene el mismo significado si casi todos los equipos están subiendo 0.20; la comparación con la liga ajusta esa interpretación.

Tendencia y temporalidad no son intercambiables. T describe un nivel ponderado frente a la liga; R describe un cambio de corto contra largo plazo frente a los cambios de liga.

## 16. Variabilidad: una lectura separada de la dirección

Calcula la desviación estándar de los totales de partido del local y del visitante sobre la muestra equilibrada:

**MATCHUP_VOLATILITY = (desviación local + desviación visitante) ÷ 2.**

Para cada equipo de liga calcula su desviación de totales disponibles. La serie puede proceder de `game_totals` o de sus resultados.

**VOL_BASELINE = mediana de las desviaciones de equipos de liga.**

**VOL_DYNAMIC_SCALE = P75 de sus distancias absolutas respecto a esa mediana.**

**VOL_EDGE = limitar((MATCHUP_VOLATILITY − VOL_BASELINE) ÷ VOL_DYNAMIC_SCALE).**

Un VOL_EDGE positivo indica mayor dispersión que la referencia; uno negativo, menor.

Para categorizar, calcula contrastes de variabilidad de los equipos de referencia, toma sus magnitudes y obtiene P50, P75 y P90. Compara la **magnitud de VOL_EDGE** con esos umbrales:

| Magnitud | `P1_TOTALS_VARIANCE_STATE` |
|---|---|
| Menor que VOL_EDGE_P50 | `NORMAL_VARIANCE` |
| Desde P50 hasta menos de P75 | `ELEVATED_VARIANCE` |
| Desde P75 hasta menos de P90 | `HIGH_VARIANCE` |
| Desde P90 | `EXTREME_VARIANCE` |
| No se puede establecer la referencia necesaria | `UNKNOWN_VARIANCE` |

Al usar magnitud, un contraste muy negativo también puede entrar en una categoría extrema: representa una diferencia grande respecto de la referencia, aunque sea por menor dispersión. El signo de VOL_EDGE permite distinguirlo.

La variabilidad **no vota por OVER o UNDER, no suma una cuarta capa direccional y no reduce por sí misma sus pesos**. Sus campos pueden quedar ausentes y el perfil direccional seguir calculándose. Algunas etiquetas internas sí combinan variabilidad con conflictos entre capas para describir el perfil.

## 17. Dirección, intensidad y coincidencia entre capas

### 17.1. Capas activas y pesos

| Capa | Señal | Peso direccional |
|---|---|---|
| `STRUCTURAL` | S | 0.45 |
| `TEMPORAL` | T | 0.30 |
| `TREND` | R | 0.25 |

Una capa es ACTIVE si su magnitud es **al menos 0.05**. Si es menor, queda IGNORE y su contribución ponderada es cero. Su señal original se conserva.

Cada `P1TotalsLayerOutput` contiene `layer` —nombre—, `status` —ACTIVE/IGNORE—, `raw_signal`, `final_signal`, `weight`, `weighted_signal`, `ignored_reason` y `raw`. `ignored_reason` explica que la magnitud quedó por debajo de 0.05 cuando corresponde.

**ACTIVE_WEIGHT_SUM = suma de pesos de las capas activas.**

**P1_TOTALS_DIRECTIONAL_SCORE = limitar(suma de señales finales × pesos de las capas activas ÷ ACTIVE_WEIGHT_SUM).**

Aquí sí se normaliza por los pesos presentes. Por ejemplo, si solo S = 0.20 está activa, el resultado es (0.45 × 0.20)/0.45 = 0.20. Si ninguna está activa, no puede formar un perfil y no devuelve un objeto válido.

### 17.2. Dirección e intensidad

Positivo produce OVER_PROFILE; negativo, UNDER_PROFILE; cero, NEUTRAL_PROFILE. Son perfiles respecto de las referencias deportivas. No especifican una línea de mercado.

La intensidad se obtiene de la magnitud:

| Magnitud | `P1_TOTALS_STRENGTH` |
|---|---|
| Menor que 0.05 | `NONE` |
| Desde 0.05 hasta menos de 0.15 | `WEAK` |
| Desde 0.15 hasta menos de 0.30 | `MODERATE` |
| Desde 0.30 hasta 0.60 inclusive | `STRONG` |
| Mayor que 0.60 | `VERY_STRONG` |

Aunque las capas activas tengan magnitud mínima 0.05, pueden compensarse y producir dirección neutral o intensidad NONE. Esa compensación no es falta de datos.

### 17.3. Alineación y estados descriptivos

**ALIGNMENT_SCORE = (S + T + R) ÷ (magnitud de S + magnitud de T + magnitud de R).**

Si el denominador es cero, vale cero. Usa las señales finales sin pesos y conserva incluso valores de capas IGNORE. +1 indica signos positivos compartidos; −1, signos negativos; cercanía a cero, compensación.

`OVER_COUNT` y `UNDER_COUNT` cuentan capas activas positivas y negativas. `IGNORE_COUNT` cuenta las ignoradas; `ACTIVE_LAYER_COUNT`, las activas. `P1_TOTALS_INTERNAL_STATE` conserva esos conteos y estas condiciones:

| Condición | Significado |
|---|---|
| `CONSENSUS_OVER` | Al menos dos capas activas y todas las activas positivas. |
| `CONSENSUS_UNDER` | Al menos dos activas y todas negativas. |
| `HEATING_CONFLICT` | Estructura y tendencia activas, con S negativa y R positiva. |
| `COOLING_CONFLICT` | Estructura y tendencia activas, con S positiva y R negativa. |
| `CHAOTIC_CONFLICT` | Variabilidad HIGH/EXTREME y estructura y temporalidad activas con signos opuestos. |
| `STRUCTURAL_NEUTRAL_SECONDARY_PUSH` | Estructura IGNORE; temporalidad y tendencia activas coinciden en un signo no neutral. |

Estas condiciones ayudan a describir por qué existe consenso o tensión. Pueden coexistir. No cambian retroactivamente los valores de S, T y R.

## 18. Lectura compuesta: dirección y cambio del perfil

La versión actual conserva otra puntuación, `P1_TOTALS_COMPOSITE`, que combina el resultado direccional con señales de ruptura y dominio de tendencia. No reemplaza el campo direccional anterior.

### 18.1. Base, ruptura y presión de cambio

**BASE_SIGNAL = S + T.**

No utiliza pesos para esta base ni la limita a ±1. Puede ir de −2 a +2.

`BREAKOUT_CONDITION` es verdadera cuando el signo de R se opone al de la base y ambas magnitudes son al menos 0.05.

Cuando se cumple:

**BREAKOUT_SCORE = signo de R × magnitud de R ÷ (magnitud de R + magnitud de BASE_SIGNAL).**

Cuando no se cumple, vale cero. La ruptura describe cuánto pesa la tendencia que desafía la base.

**HEATING_PRESSURE = R − BASE_SIGNAL**, si la base es negativa y R positiva; en otro caso, cero.

**COOLING_PRESSURE = R − BASE_SIGNAL**, si la base es positiva y R negativa; en otro caso, cero. La presión de enfriamiento conserva signo negativo.

**TREND_DOMINANCE = limitar(R − BASE_SIGNAL).**

Las presiones pueden superar magnitud 1; TREND_DOMINANCE queda entre −1 y +1. En estas operaciones se conservan las señales finales de las tres capas, incluso cuando alguna quedó IGNORE en la suma direccional.

### 18.2. Capa dominante y fórmula compuesta

`P1_TOTALS_DRIVER` es la capa activa con mayor magnitud de contribución ponderada. `P1_TOTALS_DRIVER_SIGNAL` conserva su señal sin peso y `P1_TOTALS_DRIVER_WEIGHTED`, su contribución. En igualdad se conserva la primera del orden STRUCTURAL, TEMPORAL, TREND.

**P1_TOTALS_COMPOSITE = limitar(0.55 × P1_TOTALS_DIRECTIONAL_SCORE + 0.30 × BREAKOUT_SCORE + 0.15 × TREND_DOMINANCE).**

La dirección e intensidad del compuesto usan las mismas reglas de signo y magnitud de la sección 17. ALIGNMENT_SCORE se conserva como explicación, pero **no es un término de esta fórmula**.

### 18.3. Ejemplo de dos lecturas diferentes

Supongamos S = −0.40, T = −0.20 y R = +0.60. Las tres capas están activas.

1. Aportaciones direccionales: −0.180, −0.060 y +0.150.
2. Suma de pesos = 1; dirección = −0.090: UNDER_PROFILE, WEAK.
3. Base = −0.60.
4. La tendencia se opone a la base; ruptura = 0.60/(0.60 + 0.60) = +0.50.
5. Presión de calentamiento = 0.60 − (−0.60) = 1.20; enfriamiento = 0.
6. Dominio de tendencia = limitar(1.20) = 1.
7. Compuesto = 0.55 × (−0.09) + 0.30 × 0.50 + 0.15 × 1 = **0.2505: OVER_PROFILE, MODERATE**.
8. Alineación = (−0.40 − 0.20 + 0.60)/1.20 = 0.
9. Driver = STRUCTURAL: su aportación tiene magnitud 0.18, mayor que 0.15 de TREND.

El perfil direccional conserva una inclinación baja hacia menor anotación; el compuesto refleja la tendencia que desafía esa base. Las dos lecturas responden a fórmulas diferentes y deben mostrarse con sus nombres.

### 18.4. Cuándo hay un resultado de totales

Un resultado completo tiene `status = OK`. Si faltan historial, duración de temporada, datos comparables, referencias de liga o escalas direccionales positivas, el motor puede devolver ausencia de resultado, `None`. Lo mismo ocurre si todas las capas quedan IGNORE.

Una escala cero significa que esa distribución no aporta una distancia positiva con la que normalizar; el motor no inventa otra escala. La falta de referencias de variabilidad se representa por campos opcionales y UNKNOWN_VARIANCE, y no necesariamente impide la parte direccional.

La ausencia de resultado de totales no significa UNDER, OVER ni una señal deportiva neutral. Significa que no pudo construir el perfil requerido.

## 19. Diccionario del resultado de totales

Esta tabla cubre todos los campos de `P1TotalsOutput`. Las letras S, T y R remiten a las capas explicadas arriba.

| Campo | Qué conserva |
|---|---|
| `pillar_id`, `module_id`, `module_name`, `engine_version` | Identidad del pilar, módulo y versión de las reglas. |
| `event_id`, `participants` | Encuentro y nombres de sus participantes. |
| `status`, `status_reason` | Estado del objeto y explicación, cuando existe. Un resultado completo usa OK. |
| `P1_TOTALS_DIRECTIONAL_SCORE` | Media ponderada normalizada de capas activas. |
| `P1_TOTALS_DIRECTION`, `P1_TOTALS_STRENGTH` | Dirección e intensidad de esa media. |
| `P1_TOTALS_VARIANCE_STATE` | Categoría separada de variabilidad relativa. |
| `P1_TOTALS_INTERNAL_STATE` | Conteos, condiciones descriptivas y cálculos que permiten interpretar el perfil. |
| `P1_TOTALS_STRUCTURAL_SCORE`, `STRUCTURAL_PROFILE_SCORE`, `STRUCTURAL_FINAL` | S; nombres que conservan la misma señal estructural. |
| `STRUCTURAL_ANCHOR` | Descriptor entre 0.5 y 1 de la magnitud estructural. |
| `VOL_EDGE` | Contraste firmado de dispersión del encuentro frente a la liga; puede faltar. |
| `TEMPORAL_PROFILE_SCORE`, `TEMPORAL_FINAL` | T; señal temporal frente a la referencia de anotación. |
| `MATCHUP_TREND_DELTA` | Media del cambio corto contra largo plazo de los dos participantes. |
| `TREND_BASELINE` | Mediana de cambios de equipos de liga. |
| `TREND_DYNAMIC_SCALE` | P75 de distancias de esos cambios a la mediana. |
| `TREND_SIGNAL`, `TREND_FINAL` | R; cambio del encuentro normalizado frente a la liga. |
| `EXPECTED_TOTAL_STRUCTURAL` | Suma de entornos ofensivos obtenidos al cruzar ataque y defensa. |
| `MATCHUP_VOLATILITY` | Media de desviaciones de totales de los dos equipos; puede faltar. |
| `MATCHUP_TEMPORAL_TOTAL` | Media de los totales temporales ponderados de ambos. |
| `LEAGUE_MATCH_TOTAL_BASELINE` | Mediana de totales de partidos de liga. |
| `TOTAL_DYNAMIC_SCALE` | P75 de distancias de totales de liga a esa mediana. |
| `VOL_BASELINE`, `VOL_DYNAMIC_SCALE` | Centro y escala de dispersión de equipos de liga; opcionales. |
| `VOL_EDGE_P50`, `VOL_EDGE_P75`, `VOL_EDGE_P90` | Umbrales de magnitud de contraste de variabilidad; opcionales. |
| `ACTIVE_WEIGHT_SUM` | Suma de pesos de las capas direccionales activas. |
| `STRUCTURAL_WEIGHT`, `TEMPORAL_WEIGHT`, `TREND_WEIGHT` | Pesos base 0.45, 0.30 y 0.25. |
| `STRUCTURAL_WEIGHTED`, `TEMPORAL_WEIGHTED`, `TREND_WEIGHTED` | Aportaciones ponderadas; cero si la capa queda IGNORE. |
| `BASE_SIGNAL` | S + T sin ponderar. |
| `BREAKOUT_CONDITION` | Si existe oposición con magnitudes suficientes entre base y tendencia. |
| `BREAKOUT_SCORE` | Participación relativa y firmada de la tendencia que rompe con la base. |
| `HEATING_PRESSURE`, `COOLING_PRESSURE` | Distancia R − base en los casos de calentamiento o enfriamiento. |
| `TREND_DOMINANCE` | Distancia R − base limitada a ±1. |
| `P1_TOTALS_DRIVER` | Nombre de la capa activa con mayor aportación en magnitud. |
| `P1_TOTALS_DRIVER_SIGNAL`, `P1_TOTALS_DRIVER_WEIGHTED` | Señal y aportación de esa capa. |
| `P1_TOTALS_COMPOSITE` | Combinación 0.55 de dirección, 0.30 de ruptura y 0.15 de dominio de tendencia. |
| `P1_TOTALS_COMPOSITE_DIRECTION`, `P1_TOTALS_COMPOSITE_STRENGTH` | Dirección e intensidad del compuesto. |
| `ALIGNMENT_SCORE` | Coincidencia o compensación de S, T y R sin pesos. |
| `OVER_COUNT`, `UNDER_COUNT`, `IGNORE_COUNT`, `ACTIVE_LAYER_COUNT` | Cantidades de capas activas positivas, activas negativas, ignoradas y activas totales. |
| `STRUCTURAL_STATUS`, `TEMPORAL_STATUS`, `TREND_STATUS` | ACTIVE o IGNORE de cada capa. |
| `WINDOWS_USED` | Tamaños objetivo de SHORT, RECENT, MID y FULL. |
| `WINDOW_COMPLETENESS_BY_WINDOW` | Proporción de datos disponibles respecto de cada tamaño objetivo. |
| `active_layers`, `ignored_layers` | Fichas completas de capas activas e ignoradas. |
| `raw` | Muestras, ventanas, promedios, referencias, capas, política de umbral y cálculos de ruptura y compuesto. |

Para reconstruir un resultado, siga `raw` en este orden: datos válidos y cantidad equilibrada; tamaños y completitud de ventanas; promedios por equipo; referencias y escalas de liga; señales S/T/R; pesos activos; dirección; variabilidad; base, ruptura, driver y compuesto.

## Recorrido integrado: del dato al resultado

Esta sección conecta los conceptos de todo el documento. El orden general es **expediente deportivo → siete módulos → resultado de lado → ventanas y capas → resultado de totales → entrega de ambas salidas**. Las dos ramas comparten historia, pero seleccionan muestras y producen significados distintos.

### Paso 1. Preparar una historia interpretable

`EventContext` identifica local, visitante, competición, temporada e inicio. `streak_analysis` contiene el `MatchupStreakContext`: partidos de cada equipo, enfrentamientos directos, clasificación y referencias de liga.

Antes de calcular, los marcadores se atribuyen al participante correcto. Un equipo que hoy es local conserva su anotación de un partido en el que fue visitante. Las fechas sitúan la historia; la clasificación aporta puestos, puntos, partidos y registros de temporada. `number_of_teams` ayuda a interpretar posiciones; `total_regular_season_games` permite construir las ventanas de totales.

El expediente llega preparado por el recorrido anterior a P1, explicado en la [sección 2](#2-el-expediente-deportivo-de-entrada). P1 no convierte el historial de cuotas de los otros pilares en resultados deportivos.

### Paso 2. Transformar esa historia en los siete módulos

El motor llama a M1–M7 en ese orden. Cada uno toma la parte de la historia que necesita y devuelve un `ModuleResult`. La siguiente tabla muestra qué conceptos se utilizan y qué resultado pasa al siguiente paso.

| Módulo | Transformación de las entradas | Salida utilizada |
|---|---|---|
| [M1: fuerza base](#4-m1-fuerza-base) | Obtiene registros de temporada y márgenes. Compara tasas de victoria, diferencia por partido normalizada por el P75 de liga, dispersión en ventanas acumulativas y potencia de márgenes positivos frente a negativos. Combina sus cuatro componentes con pesos 0.35, 0.35, 0.10 y 0.20 y limita el contraste. | Valor de fuerza base, dirección, intensidad, muestra y procedencia de la escala. |
| [M2: perfil ofensivo](#5-m2-perfil-ofensivo) | Extrae anotación a favor válida. Compara dispersión, media de las cinco mayores anotaciones, tasa de ceros y tasa de anotaciones de al menos dos. Combina con pesos 0.35, 0.30, 0.20 y 0.15 y limita la suma final. | Contraste ofensivo, con los promedios, frecuencias y aportaciones que lo explican. |
| [M3: enfrentamiento directo](#6-m3-enfrentamiento-directo) | Utiliza partidos entre los participantes, ya orientados al encuentro actual. Compara resultados, con media unidad por empate, y anotación; después aplica el ajuste por cantidad de enfrentamientos. | Contraste de enfrentamiento y explicación del tamaño de muestra. |
| [M4: estado inmediato](#7-m4-estado-inmediato-ajustado-por-rival) | Selecciona hasta cinco partidos válidos con posición del rival. Interpreta fuerza y debilidad de ese rival dentro de la liga y compara el estado reciente de los equipos. | Contraste inmediato que después se acota para ajustar el núcleo. |
| [M5: coste competitivo](#8-m5-coste-competitivo-del-contexto) | Parte de puesto, puntos y partidos restantes. Determina objetivo y régimen, encuentra puntos de corte, mide distancia, urgencia, severidad y multiplicador y obtiene un coste por equipo. Compara los costes mediante diferencia relativa. | Lado y magnitud de presión, con horizonte, objetivo y coste por participante. |
| [M6: evolución estructural](#9-m6-evolución-estructural) | Ordena márgenes de antiguo a reciente y los divide en cinco bloques. Compara pendiente de sus medias y estabilidad de esos bloques con pesos 0.65 y 0.35. | Contraste de evolución, con vectores, pendientes y dispersiones. |
| [M7: expectativa del rival](#10-m7-rendimiento-frente-a-la-expectativa-del-rival) | Retira el partido más antiguo. Transforma la posición del rival en expectativa de resultado y margen; obtiene `roe` y `gdoe`, los combina 0.60/0.40 y compara las medias de contexto de ambos equipos. | Contraste frente a expectativa, categorías de partido, agregados y muestra válida. |

Los objetos `ModuleComponentResult` conservan edge, peso y aportación ponderada cuando corresponden. `raw` conserva las operaciones intermedias y su procedencia. Así se puede distinguir un valor realmente neutral de un módulo que no reunió su muestra.

Una media, una desviación, un percentil y una diferencia relativa intervienen en cálculos diferentes. El límite o `clamp` se aplica donde lo define cada fórmula; no se impone un descuento general a todas las señales pequeñas.

### Paso 3. Decidir qué valores de módulo entran en la combinación

El motor consulta el estado de cada módulo. Conserva `raw_value` como salida original y forma `effective_value` para la combinación.

Un módulo utilizable, incluido DEGRADED, conserva su valor. INSUFFICIENT_DATA, INACTIVE o un estado INVALID aporta cero operativo, manteniendo el motivo. Ese cero operativo no afirma igualdad deportiva. Los pesos de los otros módulos no se redistribuyen.

La [sección 11](#11-cómo-se-combinan-los-siete-módulos-de-lado) explica esta combinación. Para seguir un caso completo, retomemos estos valores efectivos ilustrativos, ya obtenidos por los módulos:

| Módulo | Valor efectivo | Papel en la combinación |
|---|---|---|
| M1 | +0.20 | Núcleo, peso 0.30. |
| M2 | +0.10 | Núcleo, peso 0.20. |
| M3 | +0.30 | Núcleo, peso 0.15. |
| M4 | +0.20 | Ajuste acotado del núcleo. |
| M5 | −0.35 | Presión competitiva separada. |
| M6 | +0.20 | Núcleo, peso 0.20. |
| M7 | +0.10 | Núcleo, peso 0.15. |

La tabla comienza en las salidas de módulo para mostrar su encadenamiento; los ejemplos y fórmulas de cada módulo explican cómo se producen desde los registros deportivos.

### Paso 4. Pasar del núcleo al resultado final de lado

Primero se suman las cinco aportaciones del núcleo:

**0.30 × 0.20 + 0.20 × 0.10 + 0.15 × 0.30 + 0.20 × 0.20 + 0.15 × 0.10 = 0.180.**

M4 pasa por el límite −0.06/+0.06. Su +0.20 se convierte en **+0.06**, y el núcleo efectivo queda en **0.240: HOME, MEDIUM**.

M5 permanece separado. Su −0.35 significa **presión AWAY, HIGH** según la escala especial de presión. `pressure_relation` queda PRESSURE_CHALLENGES_CORE.

La tabla de decisión combina núcleo MEDIUM con presión HIGH contraria. Produce **`p1_final_state = CONFLICT` y `p1_final_bias = HOME`**. Paralelamente, el balance numérico es 0.240 − 0.350 = **−0.110**, devuelto como `value`.

La lectura correcta es: **«El núcleo deportivo favorece al local con intensidad media; el contexto presiona a favor del visitante. La regla conserva al local y marca conflicto. El balance de evidencia es negativo»**. Para obtenerla se utilizaron pesos, límite de ajuste, intensidad del núcleo, escala de presión, relación y tabla final. El signo de `value` por sí solo no reconstruye esa decisión.

Si el núcleo fuera IGNORE, las mismas reglas evaluarían si hay una ventana de contexto o NO_BET. Si fuera débil y la presión contraria suficiente, podrían establecer una ventana hacia el lado de presión. Son ramas de la tabla, no nuevas sumas.

`modules` permite revisar las fichas originales. `raw.layer_a` conserva núcleo y contribuciones; `raw.layer_b`, ajuste y presión; `raw.final`, entradas de decisión, relación, estado, lado, balance y anomalías. Las listas de módulos utilizables y apartados explican la participación real.

### Paso 5. Construir las muestras y ventanas de totales

Después de lado, el coordinador calcula totales con el mismo expediente deportivo. Selecciona partidos con anotación propia y recibida válida y equilibra las cantidades: `N_AVAIL` es el mínimo entre ambos equipos.

Retomemos diez partidos válidos por lado y temporada de 38. Las proporciones SHORT, RECENT, MID y FULL producen objetivos **6, 13, 23 y 38**. Se usan **6, 10, 10 y 10**, respectivamente. Cada cantidad usada dividida por su objetivo produce completitud; completitud por peso base produce peso temporal efectivo.

Esto utiliza el redondeo de mitad hacia arriba, las ventanas superpuestas y la normalización de pesos descritos en la [sección 12](#12-p1-totales-entradas-y-ventanas). `WINDOWS_USED` conserva los objetivos; los usados y los pesos efectivos quedan en el detalle.

De la liga se obtienen medianas y escalas P75 de distancias respecto a ellas. Esas referencias permiten expresar cuánto se aparta el encuentro de su contexto, en lugar de tratar un mismo total como alto en cualquier deporte o liga.

### Paso 6. Obtener estructura, temporalidad, tendencia y variabilidad

Las cuatro lecturas utilizan las muestras anteriores de formas distintas:

| Lectura | Cómo transforma los datos | Qué entrega |
|---|---|---|
| [Estructural](#13-capa-estructural-capacidad-de-anotar-y-recibir) | Cruza anotación a favor de cada equipo con anotación recibida del rival. Suma los entornos ofensivos y normaliza el total frente a mediana y escala de liga. | `EXPECTED_TOTAL_STRUCTURAL` y señal **S**, junto a `STRUCTURAL_ANCHOR` como descriptor. |
| [Temporal](#14-capa-temporal-nivel-anotador-de-las-ventanas) | Obtiene total por ventana y equipo, combina con pesos efectivos normalizados y promedia ambos equipos. Contrasta ese nivel con la referencia de total de liga. | `MATCHUP_TEMPORAL_TOTAL` y señal **T**. |
| [Tendencia](#15-capa-de-tendencia-cambio-reciente-frente-al-largo-plazo) | Compara 0.60 SHORT + 0.40 RECENT con 0.60 MID + 0.40 FULL por equipo. Promedia los cambios y los normaliza frente a los cambios de liga. | `MATCHUP_TREND_DELTA` y señal **R**. |
| [Variabilidad](#16-variabilidad-una-lectura-separada-de-la-dirección) | Obtiene desviaciones de totales por equipo, su media y el contraste frente a la dispersión de liga. Compara su magnitud con P50/P75/P90. | `VOL_EDGE` y categoría de varianza, separados de la dirección. |

Para continuar el ejemplo de la sección 18.3, supongamos que estos cálculos entregan **S = −0.40, T = −0.20 y R = +0.60**. Estructura y nivel temporal están por debajo de sus referencias; el cambio reciente está por encima del cambio de liga.

Como ejemplo complementario de variabilidad, VOL_EDGE = +0.50 frente a umbrales P50 = 0.20, P75 = 0.40 y P90 = 0.80 se clasifica HIGH_VARIANCE. Su signo indica mayor dispersión relativa; su categoría no añade una cuarta señal direccional.

### Paso 7. Formar dirección e interpretar coincidencias

Las tres magnitudes superan 0.05, por lo que las capas están ACTIVE. Sus pesos direccionales son 0.45, 0.30 y 0.25, distintos de los pesos temporales usados dentro de las ventanas.

Las aportaciones son **−0.180, −0.060 y +0.150**. `ACTIVE_WEIGHT_SUM` es 1. La suma normalizada produce **`P1_TOTALS_DIRECTIONAL_SCORE = −0.090`: UNDER_PROFILE, WEAK**.

Los conteos son una capa OVER, dos UNDER, cero IGNORE y tres activas. `ALIGNMENT_SCORE` = (−0.40 − 0.20 + 0.60)/1.20 = **0**: compensación entre señales. No hay consenso de las activas y se cumple HEATING_CONFLICT porque estructura negativa y tendencia positiva se oponen. HIGH_VARIANCE no activa por sí sola CHAOTIC_CONFLICT; esta última también requiere oposición entre estructura y temporalidad, que aquí comparten signo.

`STRUCTURAL_ANCHOR` vale 0.5 + 0.5 × 0.40 = **0.70**. Se conserva como explicación, sin multiplicar las capas.

Si una capa quedara IGNORE, su aportación direccional sería cero y la media se normalizaría por los pesos activos restantes. Su señal original seguiría disponible para los descriptores que la utilizan. Es una política diferente a la combinación de módulos de lado, cuyos pesos no se redistribuyen.

### Paso 8. Calcular ruptura, dominio y compuesto

La cadena continúa con las mismas S, T y R:

| Concepto | Cálculo en este caso | Resultado |
|---|---|---|
| `BASE_SIGNAL` | S + T. | −0.60. |
| `BREAKOUT_CONDITION` | R positiva se opone a base negativa; ambas magnitudes alcanzan 0.05. | Verdadera. |
| `BREAKOUT_SCORE` | +0.60/(0.60 + 0.60). | +0.50. |
| `HEATING_PRESSURE` | 0.60 − (−0.60). | 1.20. |
| `COOLING_PRESSURE` | No se cumple base positiva y R negativa. | 0. |
| `TREND_DOMINANCE` | Limitar 1.20 entre −1 y +1. | 1. |
| `P1_TOTALS_DRIVER` | Mayor magnitud de contribución activa: 0.180 frente a 0.060 y 0.150. | STRUCTURAL. |
| `P1_TOTALS_COMPOSITE` | 0.55 × (−0.09) + 0.30 × 0.50 + 0.15 × 1. | **0.2505**. |

La dirección del compuesto es **OVER_PROFILE** y su fuerza **MODERATE**. La dirección anterior sigue siendo UNDER_PROFILE, WEAK. El compuesto utiliza ruptura y dominio para expresar la tendencia que desafía la base; `ALIGNMENT_SCORE` y varianza permanecen como explicaciones, sin convertirse en términos adicionales.

La lectura completa sería: **«El nivel deportivo direccional mantiene una inclinación débil hacia menor anotación, pero la subida reciente desafía esa base y el compuesto apunta moderadamente a un perfil de mayor anotación. La estructura sigue siendo la mayor aportación direccional y la dispersión relativa es alta»**.

### Paso 9. Entregar ambas salidas y permitir su revisión

`P1TotalsOutput` reúne identidad, estado, campos direccionales, varianza, conteos, capas, ventanas, referencias, ruptura, driver, compuesto y `raw`. Sus nombres se explican en el [diccionario de salida](#19-diccionario-del-resultado-de-totales). Un perfil completo utiliza `status = OK`.

La entrada de P1 devuelve dos elementos: `side` y `totals`. En este ejemplo, lado conserva CONFLICT/HOME y balance −0.110; totales conserva sus dos lecturas direccional y compuesta. Esos valores no se promedian entre sí.

Si totales no puede formar su perfil, devuelve ausencia y lado ya calculado se mantiene. Una ausencia no se interpreta como UNDER ni como cero neutral. La conservación guarda cada salida disponible en un ámbito separado, como explica [persistencia de minería](../mining-persistence.md#7-traducción-que-realiza-cada-adaptador).

Para revisar lado se sigue **resultado final → regla de decisión → núcleo y presión → módulos → componentes y partidos**. Para revisar totales se sigue **campo direccional o compuesto → fórmulas → S/T/R y varianza → ventanas y referencias → partidos y muestras**. Estos recorridos hacen que las definiciones, fórmulas y objetos anteriores expliquen el resultado real.

## 20. Fuentes y documentación relacionada

Estos archivos son las fuentes de las reglas descritas. Sus nombres permiten que una persona técnica encuentre el cálculo; el lector de esta guía puede utilizar las explicaciones sin abrirlos.

| Archivo | Responsabilidad |
|---|---|
| [run_pillar_1_team_structure.py](../../../modules/pillars/pillar_1_team_structure/run_pillar_1_team_structure.py) | Entrada del pilar y coordinación de lado y totales. |
| [side.py](../../../modules/pillars/pillar_1_team_structure/side/side.py) | Orden de los siete módulos, pesos y reglas finales de lado. |
| [base_strength.py](../../../modules/pillars/pillar_1_team_structure/module_1/base_strength.py) | M1. |
| [offensive_profile_engine.py](../../../modules/pillars/pillar_1_team_structure/module_2/offensive_profile_engine.py) | M2. |
| [direct_matchup_profile.py](../../../modules/pillars/pillar_1_team_structure/module_3/direct_matchup_profile.py) | M3. |
| [quality_adjusted_immediate_state_engine.py](../../../modules/pillars/pillar_1_team_structure/module_4/quality_adjusted_immediate_state_engine.py) | M4. |
| [contextual_competitive_cost_engine.py](../../../modules/pillars/pillar_1_team_structure/module_5/contextual_competitive_cost_engine.py) | M5. |
| [structural_drift_engine.py](../../../modules/pillars/pillar_1_team_structure/module_6/structural_drift_engine.py) | M6. |
| [opponent_expectation_engine.py](../../../modules/pillars/pillar_1_team_structure/module_7/opponent_expectation_engine.py) | M7. |
| [totals.py](../../../modules/pillars/pillar_1_team_structure/totals/totals.py) | Todo el perfil deportivo de totales. |
| [common.py](../../../modules/pillars/common.py) | Objetos de módulo, límite, signo e intensidad general. |
| [streak_analysis_resolver.py](../../../modules/pillars/streak_analysis_resolver.py) | Obtención o reutilización del historial antes de P1. |
| [run_matchup_streak_analysis.py](../../../modules/alerts/matchup_streak_analysis/run_matchup_streak_analysis.py) | Construcción de MatchupStreakContext. |
| [head_to_head.py](../../../modules/alerts/matchup_streak_analysis/head_to_head.py) y [historical_form.py](../../../modules/alerts/matchup_streak_analysis/historical_form.py) | Preparación de enfrentamientos y resultados de cada equipo. |

Para el recorrido de entrada, consulte la [guía principal](00-flujo-principal.md). Para distinguir perfil deportivo y lectura de precios, continúe con [P2](02-pilar-2-mercado-de-lado.md) o [P3](03-pilar-3-mercado-de-totales.md). El contrato técnico general de entradas está en [inputs.md](../inputs.md).
