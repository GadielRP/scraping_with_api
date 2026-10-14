# Qué hacen los pilares P1–P5 y cómo se conserva su trabajo

**Informe para personas sin conocimientos de programación**  
**Fecha de revisión: 14 de octubre de 2026**
**Base del informe: implementación actual del proyecto, incluida la instrumentación inmutable de minería, versión de esquema 5.**

## Cómo leer este informe

Este documento sigue el recorrido de la información: desde que el sistema recibe un encuentro y sus datos hasta que guarda los resultados para estudiarlos después.

Los nombres en inglés se incluyen porque aparecen en los registros del sistema. Cada uno se explica en español. Los ejemplos numéricos son ilustrativos; no representan una evaluación real de un partido. Los pesos y reglas que se indican corresponden al código revisado en la fecha anterior.

## Cómo leer la interpretación oficial de los pilares

P2, P3, P4 y P5 ahora guardan una lectura explícita llamada `INTERPRETATION`.
Los números y cálculos originales siguen disponibles. La lectura explica su
significado mediante campos estructurados y una frase breve en español.

- **P2:** distingue coincidencia entre bookmakers, desacuerdo, neutralidad y
  cobertura parcial. Una única casa no se presenta como consenso. Compara books
  y exchange únicamente cuando sus contratos permiten esa comparación.
  Una cobertura parcial puede conservar AWAY si todas las ramas disponibles
  coinciden hacia AWAY; la cobertura sigue siendo parcial.
- **P3:** conserva la línea de cada lectura. Under en 2.5 y Over en 2.25 describen
  líneas diferentes; no se presentan automáticamente como opiniones contrarias.
  `period_readings` ofrece una lectura directa por periodo, sin recorrer cada
  ámbito detallado. Conserva por separado los contratos válidos múltiples.
- **P4:** separa dónde terminó el precio o edge de hacia dónde se movió. Un edge
  que pasa de +0.20 a +0.10 se mueve hacia AWAY, aunque su valor final sigue siendo
  HOME. Un precio que baja fortalece la selección en términos de cuota; no prueba
  que entró dinero informado. Datos insuficientes no significan estabilidad.
  Los campos se llaman `endpoint_direction`, `net_move_direction` y
  `final_run_direction`. Por ejemplo, el edge puede terminar AWAY y no haberse
  movido: endpoint AWAY, movimiento NO_MOVEMENT y tramo final ausente.
  `contracts` resume cada mercado, periodo, línea y vista mediante sus
  representantes existentes. Las distintas fuentes permanecen identificadas,
  con todas las series originales disponibles para revisión.
- **P5:** distingue empate deportivo DRAW de empate entre cuentas históricas.
  Mantiene su intensidad actual y no representa una probabilidad calibrada.

P2–P4 no reciben etiquetas de intensidad nuevas. El último tramo de P4 se llama
tramo final; no se clasifica como tardío. Los resúmenes `🏁` incluyen una capa
`EXECUTIVE_SUMMARY`, separada de `INTERPRETATION`: la primera permite leer la
conclusión principal rápidamente y la segunda conserva el detalle para auditoría
y minería. P1 Side resume dirección, fuerza y estado; P1 Totals mantiene separado
su score direccional del composite. P2 muestra FT y el periodo secundario; P3
expone las líneas por periodo; P4 resume los contratos principales de
`CHECKPOINT_VIEW`; P5 presenta la memoria 1X2 por bookmaker.

Los engines de cada pilar construyen los dos bloques y los guardan juntos en
`output_payload`. No se derivan señales nuevas ni se duplican métricas. El log
presenta primero estado, conteos, minuto objetivo y resumen ejecutivo. En modo
diagnóstico agrega `INTERPRETATION_DETAIL`. El logger solo presenta los bloques;
no decide ni calcula su significado. Los cierres 🏁 salen después de persistir y
solo para pilares habilitados.

Las evaluaciones anteriores permanecen intactas y no se les agrega esta lectura
retrospectivamente. Para consultas y exportación compacta, ver
[la sección de interpretación oficial](mining-persistence.md#interpretación-oficial).

El objetivo es entender qué significa cada resultado y de dónde sale. Ninguno de los cinco pilares, por sí solo, demuestra que una apuesta será rentable.

### Contenido

1. El sistema en una página.
2. Conceptos necesarios para interpretar los datos.
3. Qué ocurre antes de calcular los pilares.
4. P1: estructura deportiva y contexto de los participantes.
5. P1 Totales: perfil deportivo de anotación.
6. P2: lectura del mercado de lado.
7. P3: lectura del mercado de totales.
8. P4: recorrido observado del mercado.
9. P5: memoria histórica de precios.
10. Cómo se representan resultados, bloqueos y errores.
11. Cómo se guarda todo para minería.
12. Reconstrucción de una evaluación y asociación con el resultado deportivo.
13. Ejemplo de una ejecución completa.
14. Qué significa simular una ejecución.
15. Preguntas frecuentes.
16. Referencias de la implementación.

---

## 1. El sistema en una página

Imagine que cada partido recibe un expediente. Dentro están los participantes, la competición, los resultados anteriores y las cuotas disponibles. Cinco analistas revisan ese expediente desde perspectivas distintas.

| Analista | Pregunta principal | Información central |
|---|---|---|
| P1 | ¿Qué muestran el rendimiento deportivo y la situación competitiva de los participantes? | Resultados anteriores, clasificación, rivales y referencias de liga. |
| P1 Totales, segunda rama de P1 | ¿El perfil deportivo se inclina hacia mayor o menor anotación respecto de su liga? | Anotación a favor y en contra, ventanas de partidos y referencias de liga. |
| P2 | ¿Cómo se inclinan los precios hacia un participante y cómo coinciden o discrepan las fuentes? | Cuotas de ganador y hándicap, líneas y datos del exchange. |
| P3 | ¿Cómo se inclina el mercado hacia mayor o menor anotación? | Precios Over/Under y sus líneas. |
| P4 | ¿Qué recorrido siguieron los precios, las líneas y otras lecturas del mercado? | Observaciones sucesivas con sus horarios reales. |
| P5 | ¿Qué resultados hubo en encuentros anteriores con ese mismo conjunto de precios? | Cuotas actuales y una población histórica de coincidencias. |

Hay **cinco pilares y seis ámbitos de resultado** porque P1 guarda por separado su lectura de lado y su lectura de totales.

Cada ámbito conserva su lógica. P2 no recibe la conclusión de P1 para decidir su lectura; P5 no necesita que P4 produzca un patrón. Compartir un expediente, una selección temporal o un mecanismo de almacenamiento no significa que unos pilares dependan de las conclusiones de otros.

Al terminar, el sistema guarda una fotografía de la evaluación y los seis resultados. No hace una votación entre los pilares ni calcula automáticamente una puntuación general del encuentro.

La minería es la capacidad de estudiar después esas fotografías: comparar resultados, detectar relaciones y medir comportamientos. En esta etapa, guardar para minería no añade una regla predictiva ni crea un Pilar 6.

## 2. Conceptos necesarios para interpretar los datos

### 2.1. Evento, participante, mercado y selección

Un **evento** es un encuentro concreto. Su `event_id` permite distinguirlo de otros encuentros entre los mismos equipos.

**HOME** y **AWAY** identifican local y visitante, o los dos lados equivalentes del encuentro. **DRAW** significa empate.

Un **mercado** establece qué condición se está evaluando:

- **1X2:** victoria local, empate o victoria visitante.
- **Home/Away:** dos resultados, local o visitante, según las condiciones del mercado.
- **Over/Under:** superar o quedar por debajo de una línea de anotación.
- **Hándicap:** aplicar una ventaja o desventaja al marcador para resolver el resultado del contrato.

Una **selección** es una alternativa dentro del mercado: por ejemplo, local o Under.

### 2.2. Cuota y línea son números diferentes

Una **cuota decimal** es un precio. Una cuota 2.00 implica un retorno bruto de dos unidades por cada unidad si se cumple el resultado.

Una **línea** establece una condición deportiva. En «Under 2.5 a cuota 1.90», 2.5 es la línea y 1.90 es el precio.

Comparar Under 2.5 y Under 3.0 como si fueran el mismo contrato puede producir una conclusión equivocada. El sistema conserva esa distinción.

### 2.3. Periodo y contrato

El **periodo** indica qué parte del encuentro cuenta: tiempo reglamentario, tiempo completo incluyendo prórroga, primera mitad, primeras cinco entradas de béisbol u otro periodo soportado.

El **contrato** es la identidad completa de lo evaluado. Reúne mercado, periodo, línea y demás condiciones relevantes. También se conservan la selección y la casa que publicó el dato.

Por tanto, «Under = MODERATE» sería una descripción incompleta. Para estudiar esa observación después hay que reconocer el evento, el mercado, el periodo, la línea y su procedencia, cuando correspondan a esa lectura.

P1 tiene un ámbito deportivo propio: su perfil de totales no representa automáticamente un Under de una línea publicada por una casa.

### 2.4. Casa de apuestas, proveedor y exchange

Una **casa de apuestas**, también llamada bookmaker o bookie, publica precios. Un **proveedor** entrega información al sistema y puede distribuir datos de varias casas. Son identidades diferentes.

En las lecturas actuales de P2, P3 y P4 participan Pinnacle, bet365 y Betfair, dentro de sus mercados soportados. P5 calcula perfiles independientes para Pinnacle, bet365 y SofaScore; Betfair tiene un papel diagnóstico.

Betfair es el **exchange** estudiado. Allí aparecen:

- **BACK:** precio para apoyar un resultado.
- **LAY:** precio para tomar la posición contraria.
- **Size:** cantidad disponible publicada en un nivel de precio.

La cantidad disponible es contexto. Su presencia no significa que todos los cálculos la utilicen como peso ni que represente todo el dinero negociado.

### 2.5. Datos originales, variables derivadas y clasificación

Piense en esta cadena:

> Cuotas recibidas → comparación numérica → dirección → explicación del resultado.

El **input** es la información recibida. Las variables **RAW** son valores de base o intermedios conservados antes de convertir el resultado en una presentación resumida. El significado preciso de RAW depende del campo: no siempre significa «respuesta original del proveedor».

Una variable **derivada** sale de aplicar una operación a otros datos. Por ejemplo, una diferencia entre dos precios o una media de anotación.

Un **score** es una puntuación calculada. Una **clasificación** traduce un número o una combinación de reglas a una etiqueta, como HOME, MODERATE o CONFLICT.

Guardar solo la etiqueta pierde información. Dos valores diferentes pueden recibir la misma categoría y comportarse de manera distinta cuando se estudien muchos casos.

## 3. Qué ocurre antes de calcular los pilares

### 3.1. El evento debe llegar al recorrido de evaluación

El trabajo previo al inicio busca encuentros, calcula cuánto falta para comenzar y aplica sus filtros de competición, fase y momento de ejecución.

Activar el interruptor de pilares permite ejecutar ese recorrido, pero el evento todavía debe superar los filtros. Por ejemplo, un evento identificado como `knockouts/playoffs` puede quedar fuera del recorrido que exige `regular_season`.

Un evento excluido antes de llegar al procesador no necesariamente genera una evaluación minera. En cambio, una evaluación que sí se abre debe representar sus seis ámbitos, incluidos los pilares deshabilitados como `SKIPPED`.

### 3.2. Se prepara el expediente deportivo

El expediente reúne participantes, competición, temporada, horario, deporte y contexto disponible. Si P1 participa, se intenta completar la información de competición y el historial deportivo que necesita.

Ese historial puede incluir resultados de los equipos, enfrentamientos directos, clasificación y referencias de anotación de la liga. Si ya fue preparado dentro del recorrido, se reutiliza. La posibilidad de adquirirlo depende de los requisitos y momentos permitidos por el resolvedor; activar P1 no garantiza que esas entradas existan.

Las consultas externas necesarias para esta preparación se hacen antes de abrir la transacción que guardará la evaluación.

### 3.3. Se organizan las cuotas

El sistema recibe observaciones de cuotas y las organiza por evento, mercado, periodo, línea, casa, selección y procedencia. Esa organización se llama `odds_trajectory_context`.

De ahí salen muchas de las variables derivadas de mercado: precios seleccionados, pares comparables, líneas, checkpoints y referencias a las observaciones originales.

Una observación conserva sus identificadores y sus tiempos. Un dato recibido pero no admisible para el cálculo puede mantenerse en el expediente original con una explicación de disponibilidad.

### 3.4. Se elige el momento de mercado

Un **checkpoint** es el estado seleccionado para un momento previsto. T−5 significa aproximadamente cinco minutos antes del inicio.

La selección común utiliza los momentos permitidos y disponibles. A cinco minutos del inicio, si existen T−120, T−30 y T−5, puede escoger T−5. Si falta T−5 y existe T−30, puede utilizar esa lectura anterior. T−1 corresponde a un instante posterior y no completa una evaluación de T−5.

Con hora de fuente se elige el último cambio anterior o igual al objetivo, aunque llevara horas vigente. Sin tick admisible con fuente se utiliza una observación sin fuente cercana al objetivo, dentro de la tolerancia configurada (tres minutos por defecto) y disponible al evaluar. No se extiende hacia momentos antiguos.

P2–P5 comparten esta selección. P1 utiliza sus entradas deportivas.

### 3.5. Se determina el periodo de tiempo completo

La preparación común revisa primero el tiempo completo incluyendo prórroga. Si encuentra una lectura soportada que permita evaluarlo, lo selecciona. En caso contrario revisa el tiempo reglamentario.

No elige simplemente el periodo con más filas. La prioridad contractual importa. La existencia de coincidencias históricas de P5 no decide esta selección.

Los periodos secundarios soportados mantienen su identidad y pueden analizarse por separado.

### 3.6. Se captura la muestra histórica de P5

Si P5 participa, se extraen los vectores de precios actuales, se consultan sus coincidencias en la vista histórica y se guardan los miembros de las muestras dentro de la misma transacción de evaluación.

Los conteos que recibirá el cálculo se obtienen de esos miembros capturados. Así, el resultado y su población se refieren a los mismos eventos.

En PostgreSQL se utiliza una lectura consistente durante la transacción: las consultas de las distintas casas comparten una misma versión visible de la población.

### 3.7. Se fija `evaluation_as_of`

`evaluation_as_of` es la hora UTC que identifica la frontera de conocimiento de la evaluación. Se fija después de preparar y capturar las entradas necesarias.

UTC es una referencia horaria común que evita confundir horarios locales. El minuto de mercado y `evaluation_as_of` son datos distintos: uno describe la lectura respecto del inicio; el otro identifica cuándo se cerró la adquisición de esta evaluación.

Después se calculan los pilares con el conjunto preparado. Los datos que lleguen posteriormente no completan la fotografía ya cerrada.

La reconstrucción futura debe leer esa fotografía. Volver a consultar hoy una fuente externa puede devolver información diferente.

## 4. P1: estructura deportiva y contexto de los participantes

### 4.1. Qué recibe y qué pretende describir

P1 recibe el expediente deportivo y el historial preparado. Trabaja con resultados, marcadores, clasificación, rivales, enfrentamientos directos y contexto de competición.

Su rama de lado pregunta cómo se comparan los participantes. Para ello calcula siete módulos, M1–M7. Sus números suelen utilizar signo positivo para favorecer al local, negativo para favorecer al visitante y cero para neutralidad.

La ausencia de un dato se conserva con su explicación. En determinadas combinaciones, un módulo no disponible aporta un valor efectivo cero; eso no convierte su ausencia en una observación deportiva neutral.

### 4.2. M1: fuerza base de temporada

**Pregunta:** ¿qué equipo presenta una base competitiva más favorable?

Separa dos tipos de información. Los registros de temporada y la clasificación alimentan las tasas de victoria y diferencias de anotación por partido. Las series de márgenes de partidos alimentan consistencia y dirección de la variabilidad.

Calcula cuatro contrastes:

| Componente | Qué compara | Peso dentro de M1 |
|---|---|---|
| Resultados | Diferencia entre tasas de victoria. | 35% |
| Margen de anotación | Diferencia entre márgenes por partido, ajustada a la escala de liga. | 35% |
| Consistencia | Diferencia de dispersión de los márgenes. | 10% |
| Dirección de la variabilidad | Información sobre cómo se distribuyen los márgenes favorables y desfavorables. | 20% |

El módulo combina esos componentes y limita el resultado al intervalo de −1 a +1. Limitar significa que un valor que exceda el máximo se queda en el máximo admitido.

Devuelve puntuación, lado, intensidad, componentes y detalle de sus fuentes, series y cálculos. Para minería interesa tanto el resultado M1 como saber qué componente lo explicó.

### 4.3. M2: perfil ofensivo

**Pregunta:** ¿quién anota con mayor regularidad, alcanza mejores techos y evita quedarse sin anotar?

Utiliza las anotaciones válidas de cada equipo. Sus muestras pueden tener tamaños distintos.

| Componente | Cómo lo obtiene | Peso |
|---|---|---|
| Regularidad | Compara cuánto varían las anotaciones. | 35% |
| Techo ofensivo | Compara la media de las cinco mayores anotaciones; si hay menos de cinco, utiliza las disponibles. | 30% |
| Evitar ceros | Compara la proporción de partidos sin anotar. | 20% |
| Explosión ofensiva | Compara la proporción con anotación de al menos 2.0. | 15% |

El umbral 2.0 es una regla fija actual del módulo. No se adapta automáticamente a cada deporte.

Con al menos cinco anotaciones válidas por equipo, el módulo tiene su cobertura completa prevista. Con una a cuatro puede calcular un perfil degradado. Sin anotación válida de alguno, carece de información suficiente.

Se preservan las series, cantidades, medias, dispersión, tasas, pesos y aportaciones. Una etiqueta de cobertura degradada no equivale a un error de ejecución.

### 4.4. M3: enfrentamientos directos

**Pregunta:** ¿qué muestran los encuentros anteriores entre estos participantes?

Primero orienta los resultados hacia el local y visitante del encuentro actual. Un partido anterior pudo jugarse con localías invertidas.

Calcula un contraste de resultados, contando el empate como medio resultado para cada lado, y otro de anotación acumulada. El primero pesa 70% y el segundo 30%.

Después atenúa la combinación según el tamaño de la muestra: multiplica por el menor valor entre número de encuentros dividido por cinco y uno. Con dos antecedentes aplica un factor 0.40; con cinco o más llega a 1.

Esto evita que uno o dos enfrentamientos tengan la misma aportación que la muestra de referencia. El resultado mantiene los partidos usados, su orientación, conteos y factor aplicado.

### 4.5. M4: estado inmediato ajustado por el rival

**Pregunta:** ¿cómo se comparan los resultados recientes considerando a quién se enfrentó cada equipo?

Una victoria sobre un rival fuerte y una victoria sobre uno débil no reciben necesariamente la misma lectura. El módulo utiliza los cinco partidos válidos más recientes por equipo, la posición del rival y el tamaño de liga para representar fuerza y debilidad.

Ajusta resultados y márgenes, obtiene un contraste entre participantes y combina ambos: 60% para resultados ajustados y 40% para márgenes ajustados.

Su puntuación completa se conserva. En la agregación de P1, su influencia se acota a un ajuste entre −0.06 y +0.06; un M4 de +0.80 aporta allí solamente +0.06.

### 4.6. M5: coste competitivo del contexto

**Pregunta:** ¿qué coste competitivo tendría no sumar puntos en este momento de la temporada?

M5 utiliza clasificación, puntos, objetivos, posiciones de corte y partidos restantes. Su aplicabilidad actual exige haber disputado al menos el 80% de la temporada regular.

Fuera de esa ventana queda inactivo con su motivo. La falta de participación no significa presión cero observada: significa que esta regla contextual no corresponde o no puede aplicarse.

Cuando participa, identifica situaciones como alcanzar una zona objetivo, disputar una plaza o evitar una zona de peligro. Combina la gravedad base del objetivo, el régimen competitivo y la urgencia derivada de las distancias y puntos restantes. Después compara el coste del local y del visitante.

La implementación contiene supuestos concretos, entre ellos tres puntos por victoria y posiciones de corte configuradas en el motor. La lectura debe interpretarse dentro de esas reglas; no es una medición directa de motivación psicológica.

M5 no utiliza cuotas para decidir qué participante tiene mayor presión. Conserva objetivos, distancias, régimen, urgencia, costes y el contraste final.

### 4.7. M6: evolución estructural

**Pregunta:** ¿cómo ha evolucionado el rendimiento a lo largo de la historia disponible?

Ordena la historia, la organiza en cinco bloques y calcula sus medias de rendimiento. Estudia la pendiente de esos bloques y la estabilidad de la serie.

Combina 65% de contraste de tendencia global y 35% de contraste de estabilidad. De esta forma distingue una mejora sostenida de una serie que alterna resultados muy diferentes.

Guarda la muestra, la distribución por bloques, las medias, pendientes, dispersión y aportaciones. La clasificación final no sustituye esos valores.

### 4.8. M7: rendimiento frente a la expectativa del rival

**Pregunta:** ¿los equipos rindieron mejor o peor de lo esperado dada la posición de sus oponentes?

Para cada partido construye una expectativa basada en la fuerza del rival. Compara el resultado observado con esa expectativa y hace una comparación adicional del margen de anotación.

El contexto por partido combina 60% de resultado frente a expectativa y 40% de margen frente a expectativa. Después obtiene las medias de cada participante y las contrasta.

Aquí «expectativa» es la referencia construida por esta fórmula a partir de posiciones. No es una expectativa obtenida de las cuotas ni una probabilidad calibrada.

Se guardan posiciones, expectativas, resultados reales, diferencias y promedios.

### 4.9. Cómo P1 combina M1–M7

La combinación tiene tres partes.

**Primera: núcleo deportivo.**

| Módulo | Peso en el núcleo |
|---|---|
| M1 | 30% |
| M2 | 20% |
| M3 | 15% |
| M6 | 20% |
| M7 | 15% |

Multiplica cada valor efectivo por su peso y suma las aportaciones. Los pesos de módulos no disponibles no se reparten entre los demás.

**Segunda: ajuste inmediato.** Añade M4 limitado al intervalo −0.06 a +0.06. Así obtiene el núcleo efectivo y su dirección e intensidad.

La escala general de intensidad usa la magnitud: menos de 0.05 es `IGNORE`; desde 0.05 hasta menos de 0.15 es `LOW`; desde 0.15 hasta menos de 0.30 es `MEDIUM`; desde 0.30 hasta menos de 0.60 es `HIGH`; desde 0.60 es `EXTREME`.

**Tercera: relación con la presión de M5.** Compara lado e intensidad del núcleo con lado e intensidad de presión. M5 tiene una escala particular para esta combinación: `LOW`, `MODERATE`, `HIGH`, `SEVERE` y `OVERRIDE`, con cortes de magnitud en 0.15, 0.30, 0.45 y 0.60.

La decisión final sigue reglas de coincidencia y conflicto. Puede conservar el lado deportivo, marcar oposición o abrir una ventana contextual hacia el otro participante.

| Clasificación interna | Lectura en lenguaje natural |
|---|---|
| `CONFIRMED` | Las reglas conservan el lado del núcleo sin el conflicto que justificaría otra categoría. |
| `DEGRADED` | Conserva ese lado con una lectura debilitada. |
| `CONFLICT` / `STRONG_CONFLICT` | Conserva el lado del núcleo y registra oposición contextual relevante. |
| `CONTEXT_WINDOW` / `STRONG_CONTEXT_WINDOW` | El contexto establece un lado cuando el núcleo no es operativo. |
| `UNDERDOG_WINDOW` / `STRONG_UNDERDOG_WINDOW` | La presión establece el lado contrario a un núcleo débil. El nombre no implica haber consultado quién tiene la cuota más alta. |
| `NO_BET` | Las reglas no producen un lado final operativo. No es una orden de apuesta. |

También devuelve un balance numérico: núcleo efectivo más M5. **Ese balance es evidencia y no decide por sí solo el lado final.**

Ejemplo: un núcleo +0.24 hacia el local y una presión −0.35 hacia el visitante producen balance −0.11. La regla correspondiente puede clasificar `CONFLICT` conservando HOME. Leer solamente el signo −0.11 llevaría a interpretar mal la salida.

### 4.10. Qué devuelve y qué conserva minería

P1 Side devuelve su valor, los siete módulos con componentes, los cálculos RAW y la agregación: núcleo, ajuste, presión, relación, clasificación final, lado y anomalías cuando existan.

Minería conserva una unidad de resumen, unidades por módulo y unidades por componente. También preserva sus valores originales, efectivos, pesos, aportaciones, disponibilidad y motivos.

Las clasificaciones deportivas como `CONFLICT` conviven con el estado global del registro. No sustituyen a `ACTIVE`, `INSUFFICIENT_DATA`, `ERROR` o `SKIPPED`.

## 5. P1 Totales: perfil deportivo de anotación

### 5.1. Qué recibe

Recibe anotaciones a favor y en contra de ambos participantes, totales de partidos anteriores, duración de temporada regular y referencias de anotación de la liga.

Esta rama calcula sus propias capas. No toma una media de los siete módulos de lado. Tampoco utiliza la puntuación de P3 ni una línea de apuesta para decidir directamente si se superará un total concreto.

### 5.2. Equilibrar las muestras

Conserva partidos válidos y los ordena por fecha cuando la información lo permite. Utiliza la menor cantidad disponible entre los dos participantes.

Si el local tiene 18 partidos válidos y el visitante 12, la muestra equilibrada tiene 12 por lado. Esto evita comparar un horizonte de 18 con otro de 12 en las series centrales de esta rama.

### 5.3. Construir cuatro ventanas

Una ventana es un grupo de partidos recientes que representa un horizonte. Las ventanas se superponen: las más extensas incluyen también los partidos recientes.

| Ventana | Tamaño objetivo respecto de la temporada | Peso temporal base |
|---|---|---|
| `SHORT` | 15% | 30% |
| `RECENT` | 35% | 35% |
| `MID` | 60% | 20% |
| `FULL` | 100% | 15% |

El tamaño objetivo se redondea con la regla de mitad hacia arriba. Una temporada de 38 partidos genera objetivos de 6, 13, 23 y 38.

Para cada ventana distingue cuántos partidos esperaba, cuántos pudo usar y qué proporción completó. Su peso efectivo es el peso base multiplicado por esa completitud.

Con diez partidos por lado, SHORT está completa, mientras FULL tiene 10 de los 38 previstos. La ventana extensa conserva información, pero su peso temporal disminuye.

El campo público `WINDOWS_USED` guarda los tamaños objetivo. Las cantidades realmente usadas se encuentran en el detalle de ventanas; no deben confundirse.

### 5.4. Construir referencias de liga

El motor necesita saber qué significa una anotación alta o baja en ese contexto. Calcula centros y escalas a partir de resultados de liga.

La **mediana** representa el centro de una lista ordenada. Una **escala dinámica** expresa qué distancia respecto de ese centro es relevante en esa distribución. Varias escalas utilizan el percentil 75 de las distancias absolutas respecto de la mediana.

Un percentil es una posición dentro de una distribución ordenada. Se utiliza para contextualizar el encuentro frente a su liga, no como precio de mercado.

Si falta una referencia necesaria, el motor conserva la salida temprana y su explicación. No inventa una referencia de cero para continuar.

### 5.5. Las tres capas direccionales

**Capa estructural, S.** Cruza el ataque promedio de un participante con la anotación que recibe el otro. Suma los dos entornos para obtener un total estructural. Después compara ese total con el centro de liga y lo divide por la escala dinámica.

Ejemplo: local anota 1.6 y recibe 1.0; visitante anota 1.2 y recibe 1.4. El entorno local es 1.5 y el visitante 1.1; el total estructural es 2.6. Con referencia 2.4 y escala 0.8, la señal es +0.25.

**Capa temporal, T.** Calcula el nivel de anotación de cada ventana y combina esas medias utilizando los pesos efectivos. Promedia la lectura temporal de ambos participantes y la compara con la referencia de liga. Describe el nivel ponderado, dando importancia a la cobertura y los horizontes recientes.

**Capa de tendencia, R.** Construye un perfil corto con 60% SHORT y 40% RECENT, y uno largo con 60% MID y 40% FULL. Resta largo a corto para cada equipo, promedia ambos cambios y los compara con los cambios de equipos de la liga.

Una tendencia positiva significa que el tramo reciente sube respecto del horizonte amplio y de su referencia. No implica que el nivel total ya sea alto. Por eso tendencia y nivel temporal son variables distintas.

### 5.6. Variabilidad: una lectura adicional

Estudia cuánto se dispersan los totales de partido de cada participante, obtiene una lectura conjunta y la compara con la liga.

Devuelve el contraste con signo y categorías como `NORMAL_VARIANCE`, `ELEVATED_VARIANCE`, `HIGH_VARIANCE`, `EXTREME_VARIANCE` o `UNKNOWN_VARIANCE`.

La categoría considera la magnitud del contraste. Una diferencia extrema puede corresponder a mucha más o mucha menos dispersión que la referencia; el signo permite distinguir ambas situaciones.

Esta lectura no vota como una cuarta capa por Over o Under. Su ausencia tampoco elimina necesariamente una dirección que sí pudo calcularse.

### 5.7. Puntuación direccional

Las capas usan pesos distintos: estructura 45%, temporalidad 30% y tendencia 25%.

Una capa participa en la combinación direccional si su magnitud alcanza 0.05. Una capa menor conserva su valor y queda clasificada como `IGNORE`.

La puntuación direccional es la suma de las aportaciones activas dividida por la suma de sus pesos. Aquí sí se normaliza según las capas activas. Si solo participa estructura con +0.20, el resultado es +0.20.

Positivo significa `OVER_PROFILE`; negativo, `UNDER_PROFILE`; cero, `NEUTRAL_PROFILE`. Son direcciones respecto del contexto deportivo, sin una línea de apuesta incorporada.

Se conservan `OVER_COUNT`, `UNDER_COUNT`, cantidad de capas activas e ignoradas y alineación. La alineación resume si las capas apuntan al mismo lado o se compensan; no es un porcentaje de acierto.

### 5.8. Ruptura, dominio de tendencia y composite

El motor conserva una segunda puntuación, el **composite**, que combina dirección y cambio de perfil.

Primero forma una base sumando estructura y temporalidad. Si la tendencia apunta al lado contrario y ambas magnitudes alcanzan 0.05, calcula una ruptura: magnitud de tendencia dividida por magnitud de tendencia más magnitud de base, conservando el signo de la tendencia.

El **dominio de tendencia** es tendencia menos base, limitado entre −1 y +1. También conserva presiones de calentamiento o enfriamiento cuando la tendencia se opone a la base.

La fórmula final es:

> Composite = 55% de puntuación direccional + 30% de ruptura + 15% de dominio de tendencia, limitado entre −1 y +1.

La alineación se guarda como explicación; no se añade como término de esa fórmula.

Ejemplo: estructura −0.40, temporalidad −0.20 y tendencia +0.60.

1. La puntuación direccional es −0.09.
2. La base es −0.60.
3. La ruptura es +0.50.
4. El dominio de tendencia es +1, después de limitar +1.20.
5. El composite es −0.0495 + 0.15 + 0.15 = **+0.2505**.

La dirección base sigue siendo ligeramente Under y el compuesto apunta a Over moderado por la presión de cambio. Son dos lecturas diferentes que se conservan juntas.

### 5.9. Intensidad y salida

Para la puntuación direccional y el compuesto, las categorías de magnitud son:

| Magnitud | Intensidad |
|---|---|
| Menor que 0.05 | `NONE` |
| Desde 0.05 hasta menos de 0.15 | `WEAK` |
| Desde 0.15 hasta menos de 0.30 | `MODERATE` |
| Desde 0.30 hasta 0.60 inclusive | `STRONG` |
| Mayor que 0.60 | `VERY_STRONG` |

La salida conserva ventanas, cobertura, promedios, referencias, escalas, capas, pesos, contribuciones, dirección, intensidad, variabilidad, conflictos, ruptura, tendencia y composite.

Minería guarda las capas como unidades y los valores continuos como métricas. Se puede estudiar después un intervalo de composite, por ejemplo entre −0.15 y −0.10, sin limitarse a una etiqueta `WEAK_UNDER` o `MODERATE_UNDER`.

## 6. P2: lectura del mercado de lado

### 6.1. Qué recibe

Recibe la identidad del evento, las cuotas organizadas, el momento elegido y los contratos preparados. Extrae lecturas de ganador, Asian Handicap y hándicap estándar dentro de sus periodos soportados.

Trabaja con Pinnacle, bet365 y Betfair. Mantiene separadas las familias 1X2 y Home/Away, los periodos, las líneas y los lados BACK/LAY.

### 6.2. Cómo transforma precios en inclinación

Para una lectura individual necesita cuotas válidas de local y visitante del mismo contrato. Transforma cada cuota en su inverso: uno dividido por el precio.

Después resta el inverso visitante al inverso local y divide esa diferencia por la suma de ambos inversos.

Con local 1.80 y visitante 2.20 obtiene aproximadamente +0.10. El signo positivo representa HOME; el negativo, AWAY; cero, NEUTRAL.

Este contraste se llama **edge**. En 1X2 no utiliza el empate en esa fórmula, aunque lo conserve cuando existe. Por eso no es una estimación completa de probabilidad de victoria dentro de tres resultados.

No hay una intensidad general de P2 equivalente a los tramos de P1. La dirección se obtiene del signo, incluso si el valor es pequeño.

### 6.3. Qué compara entre casas

Cuando los contratos son compatibles, calcula:

- Edge individual de Pinnacle y de bet365.
- **Representante:** media de ambos edges, con pesos iguales.
- **Gap:** diferencia absoluta entre ellos.
- **Relación:** coincidencia hacia HOME, coincidencia hacia AWAY, oposición o neutralidad.

Si Pinnacle produce +0.10 y bet365 −0.06, la media es +0.02 y el gap 0.16. La media ligeramente local no oculta que las casas apuntan a lados opuestos.

Si solo hay Pinnacle, su edge puede calcularse. La media y la comparación entre las dos casas quedan bloqueadas: no se sustituyen por una falsa comparación de Pinnacle consigo misma.

### 6.4. Qué hace con el hándicap

Separa línea y precio. Puede medir una diferencia entre líneas aunque no tenga todos los precios.

Si una casa tiene −0.5 y otra −1.5, la separación es 1.0. Las comparaciones de inclinación que exigen el mismo contrato no se calculan como si esas condiciones fueran iguales.

Asian Handicap y hándicap estándar conservan lecturas distintas. P2 también compara orientaciones de ganador y hándicap cuando tiene los representantes necesarios.

### 6.5. Qué hace con Betfair y los periodos

Calcula edges BACK y LAY por separado. Cuando son compatibles, obtiene su media, diferencia y relación. También puede medir la separación relativa entre precio LAY y BACK.

La comparación casas–exchange necesita los representantes correspondientes. En hándicap exige además compatibilidad de líneas para las comparaciones de precio.

Puede comparar la orientación de tiempo completo con la de un periodo secundario soportado. En béisbol identifica expresamente las primeras cinco entradas, sin presentarlas como una primera mitad.

Las cantidades disponibles del exchange se conservan como contexto, sin ponderar estos edges.

### 6.6. Qué devuelve y guarda

Devuelve muchas señales independientes: edges, direcciones, medias, gaps, diferencias de línea y relaciones. También entrega perfiles intermedios, cobertura, entradas seleccionadas y referencias a contratos.

Minería conserva cada señal con estado, valor, motivo, evidencia y dimensiones aplicables. Una señal que compara dos casas mantiene referencias a ambas; no se le asigna arbitrariamente una sola casa como si fuera una observación individual.

P2 puede estar `ACTIVE` aunque solamente algunas de esas señales sean calculables. No produce un único «P2 = probabilidad de victoria».

## 7. P3: lectura del mercado de totales

### 7.1. Qué recibe y cómo obtiene un edge

Recibe las cuotas Over/Under, sus líneas, el momento elegido y los contratos por periodo. Utiliza Pinnacle, bet365 y Betfair.

La fórmula individual sigue el mismo procedimiento de inversos que P2, sustituyendo local/visitante por Over/Under:

> Edge = diferencia entre inverso de cuota Over e inverso de cuota Under, dividida por su suma.

Over 1.80 y Under 2.20 producen aproximadamente +0.10, dirección OVER. Al intercambiar los precios resulta −0.10, dirección UNDER. Precios iguales producen NEUTRAL.

Esto describe la inclinación de los precios. No calcula goles esperados a partir del rendimiento deportivo.

### 7.2. Diferencia de líneas y comparación de precios

Guarda la línea de cada casa y calcula tanto la resta con signo como su magnitud absoluta.

Con Pinnacle 2.5 y bet365 3.0, la diferencia es −0.5 y el gap de línea 0.5. Ese −0.5 no es un edge Under: representa una separación de condiciones.

Para obtener la media de edges y compararlos exige líneas iguales, mismo periodo y lecturas disponibles. Si las condiciones difieren, conserva los edges individuales y bloquea las comparaciones incompatibles.

Cuando existe un representante, traduce su orientación a contexto de mercado: `OPEN_BIAS`, `CLOSED_BIAS` o `NEUTRAL_BIAS`. Estas etiquetas no prueban que los equipos tengan un estilo deportivo abierto o cerrado.

### 7.3. Exchange y comparación entre periodos

Conserva Over y Under de BACK y LAY, cantidades disponibles y sus líneas. Calcula lecturas individuales y, cuando son compatibles, un representante del exchange.

La comparación con las casas distingue diferencia de líneas de diferencia de precios. No compara Over 2.5 y Over 3.0 como si fueran la misma condición.

También puede comparar la orientación de tiempo completo y primera mitad. Cada representante debe ser válido dentro de su periodo; las líneas de ambos periodos no necesitan ser iguales para comparar sus orientaciones.

Over en tiempo completo y Under en primera mitad se registran como discrepancia. El motor no deduce de ello una cantidad exacta de goles en la segunda mitad.

### 7.4. Qué devuelve y guarda

Devuelve perfiles por periodo y contrato, edges individuales, estructura de líneas, representantes, relaciones entre casas, exchange y periodos, cobertura y explicaciones.

Minería guarda sus señales e intermedios junto a las observaciones seleccionadas y dimensiones del contrato. Un edge cero permanece como cálculo válido; un edge ausente queda sin valor y con su motivo.

P1 Totales y P3 pueden discrepar: uno describe historia deportiva relativa a la liga; el otro, precios y líneas del mercado.

## 8. P4: recorrido observado del mercado

### 8.1. Qué recibe

Recibe el historial de observaciones disponible hasta la evaluación, sus tiempos, contratos y selección temporal.

Analiza las familias soportadas de 1X2, Home/Away, Asian Handicap y Over/Under. El hándicap estándar no pertenece a su dominio actual. Los periodos admitidos dependen de la familia; incluyen lecturas de tiempo completo, primera mitad y primer cuarto para totales.

Organiza cada serie por casa, proveedor, selección, mercado, periodo, línea y lado/nivel del exchange. Mantiene separados precios de contratos distintos.

### 8.2. Dos formas de mirar el recorrido

**Vista de checkpoints:** sitúa los estados reconstruidos en sus momentos previstos, por ejemplo T−360, T−120, T−30 y T−5, según la configuración y la serie.

**Vista adaptativa:** mantiene los cambios en sus horas de fuente y las observaciones sin fuente en sus horas reales. Si el último cambio precede al checkpoint final, conserva el cambio y añade un cierre lógico con el mismo precio en el objetivo. El cierre no es una nueva adquisición ni se guarda como snapshot ficticio.

Una trayectoria puede verse estable en pocos checkpoints y mostrar idas y vueltas en sus puntos intermedios. Por eso las vistas se conservan por separado.

### 8.3. Qué datos puede seguir

P4 no sigue exclusivamente cuotas. Puede formar series de inversos de cuota, líneas, límites publicados, cantidades del exchange, edges de lado o totales, representantes y diferencias entre fuentes.

Las series derivadas conservan referencias a sus ingredientes. Un aumento de cuota y un aumento de su inverso tienen sentidos contrarios; hay que mirar el tipo de valor antes de interpretar el signo.

### 8.4. Qué calcula entre dos puntos

Un tramo une dos observaciones consecutivas. Guarda valor inicial, final, diferencia, diferencia absoluta y minutos realmente transcurridos.

Cuando existe continuidad y el tiempo es positivo, divide el cambio por los minutos para obtener velocidad.

Las horas reales importan: bajar 0.10 en noventa minutos tiene una velocidad distinta de bajar lo mismo en cinco minutos.

Para cantidades del exchange, el motor conserva cambios pero deja sin valor las velocidades y sus lecturas de aceleración/desaceleración previstas para otros tipos de serie.

### 8.5. Qué calcula sobre el recorrido

| Lectura | Qué explica |
|---|---|
| Cambio neto | Diferencia entre el último valor y el primero. |
| Longitud del recorrido | Suma de cambios absolutos de los tramos continuos observados. |
| Eficiencia | Qué proporción del recorrido queda reflejada en el cambio neto, si el recorrido está completo y permite la división. |
| Patrón | Ausencia de movimiento, movimiento en un sentido, una inversión o varias inversiones. |
| Rachas | Grupos continuos de tramos en la misma dirección. |
| Giros | Picos, valles y cambios de sentido, incluidas mesetas cuando corresponda. |
| Corrección | Cuánto recuperó la primera racha opuesta respecto de la inicial. |
| Retención y sobrepaso | Cuánto del movimiento inicial quedó o si la corrección lo excedió. |
| Ventanas temporales | Dónde se concentró el recorrido: temprano, desarrollo o tramo final. |
| Relaciones entre fuentes | Cómo cambiaron la separación y la alineación entre casas y exchange. |

Ejemplo: cuota 2.00 → 1.90 → 1.95.

- Bajó 0.10 y después recuperó 0.05.
- El cambio neto es −0.05.
- El recorrido observado completo suma 0.15.
- La eficiencia es aproximadamente 0.3333.
- Existe una inversión de dirección.
- La primera corrección recupera la mitad del movimiento inicial.

La lectura es descriptiva. Estos cálculos no demuestran que alguien conociera información privilegiada ni identifican quién movió el precio.

### 8.6. Qué hace si faltan observaciones

Necesita un punto final operativo y al menos dos observaciones para formar las métricas temporales correspondientes.

Si esperaba T−120 → T−30 → T−5 y falta T−30, no inventa ese checkpoint. Puede conservar información parcial y el cambio entre extremos, pero no certifica un recorrido continuo completo para las métricas que lo exigen.

Distingue ausencia de observaciones, insuficiencia de puntos, falta del punto final, huecos de continuidad y cambios de contrato. La falta de información no se presenta como «el mercado no se movió».

Si cambia una línea, puede formar una serie de líneas; las series de cuotas de cada línea siguen separadas. Una serie cuyo contrato terminó conserva esa condición y sus antecedentes.

### 8.7. Qué devuelve y guarda

Devuelve señales por serie, observaciones, checkpoints presentes y ausentes, tramos, movimientos, transformaciones, patrones, cobertura y referencias.

Minería permite recuperar el recorrido observado, sus límites y los huecos. No interpola silenciosamente ni utiliza puntos recibidos después para completar la evaluación original.

P4 no entrega una única puntuación general de «movimiento inteligente».

## 9. P5: memoria histórica de precios

### 9.1. Qué recibe

Recibe la identidad del encuentro, el conjunto organizado de cuotas, el momento seleccionado, los contratos y las muestras históricas preparadas.

Calcula memoria para los mercados de ganador soportados: 1X2 y Home/Away. No extiende automáticamente esta búsqueda a Over/Under o hándicap.

Cada perfil pertenece a una casa, familia y periodo. Actualmente participan Pinnacle, bet365 y SofaScore por separado.

### 9.2. Construir el vector actual

Un **vector de precios** es el conjunto de cuotas que describe ese mercado:

- En 1X2: cuota local, empate y visitante.
- En Home/Away: cuota local y visitante.

Debe ser un conjunto coherente de la misma casa y contrato. Un empate de bet365 no completa un 1X2 incompleto de Pinnacle.

Los precios se validan y se redondean a tres decimales, con mitad hacia arriba, para formar la clave de coincidencia. Por ejemplo, 1.8504 pasa a 1.850.

Esto es una regla propia de búsqueda de P5. No significa que toda la persistencia minera reduzca los números a tres decimales: también conserva las entradas originales y otros valores con su precisión correspondiente.

### 9.3. Buscar antecedentes

La fuente de población sigue siendo la vista materializada `mv_p5_price_memory`. Una vista materializada es una tabla de consulta preparada previamente a partir de otros registros. Su definición vigente utiliza las cuotas actuales persistidas (`current_odds`) y la hora de actualización del precio; esta corrección no la sustituye por el último snapshot prepartido.

La búsqueda exige coincidencia de deporte, casa, familia, periodo, forma de dos o tres resultados y vector de cuotas cuantizado. Aplica los filtros de población que correspondan, como competición, temporada o país.

Excluye el evento actual y considera eventos con inicio anterior al encuentro actual. Selecciona resultados y marcadores compatibles y evita contar varias veces el mismo evento. Los motivos de exclusión y sus cantidades se conservan.

La coincidencia no exige que el precio histórico se haya observado en el mismo T−5 del caso actual. El checkpoint identifica el vector actual; la búsqueda usa la clave contractual y de precios.

**Decisión vigente sobre disponibilidad del resultado:** P5 no añade una comprobación `result_available_at <= evaluation_as_of`, una verificación extra de estado deportivo ni una consulta temporal que sustituya la vista. Depende de la construcción y alimentación acordadas de esa fuente. La captura preserva qué población recibió en esta ejecución; no añade una verificación independiente de esa premisa.

### 9.4. Congelar los miembros antes de puntuar

La captura guarda los eventos históricos exactos que entraron en la muestra. La base de datos realiza esa operación directamente y después calcula los conteos sobre los miembros guardados.

El motor recibe tamaño de muestra, victorias locales, empates y victorias visitantes, además de su identidad y filtros. No necesita cargar en memoria de Python todos los partidos de una población grande.

Si la vista cambia mañana, la muestra original sigue indicando qué eventos se utilizaron hoy.

### 9.5. Cantidad mínima y dos significados de empate

Necesita al menos **tres eventos** en la muestra para un perfil válido.

Hay dos situaciones diferentes:

1. **Empate deportivo:** un partido termina empatado. Es un resultado válido dentro de un contrato de tres resultados compatible y puede incluso ser el resultado dominante.
2. **Empate de frecuencias:** dos o más resultados comparten la mayor cantidad de apariciones. Con muestra suficiente, el perfil es válido, conserva `TIE`, dirección NONE y P5 = 0.

Una muestra de dos eventos no equivale a ese cero válido: produce insuficiencia de observaciones y una señal `BLOCKED` sin score.

### 9.6. Cómo calcula un perfil con dominante único

Primero identifica qué resultado apareció más veces. Después calcula:

| Variable | Explicación |
|---|---|
| `P_hist_DOMINANT` | Frecuencia observada del resultado dominante: cantidad dominante dividida por tamaño de muestra. |
| `BASELINE` | Referencia uniforme: 1/3 para tres resultados o 1/2 para dos. |
| `HIST_EDGE` | Distancia de la frecuencia dominante respecto de esa referencia, dividida por el espacio que queda hasta 1. |
| `CONSISTENCY` | Factor calculado como 0.5 más la mitad de HIST_EDGE. |
| `SAMPLE_FACTOR` | Tamaño dividido por ocho, con máximo 1. |
| `MSRI_RAW` | Producto de HIST_EDGE, CONSISTENCY y SAMPLE_FACTOR. |
| `MSRI_SIGNAL` | Conversión de MSRI_RAW a uno de los escalones definidos. |
| `SAMPLE_WEIGHT` | Peso adicional por tramo de tamaño de muestra. |
| `P5` | MSRI_SIGNAL multiplicado por SAMPLE_WEIGHT. |

Los escalones de MSRI_RAW son: menos de 0.10 → 0; desde 0.10 → 0.25; desde 0.20 → 0.50; desde 0.40 → 0.75; desde 0.60 → 1.00.

El peso adicional de muestra es 0.40 para tres o cuatro eventos; 0.60 para cinco a siete; 0.80 para ocho a doce; 1.00 desde trece.

El tamaño interviene dos veces: una atenuación continua antes del escalón y un peso por tramos después. Son operaciones diferentes del motor actual.

### 9.7. Ejemplo completo

Supongamos seis eventos coincidentes en un 1X2 de Pinnacle: cuatro victorias locales, un empate y una victoria visitante.

1. La muestra es suficiente: seis supera el mínimo de tres.
2. El dominante único es HOME, con cuatro resultados.
3. Su frecuencia histórica es 4/6, aproximadamente 0.6667.
4. La referencia uniforme es 1/3.
5. HIST_EDGE resulta 0.50.
6. CONSISTENCY resulta 0.75.
7. SAMPLE_FACTOR es 6/8 = 0.75.
8. MSRI_RAW es 0.50 × 0.75 × 0.75 = 0.28125.
9. Su escalón MSRI_SIGNAL es 0.50.
10. El peso de seis eventos es 0.60.
11. P5 es **0.30**, con dirección HOME.
12. La intensidad es `MODERATE`.

La frecuencia local observada fue aproximadamente 66.67%. La puntuación P5 fue 0.30. **P5 no significa «30% de probabilidad de ganar».**

Sus intensidades son `NONE` por debajo de 0.10; `WEAK` desde 0.10 hasta menos de 0.25; `MODERATE` desde 0.25 hasta menos de 0.50; `STRONG` desde 0.50.

### 9.8. Qué ocurre cuando las casas tienen distinta cobertura

| Situación | Resultado |
|---|---|
| Pinnacle tiene vector y muestra suficiente; bet365 no tiene vector. | Pinnacle puede calcular; bet365 conserva el bloqueo correspondiente. |
| bet365 calcula y Pinnacle no tiene suficientes coincidencias. | bet365 conserva su perfil; Pinnacle queda bloqueado por observaciones insuficientes. |
| SofaScore tiene sus propias coincidencias. | Calcula un perfil independiente con su vector y su muestra. |
| Todas carecen de señales calculables y no hubo errores. | P5 queda globalmente `INSUFFICIENT_DATA`. |
| Alguna calcula y otra sufre un error. | Se conservan ambos hechos; el estado global puede ser `ACTIVE` y el estado de ejecución reflejar errores parciales. |

No se promedian los perfiles de las casas para fabricar una puntuación general. Tampoco se mezclan sus miembros para aumentar artificialmente una muestra individual.

### 9.9. Betfair y la salida completa

Betfair conserva precios y cantidades BACK/LAY donde corresponde como `role = DIAGNOSTIC`, sin participar en el score de memoria. Su presencia no vuelve calculable una muestra insuficiente de otra casa.

P5 devuelve perfiles por casa y contrato, señales, cobertura, vector actual, conteos, dominante, factores, escalones, pesos, score, intensidad y explicaciones.

Minería mantiene además la clave de búsqueda, filtros, exclusiones, identificador de muestra y miembros exactos capturados. La muestra sigue ligada al resultado incluso si el cálculo posterior falla.

## 10. Cómo se representan resultados, bloqueos y errores

### 10.1. Estado de una señal

Una señal es una lectura concreta dentro de un pilar. Puede ser un número, una relación o una condición verdadera/falsa.

| Estado | Significado |
|---|---|
| `COMPUTED` | La lectura pudo calcularse. Cero y falso pueden ser resultados válidos. |
| `BLOCKED` | No se cumplen los requisitos para calcularla. Tiene un motivo explícito. |
| `ERROR` | Ocurrió un fallo durante su ejecución. Se conserva su explicación. |

Los motivos permiten diferenciar situaciones:

| Motivo | Ejemplo en lenguaje natural |
|---|---|
| `MISSING_INPUT` | Falta un precio o un ingrediente requerido. |
| `INCOMPATIBLE_CONTRACT` | Se intentaría comparar condiciones que no coinciden. |
| `INSUFFICIENT_OBSERVATIONS` | P5 tiene menos de tres antecedentes o una lectura temporal carece de puntos suficientes. |
| `MISSING_ENDPOINT` | No existe el punto final requerido de una trayectoria. |
| `NON_CONTIGUOUS_GAP` | Falta un punto necesario para certificar continuidad. |
| `AMBIGUOUS_CANDIDATE` | Existen candidatos y no puede identificarse una lectura única admisible. |
| `ZERO_DENOMINATOR` | La operación exigiría dividir entre cero. |
| `NOT_APPLICABLE` | La lectura no corresponde a ese caso o tipo de dato. |
| `CALCULATION_ERROR` | El cálculo produjo una excepción. |

El catálogo incluye otros motivos explícitos para circunstancias específicas. La evidencia puede añadir campos faltantes, cantidades disponibles/requeridas o referencias a contratos.

### 10.2. Estado global del pilar

| Estado | Significado |
|---|---|
| `ACTIVE` | Existe cálculo aprovechable según el contrato del ámbito. En P2–P5 basta una señal calculada, aunque otras estén bloqueadas o fallen. |
| `INSUFFICIENT_DATA` | No se obtuvo cálculo utilizable por falta de requisitos, sin el error que obligaría al estado ERROR. |
| `ERROR` | El ámbito terminó sin un resultado utilizable por un fallo de cálculo. |
| `SKIPPED` | El ámbito no participó, por ejemplo porque estaba deshabilitado. |

P1 tiene una adaptación específica desde sus módulos, capas y salida nativa al contrato minero. Sus clasificaciones internas se preservan; no se convierten en nuevos nombres de estado global.

En P2–P5 existe además un estado de ejecución que distingue finalización normal, finalización con errores parciales, fallo o no participación. Así, un `ACTIVE` no oculta errores de otras señales.

### 10.3. Diagnostics: el resumen de lo sucedido

Los diagnósticos agregan cantidad total de señales o unidades evaluadas, calculadas, bloqueadas y con error, junto con conteos por motivo. En P1 la jerarquía incluye módulos/componentes o capas, por lo que sus cantidades no representan necesariamente el mismo tipo de unidad que en P2–P5.

Ejemplo conceptual: catorce señales, cero calculadas, catorce bloqueadas; ocho por falta de entrada, cuatro por incompatibilidad contractual y dos por observaciones insuficientes.

Además se conserva cobertura: qué estaba completo, parcial, ausente, inválido, ambiguo o excluido según la política aplicable.

Un bloqueo es también un dato útil. Permite estudiar, por ejemplo, si una casa carece frecuentemente de cuotas en cierto mercado o si un deporte presenta menos cobertura.

## 11. Cómo se guarda todo para minería

### 11.1. Un expediente común y seis resultados independientes

Cada evaluación nueva genera un `evaluation_id` único, aunque se trate del mismo evento y minuto que una ejecución anterior.

Su fotografía común contiene:

- Identidad y contexto del encuentro.
- `evaluation_as_of` y tiempos de preparación/captura.
- Entradas originales compartidas y procedencia.
- Observaciones de cuotas recibidas y su disponibilidad.
- Contexto deportivo utilizado por P1.
- Selecciones y contratos de mercado.
- Configuración efectiva y versiones.

Los seis resultados se enlazan a esa fotografía. Las entradas compartidas se guardan una vez y se referencian desde cada ámbito; no es necesario repetir el expediente entero en cada señal.

### 11.2. Resultados, unidades y métricas

El almacenamiento se puede entender como un archivo con niveles:

| Nivel | Qué conserva |
|---|---|
| Evaluación | Fotografía común del encuentro y del conocimiento disponible. |
| Resultado de ámbito | Salida de P1 Side, P1 Totales o uno de P2–P5, con estado y diagnósticos. |
| Unidad | Resumen, módulo, componente, capa o señal concreta. |
| Métrica | Valor consultable de una unidad: número, texto o condición verdadera/falsa. |
| Muestra P5 | Encabezado, criterios, conteos y miembros históricos congelados. |
| Outcome posterior | Observación deportiva posterior, almacenada separadamente. |

La evidencia estructurada conserva relaciones y detalles; las métricas permiten buscar por intervalos y categorías sin tener que leer manualmente todo el expediente.

Una referencia actúa como un índice dentro del archivo: permite ir de la señal a los ingredientes compartidos o al detalle original del cálculo.

### 11.3. Qué se conserva de cada pilar

| Ámbito | Información que permite explicar su salida |
|---|---|
| P1 Side | Siete módulos, componentes, muestras, RAW, pesos, valores efectivos, ajustes, presión y decisión por reglas. |
| P1 Totales | Ventanas, completitud, referencias, escalas, capas, pesos, scores, ruptura, dominio de tendencia, composite, alineación, counts e intensidad. |
| P2 | Precios y líneas seleccionados, inversos/operaciones registradas, edges, representantes, gaps, relaciones y contratos. |
| P3 | Pares Over/Under, líneas, transformaciones, edges, estructura, representantes y comparaciones. |
| P4 | Puntos reales, tiempos, checkpoints, huecos, tramos, movimientos, transformaciones y patrones. |
| P5 | Vector, clave, filtros, miembros exactos, exclusiones, conteos, factores, pesos, escalones y perfil por casa. |

Las salidas tempranas también quedan representadas con su causa. Una clasificación final no reemplaza las variables materiales que llevaron a ella.

### 11.4. Las dimensiones evitan perder el significado

Se preservan, cuando corresponden, deporte, evento, mercado, periodo, línea/hándicap, selección, lado, casa, agrupación contractual e identificadores de origen.

También se conservan datos como `choice_group_key`, fuente, outcome de fuente y referencias de observación, snapshot o cuota cuando existen.

No toda señal tiene una única casa o línea. Una comparación entre casas referencia varios ingredientes; una lectura deportiva de P1 tiene su propio ámbito estructural. Conservar el significado incluye no inventar una dimensión única donde no corresponde.

### 11.5. Se distinguen los relojes

El sistema utiliza dos timestamps de snapshot con responsabilidades separadas:

| Tiempo | Pregunta que responde |
|---|---|
| `source_collected_at` | ¿Cuándo informó el proveedor este cambio o precio? Queda vacío si no aporta una hora. |
| `collected_at` | ¿Cuándo observó/persistió nuestro sistema el dato? Es el instante UTC real del lote, no el COMMIT exacto. |
| `evaluation_as_of` | ¿Cuándo se cerró el conjunto de conocimiento de la evaluación? Se guarda en el expediente minero. |

No se añaden columnas. Los checkpoints con fuente se reconstruyen con el último
precio del instrumento anterior o igual al objetivo. La observación limita los
inputs de la ejecución, sin sustituir la hora del cambio. SofaScore no proporciona
hora de cambio: utiliza observaciones reales cercanas al objetivo y no recupera
retrospectivamente sus precios antiguos.

Ejemplo: el partido empieza a las 18:00. A las 17:55 descargamos un tick de las
17:24 con precio 1.95. Puede representar T-30 (17:30) y T-5 (17:55), porque es el
último estado conservado previo a ambos. P4 mide **25 minutos sin cambio entre esos
checkpoints**. La vista adaptativa mantiene el cambio a las 17:24 y su cierre a
las 17:55, sin desplazar el cambio. Esto describe los ingredientes conservados,
no garantiza que nunca existieran fluctuaciones descartadas.

El histórico con fuente válida participa sin límite por ID ni reescritura.
Sus fechas originales permanecen, pero no prueban consultas reales en los
checkpoints reconstruidos. Las evaluaciones nuevas registran su captura real
actual; las originales guardadas siguen inmutables. No hay configuración especial
de transición.

### 11.6. Precisión y uso de recursos

La persistencia conserva los valores de tipo decimal exacto sin convertirlos a números de menor precisión ni imponer una escala que los recorte. PostgreSQL utiliza valores numéricos; los documentos estructurados representan los decimales exactos como texto decimal cuando corresponde.

Eso conserva la precisión que llega a la capa de guardado. No convierte retroactivamente en aritmética exacta los cálculos que un motor haya realizado con otro tipo numérico.

Las escrituras agrupan registros y usan mecanismos de carga por lotes. Las muestras P5 se capturan dentro de la base de datos. Las lecturas de muchas unidades o miembros se dividen en páginas, de modo que consultar cien miembros no limita a cien la población que participó en el cálculo.

### 11.7. Guardado obligatorio y transacción

Una **transacción** es un conjunto de cambios que se confirma completo o se revierte completo.

La evaluación, los seis ámbitos, sus unidades, métricas y muestras P5 pertenecen al mismo guardado conjunto. El coordinador comprueba que estén los seis ámbitos antes de completar la ejecución.

Si falla un cálculo, puede quedar representado como `ERROR` y guardarse junto con los otros resultados. Si falla la infraestructura de persistencia, la evaluación se revierte y el fallo se propaga al coordinador. Son situaciones diferentes.

Por tanto, una ejecución instrumentada no se considera completa solo porque haya calculado números en memoria. Debe confirmar su persistencia.

### 11.8. Inmutabilidad e idempotencia

**Inmutable** significa que el registro original no se actualiza ni se elimina para sustituirlo por una interpretación posterior. La base de datos aplica protecciones de escritura a los registros históricos correspondientes.

Reenviar el mismo identificador con exactamente el mismo contenido es idempotente: no crea una versión contradictoria. Reenviarlo con contenido distinto produce un conflicto, sin sobrescribir la evaluación.

Volver a evaluar el evento genera otra identidad. Las dos ejecuciones pueden compararse después.

El histórico conserva contexto y datos propios, evitando que un cambio o eliminación de entidades operativas borre en cascada la evaluación original.

### 11.9. Versiones: saber qué reglas estaban activas

La evaluación registra versión de esquema y contrato, configuración capturada, huella de configuración y huella de código efectivo. Cada ámbito conserva su versión de motor.

También incluye el identificador de commit cuando Git está disponible, versión de Python y huellas de archivos de dependencias. La huella de código distingue modificaciones locales que todavía no formen parte de un commit.

La configuración capturada incluye las constantes serializables de cálculo y opciones relevantes del recorrido, además de los pilares habilitados. Las reglas escritas directamente en el código quedan identificadas por su huella y versión.

Esto permite relacionar una puntuación con las reglas y el código que la produjeron. Para volver a ejecutar exactamente esa lógica también hace falta conservar o recuperar los archivos y el entorno correspondientes; una huella identifica contenido, no contiene por sí sola una copia del código.

### 11.10. Los logs `🏁`

Después de confirmar la persistencia, el recorrido emite resúmenes finales para los seis ámbitos. Identifican evaluación, evento, participantes, corte temporal, estado, valores finales y motivos disponibles.

P5 muestra perfiles por casa; P2–P4 muestran lecturas por mercado o serie. También aparecen ámbitos deshabilitados o sin cálculo.

Los logs facilitan la revisión humana. La reconstrucción detallada utiliza los datos persistidos, porque el resumen de consola no repite todas las entradas, trazas y muestras.

## 12. Reconstrucción y asociación con el resultado deportivo

### 12.1. Qué se puede recuperar

Una evaluación persistida permite recorrer:

> Encuentro → conocimiento capturado → contratos y observaciones → variables y componentes → señales y motivos → salida final → configuración y versiones.

Los lectores recuperan la fotografía y los seis resultados. El detalle de unidades, métricas y miembros P5 puede requerir varias páginas para completar todo el recorrido.

Reconstruir el caso no exige volver a consultar la vista P5 ni una API deportiva actual. Utiliza las entradas, muestras y resultados conservados.

### 12.2. El outcome es una pieza separada

El **outcome** es la información deportiva conocida posteriormente: marcador, ganador y desgloses disponibles por periodos o sets, con procedencia y tiempos.

Se guarda como una observación aparte. Una corrección posterior crea otra observación; no cambia la fotografía de los pilares.

Por ejemplo, si una evaluación se guardó antes del encuentro y al día siguiente se conoce un 2–1, ese 2–1 puede asociarse al caso sin incorporarlo a las entradas que supuestamente conocían los pilares antes del inicio.

La infraestructura de outcomes debe ser alimentada con esas observaciones. Una evaluación puede existir sin outcome todavía; guardar pilares no garantiza que ya se haya incorporado el resultado posterior.

### 12.3. Qué hace la unión actual

El lector de caso minable reúne la evaluación original y la observación de outcome más reciente disponible. El historial de observaciones y correcciones puede recorrerse aparte.

La unión por evento prepara la investigación, pero no resuelve automáticamente todos los contratos de apuestas. Determinar victoria, pérdida, push, resolución de hándicap o Over/Under exige aplicar después las condiciones exactas del mercado.

Calcular ROI, yield, tasa de acierto o calibración pertenece a la capa de minería e investigación. No se deduce solamente de haber guardado un score.

## 13. Ejemplo de una ejecución completa

Imagine un partido programado a las 20:00. El recorrido lo evalúa alrededor de T−5.

1. **Verifica elegibilidad.** El partido y la competición cumplen los filtros del recorrido.
2. **Prepara P1.** Reúne historia deportiva y contexto de liga. Si falta algo, conserva la causa de preparación.
3. **Organiza cuotas.** Identifica precios, casas, mercados, periodos y disponibilidad de cada observación.
4. **Selecciona mercado y momento.** Escoge el checkpoint utilizable y el periodo de tiempo completo conforme a la política.
5. **Captura P5.** Pinnacle tiene seis coincidencias; bet365 dos; SofaScore no tiene vector completo. Las muestras adquiridas se conservan con sus miembros.
6. **Cierra la adquisición.** Fija, por ejemplo, `evaluation_as_of = 19:55:18 UTC`, usando un horario ilustrativo expresado en UTC.
7. **Calcula los ámbitos.** P1 Side produce su perfil; P1 Totales produce capas y composite; P2/P3 calculan las señales que permiten sus entradas; P4 identifica recorridos y huecos; P5 calcula Pinnacle y conserva los bloqueos de las otras lecturas.
8. **Guarda conjuntamente.** Fotografía, seis ámbitos, unidades, métricas y muestras se confirman en una transacción.
9. **Muestra los resúmenes.** Los bloques `🏁` permiten revisar resultados y motivos con la identidad de evaluación.
10. **Asocia el resultado posterior.** Cuando se incorpore el outcome, la capa de lectura puede reunir ambas piezas sin modificar el expediente original.

Si bet365 no pudo calcular en el paso 7, esa ausencia permanece. Que mañana haya más información no convierte aquella señal bloqueada en una señal calculada.

## 14. Qué significa simular una ejecución

El script `simulate_pre_start_check.py` permite seleccionar eventos, un minuto simulado y qué recorridos o proveedores se activan. Los argumentos explícitos del comando prevalecen sobre los valores predeterminados editables del script.

Los interruptores de pipeline y de pilares individuales tienen funciones diferentes. Activar el pipeline permite llegar a los pilares; los interruptores individuales deciden cuáles participan, sujeto al filtro efectivo. Si se desactiva el filtro personalizado de pilares, se utilizan las opciones productivas correspondientes.

Desactivar la ingestión evita ejecutar esa recogida de cuotas. No significa que todos los demás recorridos carezcan de consultas externas: preparar contexto deportivo de P1 puede requerirlas.

El simulador utiliza filtros y mecanismos productivos, por lo que un evento puede quedar excluido antes del cálculo. Los logs de configuración y de elegibilidad ayudan a entenderlo.

**Simular T−5 de un partido antiguo hoy no recupera automáticamente lo que se sabía antes de aquel encuentro.** El minuto simulado selecciona el contexto de la prueba, pero la adquisición actual puede encontrar clasificaciones, resultados o muestras que hoy ya existen.

La evaluación nueva conserva una frontera real de adquisición actual. Para estudiar sin contaminación qué produjo el sistema en una fecha pasada, hay que leer una evaluación original que se hubiera capturado entonces. No existe un parámetro que convierta entradas adquiridas hoy en conocimiento histórico original.

Esta distinción también evita atribuir a P5 una garantía temporal adicional que se decidió no implementar sobre la vista.

## 15. Preguntas frecuentes

### ¿Un pilar ACTIVE significa que todo se calculó?

No. En P2–P5 puede bastar una señal calculada. Hay que mirar los conteos, la cobertura y los motivos de las demás.

### ¿Un resultado cero es igual a falta de información?

No. Cero puede ser una neutralidad calculada o un empate válido de frecuencias en P5. Una señal bloqueada conserva ausencia de valor y explicación.

### ¿P1 puede producir Under y P3 Over?

Sí. P1 Totales estudia perfil deportivo relativo a su liga; P3 estudia los precios de un contrato de totales. La diferencia queda disponible para investigación.

### ¿P4 explica por qué se movieron las cuotas?

Describe el recorrido observado. Sus movimientos, giros y concentraciones no identifican una causa ni un participante del mercado.

### ¿Se puede recuperar la población exacta de P5?

Sí, desde los miembros congelados de la muestra vinculada a la evaluación. La lectura paginada permite recorrer poblaciones grandes.

### ¿Los bloqueos se descartan para minería?

No. Se conservan para estudiar cobertura, incompatibilidades e insuficiencia de observaciones. Los pilares deshabilitados también quedan representados cuando existe evaluación.

### ¿Se recalculan los pilares cuando llega el marcador final?

El outcome se asocia separadamente. No se recalcula ni se sobrescribe la evaluación original para incorporar ese conocimiento posterior.

### ¿La infraestructura ya decide qué señales son rentables?

No. Conserva lo necesario para investigarlo. La validación de hipótesis, resolución de contratos y medición de rendimiento corresponden al trabajo posterior de minería.

## 16. Referencias para localizar la implementación

Estas rutas permiten a una persona técnica contrastar o ampliar el informe; no es necesario leerlas para comprender las secciones anteriores.

| Parte | Archivo o directorio principal |
|---|---|
| Coordinación y cierre de evaluación | `modules/jobs/pre_start_check_job/pillar_pipeline.py` |
| Resúmenes finales | `modules/jobs/pre_start_check_job/pillar_result_logging.py` |
| Contexto y organización de cuotas | `modules/pillars/context.py`, `modules/pillars/odds_trajectory_context.py` |
| Selección temporal y contratos | `modules/pillars/trajectory_selection.py`, `modules/pillars/market_evaluation.py` |
| P1 Side y siete módulos | `modules/pillars/pillar_1_team_structure/side/side.py`, directorios `module_1` a `module_7` |
| P1 Totales | `modules/pillars/pillar_1_team_structure/totals/totals.py` |
| P2 | `modules/pillars/pillar_2_side_market/` |
| P3 | `modules/pillars/pillar_3_totals_market_context/` |
| P4 | `modules/pillars/pillar_4/` |
| P5: preparación y cálculo | `modules/pillars/pillar_5/run_pillar_5.py`, `memory_sample.py`, `memory_score.py` |
| Población y muestras P5 | `infrastructure/persistence/repositories/pillar_5_price_memory_repository.py` |
| Estados y diagnósticos | `modules/pillars/evaluation_contracts.py`, `modules/pillars/instrumentation.py` |
| Adaptación de resultados a minería | `modules/pillars/mining/adapters/` |
| Configuración y huellas | `modules/pillars/mining/versioning.py` |
| Escritura y lectura histórica | `infrastructure/persistence/repositories/pillar_mining_repository.py` |
| Contrato técnico de persistencia | `docs/pillars/mining-persistence.md` |
| Simulación | `scripts/development/simulate_pre_start_check.py`, `docs/jobs/pre-start-simulator.md` |

**Idea final:** cada resultado debe poder explicarse con el expediente que realmente se capturó, sus operaciones y sus reglas. Conservar también lo que faltó permite estudiar mañana el sistema sin cambiar lo que produjo originalmente.
