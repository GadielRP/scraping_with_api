# Reglas competitivas de P1

## Responsabilidades y flujo

`competitive_rules.py` define los tipos y la puntuación compartida; `standings_policies.py` declara los criterios de clasificación por competición; `competition_profiles.py` asocia cada ID canónico con esas reglas, tamaño, duración y objetivos; `competition_history_config.py` conserva exclusivamente las identidades del proveedor y los ámbitos históricos recolectados. La lista operativa continúa en `tracked_competitions.py`.

P1 resuelve el perfil una vez en `_prepare_p1`, antes del historial. `CompetitionContext` transporta los metadatos del perfil y `competitive_rules`. La construcción de tablas consulta ese mismo perfil por el ID canónico del evento, sin inferencia por deporte, nombres o temporada. El historial sigue seleccionando resultados mediante sus IDs de temporada y fechas; esos IDs no eligen otra regla deportiva.

`reference_season` y los destinos conservan información descriptiva. Se aplica el perfil vigente incluso a eventos históricos. Un ID desconocido no recibe una configuración alternativa desde la base o la API: el contexto queda sin metadatos deportivos y el cálculo de tablas informa del perfil ausente.

M1, M4, M7 y Totals leen los metadatos del contexto. M4 y M7 no reconstruyen el tamaño contando filas ni suponiendo 20. M1 conserva la comprobación de cobertura. Las tablas y resultados permanecen en `streak_analysis`.

La captura existente de `p1` incluye identidad, contexto y análisis antes del cálculo. La serialización de dataclasses incorpora las reglas y sus términos canónicos sin añadir argumentos, duplicar el historial ni cambiar `input_refs`. No se alteran los identificadores de módulos ni componentes de minería.

## Estados y semántica

- **Ausente:** el perfil no contiene una regla de ese objetivo. No equivale a frontera desconocida.
- **Pendiente (`PENDING`):** el objetivo existe, pero falta determinar su frontera efectiva, elegibilidad o tabla decisoria. Los puestos base conocidos pueden registrarse sin autorizarlos como frontera efectiva.
- **Configurado (`CONFIGURED`):** regla deportiva confirmada en el perfil. Su cálculo todavía puede requerir soporte de M5.

Cada regla hereda la temporada de referencia de `CompetitionRules`; incorpora destino, temporada de destino cuando es otra competición, vía, ámbito de tabla, alcance temporal, selección, zona y referencia cuando están resueltas. Los campos vacíos expresan información pendiente; no activan valores por defecto deportivos.

Los valores canónicos son `COMPETITION_QUALIFICATION`, `STAGE_QUALIFICATION` y `DIVISION_RETENTION`. `NO_APPLICABLE_OBJECTIVE` es el resultado para un equipo fuera de las zonas de la política, siempre que ninguna regla pendiente necesaria impida decidirlo. Se mantienen `objective_type`, `points_cutoff`, `gap`, `n_league`, `base_severity`, `regime_multiplier` y `final_cost`.

## Catálogo y revisión deportiva

Los 25 perfiles se conservan en `competition_profiles.py`. La migración de vocabulario mantiene todos los rangos, cupos, tamaños, puntuaciones y temporadas de referencia actuales. No certifica su exactitud deportiva. Se retiraron las fuentes anteriores y la investigación de fronteras queda para una revisión posterior del vocabulario.

## Cálculo y diagnósticos de M5

Se conserva la activación al 80 %, con partidos regulares restantes, y un solo objetivo por equipo seleccionado por zona. No se suman objetivos; título y siembra quedan fuera. La severidad es 0.70 para otra competición, 0.85 para otra fase y 1.00 para permanencia. La clasificación conserva multiplicador 1.00; descenso directo y eliminatoria de permanencia usan 1.25. Un equipo sin objetivo aplicable aporta cero.

Las fórmulas de corte, distancia, urgencia y comparación relativa permanecen iguales. Los cortes usan la zona y referencia de la regla en lugar de deducirse de `n_league`. El máximo restante se deriva de `profile.scoring.maximum_per_game`: la misma regla que calcula los puntos de las tablas generales y los desempates. NBA, MLB y NFL registran máximo 1 y NFL empate 0.5, pero no se calculan por aproximación sus tablas ordenadas por porcentaje.

Una zona confirmada de permanencia se selecciona antes de las reglas internacionales pendientes. Cuando cualquiera de los dos equipos necesita información no resuelta, ambos costes operativos se anulan, el estado es `INSUFFICIENT_DATA` y el lado es `NONE`; los demás módulos continúan.

| Motivo | Significado |
|---|---|
| `MISSING_COMPETITION_RULES` | No hay perfil vigente para el ID canónico del evento. |
| `UNRESOLVED_OBJECTIVE_BOUNDARY` | Falta una frontera efectiva, tabla anual, promedios o reasignaciones necesarias. |
| `UNSUPPORTED_RANKING_METRIC` | El orden depende de porcentaje; no se sustituye por puntos o victorias. |
| `UNSUPPORTED_OBJECTIVE_SELECTION` | La selección requiere campeones divisionales, Wild Cards o elegibilidad compuesta. |
| `MISSING_RANKING_GROUP` / `INCOMPLETE_RANKING_GROUP` | No se identifica la tabla de grupo/división/conferencia o no tiene la población prevista con rangos compatibles. |
| `UNSUPPORTED_AGGREGATION_SCOPE` | La regla configurada exige otra tabla temporal que M5 no puede calcular. |

Los regímenes y el diagnóstico por equipo se conservan en `raw`, junto con la regla seleccionada, la temporada, la unidad y el estado. `NO_APPLICABLE_OBJECTIVE` con estado `ACTIVE` distingue presión calculada cero de información insuficiente.

## Límites y mantenimiento

Los perfiles se seleccionan únicamente por ID canónico. Un ID desconocido o ausente recibe `competitive_rules=None` y metadatos deportivos sin resolver; no se intenta adivinar el perfil mediante nombres, caché ni datos del proveedor. Para un ID conocido se aplica siempre su configuración vigente, incluido NHL con 84 partidos, independientemente del texto de temporada.

No se añaden adquisiciones, migraciones ni reconstrucciones históricas. El filtro de temporada regular y la lista operativa permanecen vigentes. Las tablas por grupos solo pueden calcularse si el análisis existente expone identificador de grupo, posiciones locales y población completa; no se fabrican a partir de una tabla general. Las rutas internas de playoffs de Liiga quedan pendientes de mayor detalle reglamentario.

Para actualizar la configuración vigente se modifica el `CompetitionRules` asociado al ID canónico en `competition_profiles.py`. Los campos y fórmulas generales de resultados y los contratos de minería conservan su forma.

## Verificación

Las pruebas de paridad usan una política sintética explícita equivalente a la anterior. Las pruebas de perfiles cubren las identidades, permanencia con/sin eliminatoria, ligas pequeñas, ausencia de descenso, clasificación de fase, separación de grupos, fronteras pendientes, porcentajes, Wild Cards, independencia de nombres de temporada y resolución antes del historial con metadatos completos. Las pruebas existentes de instrumentación verifican captura inmutable y contratos de minería.

## Ejemplo comprobado: evento 447123, Parma–Udinese

El archivo guardado identifica Serie A con `competition_id=88` y la temporada como `Serie A 26/27`. Antes, la lectura de esa etiqueta dejaba el perfil vacío. Ahora se busca directamente `COMPETITION_PROFILES[88]`: el contexto recibe 20 equipos, 38 partidos y las reglas de Serie A, con procedencia `competition_rules`.

La comprobación se realizó leyendo los JSON existentes, resolviendo un contexto nuevo y calculando Side y Totals en memoria. No se reescribieron los JSON ni las evaluaciones previas. M4 y M7 utilizan 20 desde el contexto. M5 recibe el perfil y sus 3 puntos por victoria; permanece inactivo porque la referencia de 9 partidos de 38 representa un 23.68 %, inferior al 80 %. Side conserva `NO_BET`.

## Puntuación, clasificación y agrupación compartidas

`ScoringRules` define una sola vez los puntos de victoria regular, victoria en prórroga, empate, derrota en prórroga y derrota regular. La tabla general y las minitablas de desempate usan `scoring.points(stats)`; M5 usa `scoring.maximum_per_game` para el horizonte restante. No existe otro `points_per_win` configurable. El campo de diagnóstico de M5 conserva ese nombre como resultado derivado.

`standings` contiene una política propia de la competición, con métrica principal, criterios ordenados y confirmaciones pendientes. La puntuación se mantiene separada. Las políticas pueden distinguir tablas provisionales y finales y ejecutar minitablas o los procedimientos de NBA, MLB y NFL. `grouping_method` selecciona tabla general, conferencia, liga, división o grupo, y determina si el contexto describe una tabla única o varias. No hay un interruptor global que cambie esa agrupación.

Ejemplo Serie A: el perfil 88 utiliza `FOOTBALL_SCORING` y la política `SERIE_A`. Una victoria y un empate producen 4 puntos tanto en la tabla general como en una minitabla. Tres partidos restantes representan un máximo de 9 puntos para M5. Cambiar la puntuación compartida cambia los tres cálculos; cambiar el orden de desempate no cambia cuántos puntos concede una victoria.

M5 calcula los partidos y puntos restantes por participante, utilizando sus propios partidos jugados. El avance global de la liga solo determina la ventana de temporada madura; un tercero que haya terminado su calendario no cierra la ventana de ambos participantes. Los diagnósticos `games_remaining` y `max_points_remaining` contienen las claves `home` y `away`, y cada equipo incluye también sus propios valores.

El flujo propaga `debug_mode` al motor de tablas como `send_debug_standings`. Cuando está activo, la tabla se escribe en el log tanto en cálculos nuevos como en lecturas de caché. Este reporte del motor no envía mensajes de Telegram.

Todos los perfiles comparten los tipos de `rule_terms.py`. PFL conserva `objectives=None`; WNBA conserva sus reglas actuales hasta la revisión deportiva. Ninguna de estas decisiones implica validar sus fronteras.

La agrupación argentina está declarada como grupos. Si los datos no permiten asignar un equipo a su grupo, queda identificado como `UNKNOWN`; no se inventa su pertenencia. M5 exige los grupos y poblaciones correctos para operar. Porcentajes, promedios y Wild Cards siguen teniendo las limitaciones de cálculo documentadas.

## Identidades y términos canónicos

[`competition_ids.py`](../../modules/competition/competition_ids.py) reúne referencias legibles a `competitions.competition_id` y sus nombres. Distingue registros generales y de previa: una clasificación a la previa usa el ID de esa fila, no un ID del proveedor. Los perfiles y el seguimiento usan estas constantes; la pertenencia a seguimiento sigue siendo independiente.

[`rule_terms.py`](../../modules/competition/rule_terms.py) documenta los campos y el vocabulario general: objetivos, vías, estados, selección, ámbitos, agrupación y métrica. `CompetitionStage` contiene únicamente `PLAYOFFS` y `PLAY_IN`. Cuartos, octavos, primera ronda y nombres comerciales se describen en `note` y no se interpretan para calcular resultados.

`target` se eliminó. `COMPETITION_QUALIFICATION` usa `destination_competition_id`, sin fase interna; `STAGE_QUALIFICATION` usa `destination_stage`, sin competición externa; `DIVISION_RETENTION` no usa ninguno. Las dataclasses rechazan combinaciones contradictorias, vías incompatibles y reglas configuradas por rango sin puestos.

`destination_competition_name` es un campo de la dataclass, configurado explícitamente en cada regla de clasificación externa. Puede nombrar un destino pendiente cuyo ID aún se desconoce. El ID sigue siendo la identidad de la competición; el nombre es descriptivo y no se interpreta como lógica. `dataclasses.asdict` incluye el nombre automáticamente, también en el diagnóstico de M5.

Una previa de Champions se configura con `competition_ids.UEFA_CHAMPIONS_LEAGUE_QUALIFICATION` y `QualificationPath.QUALIFYING_ROUND`. El ID identifica la fila concreta; la vía conserva el significado deportivo general. Acceder al propio Play-In usa fase `PLAY_IN` y vía `DIRECT`.

Una vía desconocida se representa con `None` únicamente en una regla `PENDING`. También puede faltar el ID de destino en una clasificación externa pendiente cuando no exista una fila general apropiada. No se sustituye por el ID de un grupo o de eliminatorias: el comentario y la nota registran qué identidad falta. Las otras reglas configuradas necesitan un ID positivo de destino.

Las referencias generales de AFC Champions League Two, Libertadores, Sudamericana y FIBA Intercontinental Cup quedaron pendientes porque el catálogo consultado solo contiene filas de grupos, previas o eliminatorias. Se conserva la previa de Libertadores mediante su ID específico. Este ajuste no modifica los rangos ni las cuotas; tres reglas previamente configuradas pasan a pendientes por la identidad ausente.

El catálogo construye `CompetitionRules` directamente. Los constructores de objetivos completan tipo y puesto de referencia. El constructor externo se llama `create_competition_qualification_rule`; no hay un wrapper para generar tuplas de reglas pendientes. Copa del Rey de fútbol (448) y EFL Cup (391) quedan identificadas para definir sus asignaciones después de investigar las condiciones deportivas. No se implementa una política de reasignación supuesta.
