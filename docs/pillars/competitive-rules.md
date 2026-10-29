# Reglas competitivas de P1

## Responsabilidades y flujo

`competitive_rules.py` define los tipos y la puntuación compartida; `standings_policies.py` declara los criterios de clasificación por competición; `competition_profiles.py` asocia cada ID canónico con esas reglas, tamaño, duración y objetivos; `competition_history_config.py` conserva exclusivamente las identidades del proveedor y los ámbitos históricos recolectados. La lista operativa continúa en `tracked_competitions.py`. La [auditoría de clasificaciones](../competition/standings-audit.md) compara los métodos anteriores y actuales para las 22 competiciones activas y señala sus límites.

P1 resuelve el perfil una vez en `_prepare_p1`, antes del historial. `CompetitionContext` transporta los metadatos del perfil y `competitive_rules`. La construcción de tablas consulta ese mismo perfil por el ID canónico del evento, sin inferencia por deporte, nombres o temporada. El historial sigue seleccionando resultados mediante sus IDs de temporada y fechas; esos IDs no eligen otra regla deportiva.

`reference_season` y los destinos conservan información descriptiva. Se aplica el perfil vigente incluso a eventos históricos. Un ID desconocido no recibe una configuración alternativa desde la base o la API: el contexto queda sin metadatos deportivos y el cálculo de tablas informa del perfil ausente.

M1, M4, M7 y Totals leen los metadatos del contexto. M4 y M7 no reconstruyen el tamaño contando filas ni suponiendo 20. M1 conserva la comprobación de cobertura. Las tablas y resultados permanecen en `streak_analysis`.

La captura existente de `p1` incluye identidad, contexto y análisis antes del cálculo. La serialización de dataclasses incorpora las reglas y su procedencia sin añadir argumentos, duplicar el historial ni cambiar `input_refs`. No se alteran los identificadores de módulos ni componentes de minería.

## Estados y semántica

- **Ausente:** el perfil no contiene una regla de ese objetivo. No equivale a frontera desconocida.
- **Pendiente (`PENDING`):** el objetivo existe, pero falta determinar su frontera efectiva, elegibilidad o tabla decisoria. Los puestos base conocidos pueden registrarse sin autorizarlos como frontera efectiva.
- **Configurado (`CONFIGURED`):** regla deportiva confirmada en el perfil. Su cálculo todavía puede requerir soporte de M5.

Cada regla hereda la temporada de referencia y las fuentes de `CompetitionRules`; incorpora destino, temporada de destino cuando es otra competición, vía, ámbito de tabla, alcance temporal, selección, zona y referencia cuando están resueltas. Los campos vacíos expresan información pendiente; no activan valores por defecto deportivos.

Los valores canónicos son `COMPETITION_QUALIFICATION`, `STAGE_QUALIFICATION` y `DIVISION_RETENTION`. `NO_APPLICABLE_OBJECTIVE` es el resultado para un equipo fuera de las zonas de la política, siempre que ninguna regla pendiente necesaria impida decidirlo. Se mantienen `objective_type`, `points_cutoff`, `gap`, `n_league`, `base_severity`, `regime_multiplier` y `final_cost`.

## Veinte perfiles iniciales

Las fuentes se incluyen por perfil en el catálogo. Estos valores iniciales implementan la planificación acordada; no se actualizan mediante consultas durante la ejecución.

| Competición | Temporada | Equipos | Partidos por equipo | Reglas y límites | Fuentes |
|---|---|---:|---:|---|---|
| LaLiga | 2026/27 | 20 | 38 | Permanencia 18–20; seguro 17. UEFA pendiente para 2027/28. | [Fuente 1](https://www.laliga.com/noticias/equipos-que-bajan-a-segunda) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Premier League | 2026/27 | 20 | 38 | Permanencia 18–20; seguro 17. UEFA pendiente para 2027/28. | [Fuente 1](https://www.premierleague.com/en/news/4657245/202526-premier-league-relegation-faq) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Serie A | 2026/27 | 20 | 38 | Permanencia 18–20; seguro 17. Desempate excepcional sin zona fija adicional. UEFA pendiente. | [Fuente 1](https://en.legaseriea.it/serie-a/news/180-minutes-left-in-fight-for-survival) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Bundesliga | 2026/27 | 18 | 34 | Descenso 17–18 y eliminatoria 16; seguro 15. UEFA pendiente. | [Fuente 1](https://www.bundesliga.com/en/faq/what-are-the-rules-and-regulations-of-soccer/how-does-promotion-and-relegation-work-in-the-bundesliga-10645) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Ligue 1 | 2026/27 | 18 | 34 | Descenso 17–18 y eliminatoria 16; seguro 15. UEFA pendiente. | [Fuente 1](https://ligue1.com/en/articles/l1_article_3812-) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Liga Portugal | 2026/27 | 18 | 34 | Descenso 17–18 y eliminatoria 16; seguro 15. UEFA pendiente. | [Fuente 1](https://www.ligaportugal.pt/pages/sobre-a-liga) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Eredivisie | 2026/27 | 18 | 34 | Descenso 17–18 y eliminatoria 16; seguro 15. UEFA y participantes del playoff doméstico pendientes. | [Fuente 1](https://www.knvb.nl/downloads/bestand/30062/definitiefcompetitieprogrammavriendenloterijeredivisie) · [Fuente 2](https://www.uefa.com/nationalassociations/uefarankings/accesslist/allcompetitions/) |
| Saudi Pro League | 2026/27 | 18 | 34 | Descenso 16–18; seguro 15. AFC Champions League Elite / Two pendientes para 2027/28. | [Fuente 1](https://www.sportmonks.com/glossary/saudi-pro-league/) · [Fuente 2](https://www.saff.com.sa/news.php?id=4184) |
| Brasileirão | 2026 | 20 | 38 | Descenso 17–20; seguro 16. Libertadores: base 1–4 grupos y 5 preliminar, frontera efectiva pendiente. Sudamericana: seis mejores elegibles, sin intervalo fijo. | [Fuente 1](https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Brasileiro_Serie_A_2026_c984f8cf05.pdf) |
| Liga MX Apertura | 2026 | 18 | 17 | 1–8 a cuartos, sin Play-In. Sin permanencia. Concacaf Champions Cup pendiente del acumulado y otras vías. | [Fuente 1](https://www.rayados.com/es/noticias/21455/descubre-el-calendario-de-rayados-para-el-torneo-apertura-2026) · [Fuente 2](https://www.si.com/mx/liga-mx/oficial-la-liga-mx-se-queda-sin-ascenso-descenso-y-multas-2026-2027) |
| Liga MX Clausura | 2026 | 18 | 17 | 1–8 a cuartos, sin Play-In. Sin permanencia. Concacaf pendiente. Solo Clausura 2026, sin extrapolación a 2027. | [Fuente 1](https://www.rayados.com/es/noticias/21455/descubre-el-calendario-de-rayados-para-el-torneo-apertura-2026) · [Fuente 2](https://fmf.mx/docs/reglamentos/458.pdf) |
| Liga Profesional de Argentina Apertura | 2026 | 30 | 16 | Dos grupos de 15; 1–8 de cada grupo a octavos. Permanencia anual/promedios e internacional pendientes. | [Fuente 1](https://www.ligaprofesional.ar/wp-content/uploads/2026/01/Reglamento-Torneos-LPF-Primera-2026-1.pdf) |
| Liga Profesional de Argentina Clausura | 2026 | 30 | 16 | Dos grupos de 15; 1–8 de cada grupo a octavos. Permanencia anual/promedios e internacional pendientes. | [Fuente 1](https://www.ligaprofesional.ar/wp-content/uploads/2026/01/Reglamento-Torneos-LPF-Primera-2026-1.pdf) |
| NBA | 2026/27 | 30 | 82 | Conferencia: 1–6 playoffs y 7–10 Play-In. Sin permanencia ni objetivo internacional. Cálculo por porcentaje no soportado. | [Fuente 1](https://www.nba.com/news/nba-play-in-tournament) · [Fuente 2](https://www.nba.com/news/2026-27-nba-regular-season-schedule) |
| Euroleague | 2026/27 | 20 | 38 | Tabla general: 1–6 playoffs y 7–10 Play-In. Sin permanencia ni objetivo internacional; unidad de cálculo: victoria. | [Fuente 1](https://mediacentre.euroleague.net/en/app/2/communication/communication/preview/24407) |
| MLB | 2026 | 30 | 162 | Por liga: tres campeones divisionales y tres Wild Cards. Sin corte global 1–6. Porcentajes y selección compuesta pendientes de soporte. | [Fuente 1](https://www.mlb.com/news/mlb-playoff-format-faq) |
| NFL | 2026 | 32 | 17 | Por conferencia: cuatro campeones divisionales y tres Wild Cards. Sin corte global 1–7. Porcentajes y selección compuesta pendientes de soporte; empate 0.5. | [Fuente 1](https://operations.nfl.com/media/nppjkdp1/2023-record-and-fact-book.pdf) |
| SHL | 2026/27 | 14 | 52 | 1–6 cuartos; 7–10 ronda previa; 13–14 permanencia, seguro 12. CHL pendiente. | [Fuente 1](https://www.swehockey.se/toppnyheter/het-premiaerloerdag-foer-shl/) · [Fuente 2](https://www.chl.hockey/en/about-us/chl-format) |
| Liiga | 2026/27 | 17 | 64 | 1–12 playoffs; accesos internos pendientes. 13–16 permanencia y 17 descenso; seguro 12. CHL pendiente. | [Fuente 1](https://hc.tps.fi/fi-fi/article/uutiset/liiga-tiedottaa-uusi-1410-mallinen-sarjamuoto-kaynnistyy-kaudella-202728/1573/) · [Fuente 2](https://www.chl.hockey/en/about-us/chl-format) |
| NHL | 2026/27 | 32 | 84 | tres primeros por división y dos Wild Cards por conferencia. Sin permanencia ni internacional. Wild Cards sin soporte; 84 partidos. | [Fuente 1](https://www.nhl.com/news/nhl-stats-pack-2026-27-regular-season-schedule) · [Fuente 2](https://www.nhl.com/info/standings-info/playoff-format) |

Argentina conserva separados los alcances: torneo de 16 partidos y tabla de grupo para fase; acumulado anual de 32 partidos para clasificación internacional y un descenso; registro multitemporada para promedios. El descendido por promedios se excluye al resolver el descenso anual. Ninguna de estas fronteras se deduce de los últimos puestos de un grupo.

Las reglas de acceso internacional europeo obtenidas en 2026/27 apuntan a 2027/28; la lista de acceso de la temporada en curso no determina automáticamente esas plazas. Brasil conserva expresamente la selección de seis elegibles para Sudamericana. Las reasignaciones, campeones y exclusiones siguen pendientes.

Las identidades incorporadas se comprobaron mediante consulta de solo lectura a `competitions`: Portugal, canónica 261, SofaScore 238/52; Argentina Apertura, canónica 2476, SofaScore 155/143625; Clausura, canónica 310, SofaScore 155/156467. No se habilitan competiciones comentadas por tener un perfil.

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

Los regímenes y el diagnóstico por equipo se conservan en `raw`, junto con la regla seleccionada, las fuentes, la temporada, la unidad y el estado. `NO_APPLICABLE_OBJECTIVE` con estado `ACTIVE` distingue presión calculada cero de información insuficiente.

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

`standings` contiene una política propia de la competición, con métrica principal, criterios ordenados, fuentes y confirmaciones pendientes. La puntuación se mantiene separada. Las políticas pueden distinguir tablas provisionales y finales y ejecutar minitablas o los procedimientos de NBA, MLB y NFL. `grouping_method` selecciona tabla general, conferencia, liga, división o grupo, y determina si el contexto describe una tabla única o varias. No hay un interruptor global que cambie esa agrupación.

Ejemplo Serie A: el perfil 88 utiliza `FOOTBALL_SCORING` y la política `SERIE_A`. Una victoria y un empate producen 4 puntos tanto en la tabla general como en una minitabla. Tres partidos restantes representan un máximo de 9 puntos para M5. Cambiar la puntuación compartida cambia los tres cálculos; cambiar el orden de desempate no cambia cuántos puntos concede una victoria.

Los perfiles de MLS (76), AHL (554) y CBA (174) se detallan en `docs/pillars/mls-ahl-cba-profiles.md`, incluyendo sus fuentes y verificaciones pendientes. WNBA (145) y PFL (5151) conservan objetivos sin configurar (`objectives=None`), que no equivalen a objetivos ausentes.

La agrupación argentina está declarada como grupos. Si los datos no permiten asignar un equipo a su grupo, queda identificado como `UNKNOWN`; no se inventa su pertenencia. M5 exige los grupos y poblaciones correctos para operar. Porcentajes, promedios y Wild Cards siguen teniendo las limitaciones de cálculo documentadas.
