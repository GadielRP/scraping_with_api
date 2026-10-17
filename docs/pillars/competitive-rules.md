# Reglas competitivas de P1

## Responsabilidades y flujo

`tracked_competitions.py` decide qué competiciones procesa el trabajo previo al partido. `league_config.py` mantiene identidades, configuración de tablas, temporadas recolectadas y perfiles deportivos por temporada. `competitive_rules.py` define los datos inmutables de esos perfiles; no decide presión ni adquisición.

`resolve_p1_competition_context(event_identity, competition_context)` selecciona el perfil por identidad exacta del proveedor y etiqueta explícita de temporada. Se ejecuta una vez en `_prepare_p1`, después de resolver los metadatos existentes y antes del historial. No hace consultas externas. Devuelve `CompetitionContext` enriquecido y corrige tamaño, duración y agrupación para la temporada coincidente aunque la caché esté completa. El historial y las tablas permanecen en `streak_analysis`.

Se conservan las firmas actuales de los calculadores y las políticas de selección de fuentes y tamaño de M1, M4, M7 y Totals. La configuración sigue usando `standings_method` para construir y ordenar las tablas; el perfil declara su unidad y máximo incremento por partido. `WIN_PERCENTAGE` no se interpreta como una unidad aditiva aunque el registro contenga victorias en `points`.

La captura existente de `p1` incluye identidad, contexto y análisis antes del cálculo. La serialización de dataclasses incorpora las reglas y su procedencia sin añadir argumentos, duplicar el historial ni cambiar `input_refs`. No se alteran los identificadores de módulos ni componentes de minería.

## Estados y semántica

- **Ausente:** el perfil no contiene una regla de ese objetivo. No equivale a frontera desconocida.
- **Pendiente (`PENDING`):** el objetivo existe, pero falta determinar su frontera efectiva, elegibilidad o tabla decisoria. Los puestos base conocidos pueden registrarse sin autorizarlos como frontera efectiva.
- **Configurado (`CONFIGURED`):** regla deportiva confirmada en el perfil. Su cálculo todavía puede requerir soporte de M5.

Cada regla hereda temporada doméstica y fuentes de `CompetitionRules`; incorpora destino, temporada de destino cuando es otra competición, vía, ámbito de tabla, alcance temporal, selección, zona y referencia cuando están resueltas. Los campos vacíos expresan información pendiente; no activan valores por defecto deportivos.

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
| SHL | 2026/27 | 14 | 52 | Comentada: 1–6 cuartos; 7–10 ronda previa; 13–14 permanencia, seguro 12. CHL pendiente. | [Fuente 1](https://www.swehockey.se/toppnyheter/het-premiaerloerdag-foer-shl/) · [Fuente 2](https://www.chl.hockey/en/about-us/chl-format) |
| Liiga | 2026/27 | 17 | 64 | Comentada: 1–12 playoffs; accesos internos pendientes. 13–16 permanencia y 17 descenso; seguro 12. CHL pendiente. | [Fuente 1](https://hc.tps.fi/fi-fi/article/uutiset/liiga-tiedottaa-uusi-1410-mallinen-sarjamuoto-kaynnistyy-kaudella-202728/1573/) · [Fuente 2](https://www.chl.hockey/en/about-us/chl-format) |
| NHL | 2026/27 | 32 | 84 | Comentada: tres primeros por división y dos Wild Cards por conferencia. Sin permanencia ni internacional. Wild Cards sin soporte; 84 partidos. | [Fuente 1](https://www.nhl.com/news/nhl-stats-pack-2026-27-regular-season-schedule) · [Fuente 2](https://www.nhl.com/info/standings-info/playoff-format) |

Argentina conserva separados los alcances: torneo de 16 partidos y tabla de grupo para fase; acumulado anual de 32 partidos para clasificación internacional y un descenso; registro multitemporada para promedios. El descendido por promedios se excluye al resolver el descenso anual. Ninguna de estas fronteras se deduce de los últimos puestos de un grupo.

Las reglas de acceso internacional europeo obtenidas en 2026/27 apuntan a 2027/28; la lista de acceso de la temporada en curso no determina automáticamente esas plazas. Brasil conserva expresamente la selección de seis elegibles para Sudamericana. Las reasignaciones, campeones y exclusiones siguen pendientes.

Las identidades incorporadas se comprobaron mediante consulta de solo lectura a `competitions`: Portugal, canónica 261, SofaScore 238/52; Argentina Apertura, canónica 2476, SofaScore 155/143625; Clausura, canónica 310, SofaScore 155/156467. No se habilitan competiciones comentadas por tener un perfil.

## Cálculo y diagnósticos de M5

Se conserva la activación al 80 %, con partidos regulares restantes, y un solo objetivo por equipo seleccionado por zona. No se suman objetivos; título y siembra quedan fuera. La severidad es 0.70 para otra competición, 0.85 para otra fase y 1.00 para permanencia. La clasificación conserva multiplicador 1.00; descenso directo y eliminatoria de permanencia usan 1.25. Un equipo sin objetivo aplicable aporta cero.

Las fórmulas de corte, distancia, urgencia y comparación relativa permanecen iguales. Los cortes usan la zona y referencia de la regla en lugar de deducirse de `n_league`. El máximo restante usa 3 puntos en fútbol/SHL/Liiga, 2 en NHL y 1 victoria en EuroLeague. NBA, MLB y NFL registran máximo 1 y NFL empate 0.5, pero no se calculan por aproximación sus tablas ordenadas por porcentaje.

Una zona confirmada de permanencia se selecciona antes de las reglas internacionales pendientes. Cuando cualquiera de los dos equipos necesita información no resuelta, ambos costes operativos se anulan, el estado es `INSUFFICIENT_DATA` y el lado es `NONE`; los demás módulos continúan.

| Motivo | Significado |
|---|---|
| `MISSING_COMPETITION_RULES` | No hay perfil de identidad/temporada resuelto. |
| `COMPETITION_RULES_SEASON_MISMATCH` | El contexto contiene un perfil ajeno a la temporada explícita. |
| `UNRESOLVED_OBJECTIVE_BOUNDARY` | Falta una frontera efectiva, tabla anual, promedios o reasignaciones necesarias. |
| `UNSUPPORTED_RANKING_METRIC` | El orden depende de porcentaje; no se sustituye por puntos o victorias. |
| `UNSUPPORTED_OBJECTIVE_SELECTION` | La selección requiere campeones divisionales, Wild Cards o elegibilidad compuesta. |
| `MISSING_RANKING_GROUP` / `INCOMPLETE_RANKING_GROUP` | No se identifica la tabla de grupo/división/conferencia o no tiene la población prevista con rangos compatibles. |
| `UNSUPPORTED_AGGREGATION_SCOPE` | La regla configurada exige otra tabla temporal que M5 no puede calcular. |

Los regímenes y el diagnóstico por equipo se conservan en `raw`, junto con la regla seleccionada, las fuentes, la temporada, la unidad y el estado. `NO_APPLICABLE_OBJECTIVE` con estado `ACTIVE` distingue presión calculada cero de información insuficiente.

## Límites y mantenimiento

Solo se aplican perfiles con coincidencia explícita de temporada; etiquetas desconocidas y otras temporadas no heredan reglas actuales. Los metadatos generales anteriores siguen disponibles para los otros módulos cuando no hay perfil coincidente. NHL conserva su valor general anterior de 82 y lo sustituye por 84 exclusivamente en el perfil 2026/27.

No se añaden adquisiciones, migraciones ni reconstrucciones históricas. El filtro de temporada regular y la lista operativa permanecen vigentes. Las tablas por grupos solo pueden calcularse si el análisis existente expone identificador de grupo, posiciones locales y población completa; no se fabrican a partir de una tabla general. Las rutas internas de playoffs de Liiga quedan pendientes de mayor detalle reglamentario.

Para una nueva temporada se incorpora otro `CompetitionRules` al catálogo; no se cambia la clave de un perfil histórico ni se reutiliza por defecto el más reciente. Los campos y fórmulas generales de resultados y los contratos de minería conservan su forma.

## Verificación

Las pruebas de paridad usan una política sintética explícita equivalente a la anterior. Las pruebas de perfiles cubren las identidades, permanencia con/sin eliminatoria, ligas pequeñas, ausencia de descenso, clasificación de fase, separación de grupos, fronteras pendientes, porcentajes, Wild Cards, aislamiento de temporada y resolución antes del historial con metadatos completos. Las pruebas existentes de instrumentación verifican captura inmutable y contratos de minería.
