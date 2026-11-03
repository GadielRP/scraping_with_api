"""Referencias legibles a competition_id del catálogo local.

Las constantes identifican competiciones concretas; no son términos de reglas
deportivas. Se verifican contra competitions, distinguiendo nombre, ámbito,
deporte y los identificadores del proveedor. No son source_tournament_id ni
source_unique_tournament_id. No se consulta la base de datos durante el cálculo.

COMPETITION_NAMES contiene las etiquetas de estas referencias del catálogo.
Los perfiles configuran explícitamente el nombre legible de cada destino.
"""

from types import MappingProxyType
from typing import Final, Mapping


# Perfiles deportivos actuales.
LALIGA: Final[int] = 167
PREMIER_LEAGUE: Final[int] = 168
SERIE_A: Final[int] = 88
BUNDESLIGA: Final[int] = 171
LIGUE_1: Final[int] = 172
LIGA_PORTUGAL: Final[int] = 261
EREDIVISIE: Final[int] = 317
SAUDI_PRO_LEAGUE: Final[int] = 50
BRASILEIRAO: Final[int] = 318
LIGA_MX_APERTURA: Final[int] = 328
LIGA_MX_CLAUSURA: Final[int] = 2192
ARGENTINA_APERTURA: Final[int] = 2476
ARGENTINA_CLAUSURA: Final[int] = 310
MLS: Final[int] = 76
NBA: Final[int] = 176
EUROLEAGUE: Final[int] = 429
MLB: Final[int] = 129
NFL: Final[int] = 5153
SHL: Final[int] = 180
LIIGA: Final[int] = 525
NHL: Final[int] = 5196
AHL: Final[int] = 554
WNBA: Final[int] = 145
CBA: Final[int] = 174
PFL: Final[int] = 5151

# Destinos externos y copas que podrán otorgar plazas.
UEFA_CHAMPIONS_LEAGUE: Final[int] = 348
UEFA_CHAMPIONS_LEAGUE_QUALIFICATION: Final[int] = 6151
UEFA_EUROPA_LEAGUE: Final[int] = 400
UEFA_EUROPA_LEAGUE_QUALIFICATION: Final[int] = 6182
UEFA_CONFERENCE_LEAGUE: Final[int] = 441
UEFA_CONFERENCE_LEAGUE_QUALIFICATION: Final[int] = 6138
AFC_CHAMPIONS_LEAGUE_ELITE: Final[int] = 3309
AFC_CHAMPIONS_LEAGUE_ELITE_QUALIFICATION: Final[int] = 9371
AFC_CHAMPIONS_LEAGUE_TWO_QUALIFICATION: Final[int] = 9447
CONCACAF_CHAMPIONS_CUP: Final[int] = 2746
CHAMPIONS_HOCKEY_LEAGUE: Final[int] = 223
CONMEBOL_LIBERTADORES_QUALIFICATION: Final[int] = 2748
# No se usa una fila de grupos o eliminatorias como identidad general.
# Pendientes de registro general: AFC Champions League Two, CONMEBOL
# Libertadores, CONMEBOL Sudamericana y FIBA Intercontinental Cup.
# Sus destinos directos permanecen sin ID y con estado PENDING en los perfiles.
EFL_CUP: Final[int] = 391
COPA_DEL_REY: Final[int] = 448  # Fútbol; no las copas homónimas de otros deportes.


COMPETITION_NAMES: Final[Mapping[int, str]] = MappingProxyType({
    LALIGA: "LaLiga",
    PREMIER_LEAGUE: "Premier League",
    SERIE_A: "Serie A",
    BUNDESLIGA: "Bundesliga",
    LIGUE_1: "Ligue 1",
    LIGA_PORTUGAL: "Liga Portugal Betclic",
    EREDIVISIE: "VriendenLoterij Eredivisie",
    SAUDI_PRO_LEAGUE: "Saudi Pro League",
    BRASILEIRAO: "Campeonato Brasileiro Série A",
    LIGA_MX_APERTURA: "Liga MX, Apertura",
    LIGA_MX_CLAUSURA: "Liga MX, Clausura",
    ARGENTINA_APERTURA: "Liga Profesional de Fútbol, Apertura",
    ARGENTINA_CLAUSURA: "Liga Profesional, Clausura",
    MLS: "MLS",
    NBA: "NBA",
    EUROLEAGUE: "Euroleague",
    MLB: "MLB",
    NFL: "NFL",
    SHL: "SHL",
    LIIGA: "Liiga",
    NHL: "NHL",
    AHL: "AHL",
    WNBA: "WNBA",
    CBA: "China CBA",
    PFL: "Philippines Football League",
    UEFA_CHAMPIONS_LEAGUE: "UEFA Champions League",
    UEFA_CHAMPIONS_LEAGUE_QUALIFICATION: "UEFA Champions League, Qualification",
    UEFA_EUROPA_LEAGUE: "UEFA Europa League",
    UEFA_EUROPA_LEAGUE_QUALIFICATION: "UEFA Europa League, Qualification",
    UEFA_CONFERENCE_LEAGUE: "UEFA Conference League",
    UEFA_CONFERENCE_LEAGUE_QUALIFICATION: "UEFA Conference League, Qualification",
    AFC_CHAMPIONS_LEAGUE_ELITE: "AFC Champions League Elite",
    AFC_CHAMPIONS_LEAGUE_ELITE_QUALIFICATION: "AFC Champions League Elite, Qualification",
    AFC_CHAMPIONS_LEAGUE_TWO_QUALIFICATION: "AFC Champions League Two, Qualification",
    CONCACAF_CHAMPIONS_CUP: "CONCACAF Champions Cup",
    CHAMPIONS_HOCKEY_LEAGUE: "Champions Hockey League",
    CONMEBOL_LIBERTADORES_QUALIFICATION: "CONMEBOL Libertadores, Qualification",
    EFL_CUP: "EFL Cup",
    COPA_DEL_REY: "Copa del Rey",
})
