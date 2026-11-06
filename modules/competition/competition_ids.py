"""Portable provider identities for sporting profiles and their destinations.

Each constant identifies a specific SofaScore tournament record. Main and
qualification records retain separate identities and readable names. Local
competitions.competition_id values are resolved by persistence, never stored here.
"""

from types import MappingProxyType
from typing import Final, Mapping

from .competition_identity import CompetitionIdentity


# Perfiles deportivos actuales. #CompetitionIdentity.source_unique_tournament_id y source_tournament_id distinguen la fase principal de la fase previa.
# La identidad de proveedor es inmutable y no depende de la base de datos ni de la configuración local. La identidad de destino se resuelve por el repositorio EventSourceMappingRepository.
LALIGA: Final[CompetitionIdentity] = CompetitionIdentity(8, 36, "sofascore")
PREMIER_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(17, 1, "sofascore")
SERIE_A: Final[CompetitionIdentity] = CompetitionIdentity(23, 33, "sofascore")
BUNDESLIGA: Final[CompetitionIdentity] = CompetitionIdentity(35, 42, "sofascore")
LIGUE_1: Final[CompetitionIdentity] = CompetitionIdentity(34, 4, "sofascore")
LIGA_PORTUGAL: Final[CompetitionIdentity] = CompetitionIdentity(238, 52, "sofascore")
EREDIVISIE: Final[CompetitionIdentity] = CompetitionIdentity(37, 39, "sofascore")
SAUDI_PRO_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(955, 3708, "sofascore")
BRASILEIRAO: Final[CompetitionIdentity] = CompetitionIdentity(325, 83, "sofascore")
LIGA_MX_APERTURA: Final[CompetitionIdentity] = CompetitionIdentity(11621, 28, "sofascore")
LIGA_MX_APERTURA_PLAY_IN: Final[CompetitionIdentity] = CompetitionIdentity(11621, 123913, "sofascore")
LIGA_MX_APERTURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(11621, 16755, "sofascore")
LIGA_MX_CLAUSURA: Final[CompetitionIdentity] = CompetitionIdentity(11620, 16753, "sofascore")
LIGA_MX_CLAUSURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(11620, 16754, "sofascore")
ARGENTINA_APERTURA: Final[CompetitionIdentity] = CompetitionIdentity(155, 143625, "sofascore")
ARGENTINA_APERTURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(155, 151536, "sofascore")
ARGENTINA_CLAUSURA: Final[CompetitionIdentity] = CompetitionIdentity(155, 156467, "sofascore")
ARGENTINA_CLAUSURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(155, 169353, "sofascore")
MLS: Final[CompetitionIdentity] = CompetitionIdentity(242, 18, "sofascore")
MLS_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(242, 7048, "sofascore")
NBA: Final[CompetitionIdentity] = CompetitionIdentity(132, 177, "sofascore")
NBA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(132, 10561, "sofascore")
NBA_PLAY_IN: Final[CompetitionIdentity] = CompetitionIdentity(132, 81454, "sofascore")
EUROLEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(138, 42527, "sofascore")
EUROLEAGUE_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(138, 5778, "sofascore")
EUROLEAGUE_PLAY_IN: Final[CompetitionIdentity] = CompetitionIdentity(138, 128726, "sofascore")
MLB: Final[CompetitionIdentity] = CompetitionIdentity(11205, 108806, "sofascore")
MLB_POSTSEASON: Final[CompetitionIdentity] = CompetitionIdentity(11205, 108845, "sofascore")
NFL: Final[CompetitionIdentity] = CompetitionIdentity(9464, 108947, "sofascore")
NFL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(9464, 41273, "sofascore")
SHL: Final[CompetitionIdentity] = CompetitionIdentity(261, 115, "sofascore")
SHL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(261, 360, "sofascore")
LIIGA: Final[CompetitionIdentity] = CompetitionIdentity(134, 108, "sofascore")
LIIGA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(134, 5705, "sofascore")
NHL: Final[CompetitionIdentity] = CompetitionIdentity(234, 142, "sofascore")
NHL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(234, 10560, "sofascore")
AHL: Final[CompetitionIdentity] = CompetitionIdentity(844, 2768, "sofascore")
AHL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(844, 13946, "sofascore")
WNBA: Final[CompetitionIdentity] = CompetitionIdentity(486, 591, "sofascore")
WNBA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(486, 11419, "sofascore")
CBA: Final[CompetitionIdentity] = CompetitionIdentity(1566, 10233, "sofascore")
CBA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(1566, 33232, "sofascore")
PFL: Final[CompetitionIdentity] = CompetitionIdentity(1654, 19822, "sofascore")

# Destinos externos y copas que podrán otorgar plazas.
UEFA_CHAMPIONS_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(7, 138314, "sofascore")
UEFA_CHAMPIONS_LEAGUE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(7, 1339, "sofascore")
UEFA_EUROPA_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(679, 138315, "sofascore")
UEFA_EUROPA_LEAGUE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(679, 10910, "sofascore")
UEFA_CONFERENCE_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(17015, 138316, "sofascore")
UEFA_CONFERENCE_LEAGUE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(17015, 93604, "sofascore")
AFC_CHAMPIONS_LEAGUE_ELITE: Final[CompetitionIdentity] = CompetitionIdentity(463, 1085, "sofascore")
AFC_CHAMPIONS_LEAGUE_ELITE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(463, 10172, "sofascore")
AFC_CHAMPIONS_LEAGUE_TWO_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(668, 19500, "sofascore")
AFC_CHAMPIONS_LEAGUE_TWO: Final[CompetitionIdentity] = CompetitionIdentity(668, None, "sofascore")
CONCACAF_CHAMPIONS_CUP: Final[CompetitionIdentity] = CompetitionIdentity(498, 9895, "sofascore")
CHAMPIONS_HOCKEY_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(494, 116755, "sofascore")
CHAMPIONS_HOCKEY_LEAGUE_KNOCKOUT_STAGE: Final[CompetitionIdentity] = CompetitionIdentity(494, 44575, "sofascore")
CONMEBOL_LIBERTADORES_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(384, 14805, "sofascore")
CONMEBOL_LIBERTADORES: Final[CompetitionIdentity] = CompetitionIdentity(384, None, "sofascore")
CONMEBOL_SUDAMERICANA: Final[CompetitionIdentity] = CompetitionIdentity(480, None, "sofascore")
FIBA_INTERCONTINENTAL_CUP: Final[CompetitionIdentity] = CompetitionIdentity(1512, None, "sofascore")
# Competitions without a single general tournament row use a family identity
# (source_unique_tournament_id with source_tournament_id=None) and an explicit
# destination_phase in the qualification rule.
EFL_CUP: Final[CompetitionIdentity] = CompetitionIdentity(21, 17, "sofascore")
COPA_DEL_REY: Final[CompetitionIdentity] = CompetitionIdentity(329, 150, "sofascore")  # Fútbol; no las copas homónimas de otros deportes.


COMPETITION_NAMES: Final[Mapping[CompetitionIdentity, str]] = MappingProxyType({
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
    LIGA_MX_APERTURA_PLAY_IN: "Liga MX, Apertura, Play In",
    LIGA_MX_APERTURA_PLAYOFFS: "Liga MX, Apertura Playoffs",
    LIGA_MX_CLAUSURA: "Liga MX, Clausura",
    LIGA_MX_CLAUSURA_PLAYOFFS: "Liga MX, Clausura Playoffs",
    ARGENTINA_APERTURA: "Liga Profesional de Fútbol, Apertura",
    ARGENTINA_APERTURA_PLAYOFFS: "Liga Profesional de Fútbol, Apertura Playoffs",
    ARGENTINA_CLAUSURA: "Liga Profesional, Clausura",
    ARGENTINA_CLAUSURA_PLAYOFFS: "Liga Profesional, Clausura Playoffs",
    MLS: "MLS",
    MLS_PLAYOFFS: "MLS, Playoffs",
    NBA: "NBA",
    NBA_PLAYOFFS: "NBA, Playoffs",
    NBA_PLAY_IN: "NBA, Play-In Tournament",
    EUROLEAGUE: "Euroleague",
    EUROLEAGUE_PLAYOFFS: "Euroleague, Playoffs",
    EUROLEAGUE_PLAY_IN: "Euroleague, Play-In",
    MLB: "MLB",
    MLB_POSTSEASON: "MLB Postseason",
    NFL: "NFL",
    NFL_PLAYOFFS: "NFL, Playoffs",
    SHL: "SHL",
    SHL_PLAYOFFS: "SHL, Playoffs",
    LIIGA: "Liiga",
    LIIGA_PLAYOFFS: "Liiga Playoffs",
    NHL: "NHL",
    NHL_PLAYOFFS: "NHL, Playoffs",
    AHL: "AHL",
    AHL_PLAYOFFS: "AHL, Playoffs",
    WNBA: "WNBA",
    WNBA_PLAYOFFS: "WNBA, Playoffs",
    CBA: "China CBA",
    CBA_PLAYOFFS: "China CBA, Playoffs",
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
    AFC_CHAMPIONS_LEAGUE_TWO: "AFC Champions League Two",
    CONCACAF_CHAMPIONS_CUP: "CONCACAF Champions Cup",
    CHAMPIONS_HOCKEY_LEAGUE: "Champions Hockey League",
    CHAMPIONS_HOCKEY_LEAGUE_KNOCKOUT_STAGE: "Champions Hockey League, Knockout stage",
    CONMEBOL_LIBERTADORES_QUALIFICATION: "CONMEBOL Libertadores, Qualification",
    CONMEBOL_LIBERTADORES: "CONMEBOL Libertadores",
    CONMEBOL_SUDAMERICANA: "CONMEBOL Sudamericana",
    FIBA_INTERCONTINENTAL_CUP: "FIBA Intercontinental Cup",
    EFL_CUP: "EFL Cup",
    COPA_DEL_REY: "Copa del Rey",
})
