"""Portable provider identities for sporting profiles and their destinations.

Each constant identifies a specific SofaScore tournament record. Main and
qualification records retain separate identities and readable names. Local
competitions.competition_id values are resolved by persistence, never stored as
identity data here; inline competition_id comments are informational DB snapshots.
"""

from types import MappingProxyType
from typing import Final, Mapping

from .competition_identity import CompetitionIdentity


# Perfiles deportivos actuales. CompetitionIdentity.source_unique_tournament_id y
# source_tournament_id distinguen la fase principal de la fase previa. La identidad
# de proveedor no depende de la base de datos; los competition_id comentados son
# referencias informativas consultadas en la DB local. El destino se resuelve por
# EventSourceMappingRepository.
LALIGA: Final[CompetitionIdentity] = CompetitionIdentity(8, 36, "sofascore", competition_name="LaLiga")  # competition_id=167
PREMIER_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(17, 1, "sofascore", competition_name="Premier League")  # competition_id=168
SERIE_A: Final[CompetitionIdentity] = CompetitionIdentity(23, 33, "sofascore", competition_name="Serie A")  # competition_id=88
BUNDESLIGA: Final[CompetitionIdentity] = CompetitionIdentity(35, 42, "sofascore", competition_name="Bundesliga")  # competition_id=171
LIGUE_1: Final[CompetitionIdentity] = CompetitionIdentity(34, 4, "sofascore", competition_name="Ligue 1")  # competition_id=172
LIGA_PORTUGAL: Final[CompetitionIdentity] = CompetitionIdentity(238, 52, "sofascore", competition_name="Liga Portugal Betclic")  # competition_id=261
EREDIVISIE: Final[CompetitionIdentity] = CompetitionIdentity(37, 39, "sofascore", competition_name="VriendenLoterij Eredivisie")  # competition_id=317
SAUDI_PRO_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(955, 3708, "sofascore", competition_name="Saudi Pro League")  # competition_id=50
BRASILEIRAO: Final[CompetitionIdentity] = CompetitionIdentity(325, 83, "sofascore", competition_name="Campeonato Brasileiro Série A")  # competition_id=318
LIGA_MX_APERTURA: Final[CompetitionIdentity] = CompetitionIdentity(11621, 28, "sofascore", competition_name="Liga MX, Apertura")  # competition_id=328
LIGA_MX_APERTURA_PLAY_IN: Final[CompetitionIdentity] = CompetitionIdentity(11621, 123913, "sofascore", competition_name="Liga MX, Apertura, Play In")  # competition_id=1042
LIGA_MX_APERTURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(11621, 16755, "sofascore", competition_name="Liga MX, Apertura Playoffs")  # competition_id=1126
LIGA_MX_CLAUSURA: Final[CompetitionIdentity] = CompetitionIdentity(11620, 16753, "sofascore", competition_name="Liga MX, Clausura")  # competition_id=2192
LIGA_MX_CLAUSURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(11620, 16754, "sofascore", competition_name="Liga MX, Clausura Playoffs")  # competition_id=1751
ARGENTINA_APERTURA: Final[CompetitionIdentity] = CompetitionIdentity(155, 143625, "sofascore", competition_name="Liga Profesional de Fútbol, Apertura")  # competition_id=2476
ARGENTINA_APERTURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(155, 151536, "sofascore", competition_name="Liga Profesional de Fútbol, Apertura Playoffs")  # competition_id=4935
ARGENTINA_CLAUSURA: Final[CompetitionIdentity] = CompetitionIdentity(155, 156467, "sofascore", competition_name="Liga Profesional, Clausura")  # competition_id=310
ARGENTINA_CLAUSURA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(155, 169353, "sofascore", competition_name="Liga Profesional, Clausura Playoffs")  # competition_id=1070
MLS: Final[CompetitionIdentity] = CompetitionIdentity(242, 18, "sofascore", competition_name="MLS")  # competition_id=76
MLS_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(242, 7048, "sofascore", competition_name="MLS, Playoffs")  # competition_id=581
NBA: Final[CompetitionIdentity] = CompetitionIdentity(132, 177, "sofascore", competition_name="NBA")  # competition_id=176
NBA_CUP_FINAL: Final[CompetitionIdentity] = CompetitionIdentity(132, 124629, "sofascore")  # competition_id=177
NBA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(132, 10561, "sofascore", competition_name="NBA, Playoffs")  # competition_id=146
NBA_PLAY_IN: Final[CompetitionIdentity] = CompetitionIdentity(132, 81454, "sofascore", competition_name="NBA, Play-In Tournament")  # competition_id=178
EUROLEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(138, 42527, "sofascore", competition_name="Euroleague")  # competition_id=429
EUROLEAGUE_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(138, 5778, "sofascore", competition_name="Euroleague, Playoffs")  # competition_id=147
EUROLEAGUE_PLAY_IN: Final[CompetitionIdentity] = CompetitionIdentity(138, 128726, "sofascore", competition_name="Euroleague, Play-In")  # competition_id=4519
MLB: Final[CompetitionIdentity] = CompetitionIdentity(11205, 108806, "sofascore", competition_name="MLB")  # competition_id=129
MLB_POSTSEASON: Final[CompetitionIdentity] = CompetitionIdentity(11205, 108845, "sofascore", competition_name="MLB Postseason")  # competition_id=14863
NFL: Final[CompetitionIdentity] = CompetitionIdentity(9464, 108947, "sofascore", competition_name="NFL")  # competition_id=5153
NFL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(9464, 41273, "sofascore", competition_name="NFL, Playoffs")  # competition_id=5154
SHL: Final[CompetitionIdentity] = CompetitionIdentity(261, 115, "sofascore", competition_name="SHL")  # competition_id=180
SHL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(261, 360, "sofascore", competition_name="SHL, Playoffs")  # competition_id=181
LIIGA: Final[CompetitionIdentity] = CompetitionIdentity(134, 108, "sofascore", competition_name="Liiga")  # competition_id=525
LIIGA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(134, 5705, "sofascore", competition_name="Liiga Playoffs")  # competition_id=3707
NHL: Final[CompetitionIdentity] = CompetitionIdentity(234, 142, "sofascore", competition_name="NHL")  # competition_id=5196
NHL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(234, 10560, "sofascore", competition_name="NHL, Playoffs")  # competition_id=1810
AHL: Final[CompetitionIdentity] = CompetitionIdentity(844, 2768, "sofascore", competition_name="AHL")  # competition_id=554
AHL_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(844, 13946, "sofascore", competition_name="AHL, Playoffs")  # competition_id=150
WNBA: Final[CompetitionIdentity] = CompetitionIdentity(486, 591, "sofascore", competition_name="WNBA")  # competition_id=145
WNBA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(486, 11419, "sofascore", competition_name="WNBA, Playoffs")  # competition_id=331
CBA: Final[CompetitionIdentity] = CompetitionIdentity(1566, 10233, "sofascore", competition_name="China CBA")  # competition_id=174
CBA_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(1566, 33232, "sofascore", competition_name="China CBA, Playoffs")  # competition_id=175
PFL: Final[CompetitionIdentity] = CompetitionIdentity(1654, 19822, "sofascore", competition_name="Philippines Football League")  # competition_id=5151

# Destinos externos y copas que podrán otorgar plazas.
UEFA_CHAMPIONS_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(7, 138314, "sofascore", competition_name="UEFA Champions League")  # competition_id=348
UEFA_CHAMPIONS_LEAGUE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(7, 1339, "sofascore", competition_name="UEFA Champions League, Qualification")  # competition_id=6151
UEFA_EUROPA_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(679, 138315, "sofascore", competition_name="UEFA Europa League")  # competition_id=400
UEFA_EUROPA_LEAGUE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(679, 10910, "sofascore", competition_name="UEFA Europa League, Qualification")  # competition_id=6182
UEFA_CONFERENCE_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(17015, 138316, "sofascore", competition_name="UEFA Conference League")  # competition_id=441
UEFA_CONFERENCE_LEAGUE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(17015, 93604, "sofascore", competition_name="UEFA Conference League, Qualification")  # competition_id=6138
AFC_CHAMPIONS_LEAGUE_ELITE: Final[CompetitionIdentity] = CompetitionIdentity(463, 1085, "sofascore", competition_name="AFC Champions League Elite")  # competition_id=3309
AFC_CHAMPIONS_LEAGUE_ELITE_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(463, 10172, "sofascore", competition_name="AFC Champions League Elite, Qualification")  # competition_id=9371
CONCACAF_CHAMPIONS_CUP: Final[CompetitionIdentity] = CompetitionIdentity(498, 9895, "sofascore", competition_name="CONCACAF Champions Cup")  # competition_id=2746
CHAMPIONS_HOCKEY_LEAGUE: Final[CompetitionIdentity] = CompetitionIdentity(494, 116755, "sofascore", competition_name="Champions Hockey League")  # competition_id=223
CHAMPIONS_HOCKEY_LEAGUE_KNOCKOUT_STAGE: Final[CompetitionIdentity] = CompetitionIdentity(494, 44575, "sofascore", competition_name="Champions Hockey League, Knockout stage")  # competition_id=3315
CONMEBOL_LIBERTADORES_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(384, 14805, "sofascore", competition_name="CONMEBOL Libertadores, Qualification")  # competition_id=2748
# Competitions without a single general tournament row use a family identity
# (source_unique_tournament_id with source_tournament_id=None) and an explicit destination_phase in the qualification rule.
CONMEBOL_LIBERTADORES: Final[CompetitionIdentity] = CompetitionIdentity(384, None, "sofascore", competition_name="CONMEBOL Libertadores")  # competition_ids de la familia: 131, 136, 360, 1195, 2748, 4185, 4190, 4219, 4220, 4227
CONMEBOL_SUDAMERICANA: Final[CompetitionIdentity] = CompetitionIdentity(480, None, "sofascore", competition_name="CONMEBOL Sudamericana")  # competition_ids de la familia: 137, 139, 144, 363, 1196, 1765, 3319, 4183, 4216, 4217
AFC_CHAMPIONS_LEAGUE_TWO_QUALIFICATION: Final[CompetitionIdentity] = CompetitionIdentity(668, 19500, "sofascore", competition_name="AFC Champions League Two, Qualification")  # competition_id=9447
AFC_CHAMPIONS_LEAGUE_TWO: Final[CompetitionIdentity] = CompetitionIdentity(668, None, "sofascore", competition_name="AFC Champions League Two")  # competition_ids de la familia: 1567, 1592, 1597, 1598, 1909, 1916, 1917, 1918, 2882, 9447

# COMPETITIONS/TOURNAMENTS THAT ARE NOT MAIN PHASES, BUT CAN GRANT ACCESS TO OTHER COMPETITIONS. THEY ARE NOT TRACKED FOR NOW, BUT THEIR IDENTITIES ARE USED IN RULES.
EFL_CUP: Final[CompetitionIdentity] = CompetitionIdentity(21, 17, "sofascore", competition_name="EFL Cup")  # competition_id=391
FA_CUP: Final[CompetitionIdentity] = CompetitionIdentity(19, 16, "sofascore", competition_name="FA Cup")  # competition_id=774
COPPA_ITALIA: Final[CompetitionIdentity] = CompetitionIdentity(328, 35, "sofascore", competition_name="Coppa Italia")  # competition_id=388
COPA_DEL_REY: Final[CompetitionIdentity] = CompetitionIdentity(329, 150, "sofascore", competition_name="Copa del Rey")  # competition_id=448; Fútbol; no las copas homónimas de otros deportes.
DFB_POKAL: Final[CompetitionIdentity] = CompetitionIdentity(217, 43, "sofascore", competition_name="DFB-Pokal")  # competition_id=74
COPA_DO_BRASIL: Final[CompetitionIdentity] = CompetitionIdentity(373, 283, "sofascore", competition_name="Copa do Brasil")  # competition_id=293; Fútbol; no las copas homónimas de otros deportes.
COPA_ARGENTINA: Final[CompetitionIdentity] = CompetitionIdentity(1024, 273, "sofascore", competition_name="Copa Argentina")  # competition_id=2401; Fútbol; no las copas homónimas de otros deportes.
COUPE_DE_FRANCE: Final[CompetitionIdentity] = CompetitionIdentity(335, 77, "sofascore", competition_name="Coupe de France")  # competition_id=236; Fútbol masculino
TACA_DE_PORTUGAL: Final[CompetitionIdentity] = CompetitionIdentity(336, 207, "sofascore", competition_name="Taça de Portugal")  # competition_id=374; Fútbol masculino
KNVB_BEKER: Final[CompetitionIdentity] = CompetitionIdentity(330, 121, "sofascore", competition_name="KNVB Beker")  # competition_id=390
US_OPEN_CUP: Final[CompetitionIdentity] = CompetitionIdentity(495, 6814, "sofascore")  # competition_id=3661; Fútbol; no la homónima de tenis.
SAUDI_KINGS_CUP: Final[CompetitionIdentity] = CompetitionIdentity(2058, 42021, "sofascore", competition_name="King's Cup")  # competition_id=681; FUTBOL MASCULINO
LIGA_MX_CAMPEON_DE_CAMPEONES: Final[CompetitionIdentity] = CompetitionIdentity(2103, 54197, "sofascore", competition_name="Campeón de Campeones")  # competition_id=7935
LEAGUES_CUP_GROUP_STAGE: Final[CompetitionIdentity] = CompetitionIdentity(13783, 146257, "sofascore")  # competition_id=8807; Fútbol; no la homónima de tenis.
LEAGUES_CUP: Final[CompetitionIdentity] = CompetitionIdentity(13783, 10964, "sofascore")  # sin fila en DB para (13783, 10964); Fútbol; no la homónima de tenis.
# WINNER OF MLS REGULAR SEASON GETS A SPOT IN CONCACAF CHAMPIONS CUP, BUT THERE IS NO SEPARATE TOURNAMENT RECORD FOR IT. SO TECHNICALLY MLS_PLAYOFFS: Final[CompetitionIdentity] = CompetitionIdentity(242, 7048, "sofascore")

COMPETITION_NAMES: Final[Mapping[CompetitionIdentity, str]] = MappingProxyType({
    identity: identity.competition_name
    for identity in tuple(globals().values())
    if isinstance(identity, CompetitionIdentity) and identity.competition_name is not None
})
