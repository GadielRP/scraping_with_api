"""Vocabulario canónico de los perfiles deportivos.

Campos de QualificationDestination, RankSelection y ObjectiveRule
-------------------------------------------------------------
objective_type: finalidad deportiva: acceder a otra competición, avanzar de
    fase en la actual o conservar la categoría. No es el resultado de un equipo.
destination_competition_identity: identidad de proveedor, con source,
    source_unique_tournament_id y source_tournament_id. Distingue las filas de
    fase principal y previa. Obligatoria en clasificación externa CONFIGURED;
    None con comentario y estado PENDING si falta la identidad adecuada.
    competition_ids.py reúne estas referencias; no contiene claves locales.
    El nombre legible se guarda en destination_competition_identity.competition_name.
destination_phase: fase a la que se accede en el destino: interno o externo.
    Obligatoria para objetivos de fase y para objetivos externos configurados.
    Solo puede ser None en una asignación PENDING cuya fase no se conoce.
destination_round: ronda concreta de entrada dentro de destination_phase, si se
    conoce y distingue el límite deportivo; por ejemplo ROUND_OF_16 o
    QUARTER_FINALS. No reemplaza destination_phase ni qualification_path.
qualification_path: vía para llegar a una competición externa o fase interna.
    Debe ser conocida en reglas CONFIGURED; None se reserva a vías desconocidas
    PENDING. No describe la fase destino.
relegation_path: mecanismo de riesgo de descenso, separado de la vía de
    clasificación: directo o playoff de permanencia.
RankSelection.ranking_scope: población cuya tabla decide la selección. GLOBAL abarca toda la
    competición; LEAGUE es una subliga, como AL/NL dentro de MLB.
RankSelection.aggregation_scope: periodo de resultados que cuenta: torneo actual, acumulado
    del ciclo de temporada (puede cruzar años naturales) o varias temporadas.
status: CONFIGURED indica una frontera deportiva declarada como resuelta;
    PENDING indica que falta resolver/verificar la regla. No garantiza soporte
    del motor ni significa que un equipo ya haya asegurado la clasificación.
RankSelection.rank_start, RankSelection.rank_end: primer y último puesto de la zona, ambos inclusivos y
    relativos a ranking_scope. Se especifican juntos; None si no hay rango.
RankSelection.reference_rank: puesto de referencia para medir la distancia competitiva.
    En acceso suele ser rank_end; en permanencia es el último puesto seguro.
RankSelection.selection_method: cómo se eligen los equipos; no define el orden de desempate.
RankSelection.places: cantidad de plazas cuando la selección no se expresa como un rango.
RankSelection.seasons: temporadas que abarca un método multianual (por ejemplo el promedio de descenso).
RankSelection.group_size: número de equipos de la tabla local, no de toda la competición.
RankSelection.ranking_group: grupo concreto dentro del ámbito, por ejemplo Atlantic.
    Es una identidad local, no un término deportivo global.
note: detalles humanos, nombre comercial, formato de serie o motivo pendiente.
    El motor no interpreta esta nota para seleccionar equipos.

Campos de QualificationSources y TournamentBerthRule
---------------------------------------------------
Las plazas de tabla no se declaran aparte: salen de los objetivos
COMPETITION_QUALIFICATION del perfil, una por puesto, en orden. La
configuración solo añade las plazas que se ganan fuera de la tabla.
before_table_slot: posición (1-based) en la secuencia de plazas de tabla que
    esta plaza precede; None la coloca tras todas. Fija el orden de prioridad.
reallocate_to_table: si ningún club elegible la gana, pasa al mejor clasificado
    que aún no tenga plaza.
open_to_outsiders: el campeón ajeno a la tabla (otra división) conserva la plaza.
shared_table: tabla compartida por varios torneos que ordena a los clubes.

Campos de CompetitionRules
--------------------------
reference_season: edición descriptiva del perfil vigente; no filtra eventos ni
    selecciona versiones históricas del perfil.
number_of_teams: población total de la competición; None si no se configuró.
total_regular_season_games: partidos regulares por equipo; None si se desconoce.
scoring: puntos otorgados por resultado, compartidos con tablas y cálculos.
standings: métrica principal y criterios de orden/desempate de las tablas.
objectives: reglas de objetivos. None significa cobertura no configurada;
    una tupla vacía declara que no hay objetivos de este catálogo.
grouping_method: estrategia que identifica a qué tabla pertenece cada equipo.
    Conserva la competición porque sus resolutores de pertenencia son distintos.
note: descripción o limitaciones del perfil.

Campos de StandingsRules y RankingCriterion
------------------------------------------
name: identidad de la política de orden, no una vía ni un destino.
primary_metric: valor principal para ordenar (puntos, victorias o porcentaje).
in_season_criteria: pasos de desempate mientras no se alcanza el 85 % de partidos computados.
closing_projection_criteria: pasos desde el 85 % inclusive, para aproximar la clasificación de cierre.
restart_sequence: cuándo una separación reinicia la secuencia completa (pair_after_reapply, pair o
    complete_meetings); None si nunca.
record_percentage: expone también el porcentaje en el registro de clasificación.
regulation_scores_only: excluye el marcador de prórroga en criterios de tanteo.
fair_play_weights: pares (stat, peso) de la política para calcular puntos de penalización
    disciplinaria. Menos puntos es mejor; vacío significa baremo no configurado.
verified: indicador existente de revisión de la política, independiente de status.
note: detalles de la política, sin instrucciones ejecutables.
metrics: claves de datos que un criterio compara; pueden formar una minitabla.
meetings: encuentros necesarios por pareja para aplicar el criterio.
only_two, only_multiple: limita el criterio a empates de dos o varios equipos.
reapply: vuelve a aplicar el criterio al subconjunto que sigue empatado.
unavailable: motivo explícito por el que no se puede evaluar el criterio.
incomplete_meetings_action: qué hace el criterio si falta una ida o vuelta exigida: skip continúa,
    share_position conserva la igualdad y use_in_season vuelve a los criterios en curso.
only_across_boundaries: aplica el criterio solo si los puestos empatados cruzan el título o una frontera;
    en otro caso se omite.
pending_across_boundaries: ordena a los candidatos pero deja sus puestos pendientes si el empate cruza
    el título o el descenso, porque lo decide la resolución oficial.
complete_block: compara todas las métricas de la minitabla antes de reaplicarla a los que siguen empatados.

Campos de ScoringRules
----------------------
regulation_win, overtime_win, draw, overtime_loss, loss: puntos por victoria
    reglamentaria, victoria en prórroga/penales, empate, derrota en prórroga y
    derrota reglamentaria. Son cantidades, no opciones de vocabulario.

Los valores de estos StrEnum son cadenas explícitas y serializables como JSON.
Las dataclasses validan el vocabulario al construirse; las anotaciones solas no
validan argumentos. Estos términos no certifican las fronteras del catálogo,
cuya revisión deportiva queda pendiente.
"""

from enum import StrEnum


class ObjectiveType(StrEnum):
    """Finalidad de la regla, independiente de su destino y selección."""

    COMPETITION_QUALIFICATION = "COMPETITION_QUALIFICATION"
    PHASE_QUALIFICATION = "PHASE_QUALIFICATION"
    DIVISION_RETENTION = "DIVISION_RETENTION"


class DestinationPhase(StrEnum):
    """Fase de destino, externa o interna, separada de la vía de acceso."""

    LEAGUE_PHASE = "LEAGUE_PHASE"
    GROUP_STAGE = "GROUP_STAGE"
    QUALIFICATION_ROUND = "QUALIFICATION_ROUND"
    PLAY_IN = "PLAY_IN"
    PLAYOFFS = "PLAYOFFS"


class DestinationRound(StrEnum):
    """Ronda concreta de entrada dentro de la fase de destino."""

    PRELIMINARY_ROUND = "PRELIMINARY_ROUND"
    SECOND_QUALIFYING_ROUND = "SECOND_QUALIFYING_ROUND"
    THIRD_QUALIFYING_ROUND = "THIRD_QUALIFYING_ROUND"
    QUALIFYING_PLAYOFF = "QUALIFYING_PLAYOFF"
    ROUND_OF_16 = "ROUND_OF_16"
    ROUND_OF_32 = "ROUND_OF_32"
    QUARTER_FINALS = "QUARTER_FINALS"
    FINAL = "FINAL"


class QualificationPath(StrEnum):
    """Vía relativa al destino declarado; el destino conserva su propio campo."""

    # The stated ranking grants entry to destination_phase without an
    # intermediary domestic playoff. It does not imply a main competition phase.
    DIRECT = "DIRECT"
    VIA_DOMESTIC_PLAYOFF = "VIA_DOMESTIC_PLAYOFF"


class RelegationPath(StrEnum):
    """Vía de salida de la división por la frontera de permanencia."""

    DIRECT_RELEGATION = "DIRECT_RELEGATION"
    RELEGATION_PLAYOFF = "RELEGATION_PLAYOFF"


class RuleStatus(StrEnum):
    """Resolución deportiva de la regla, independiente del soporte del motor."""

    CONFIGURED = "CONFIGURED"
    PENDING = "PENDING"


class RankingScope(StrEnum):
    """Población de la tabla utilizada para seleccionar equipos."""

    GLOBAL = "GLOBAL"
    CONFERENCE = "CONFERENCE"
    DIVISION = "DIVISION"
    GROUP = "GROUP"
    LEAGUE = "LEAGUE"


class AggregationScope(StrEnum):
    """Periodo de resultados que alimenta la tabla de selección."""

    TOURNAMENT = "TOURNAMENT"
    SEASON_AGGREGATE = "SEASON_AGGREGATE"
    MULTISEASON_RECORD = "MULTISEASON_RECORD"


class Finalist(StrEnum):
    """Position in a decided final."""

    WINNER = "winner"
    RUNNER_UP = "runner_up"


class SelectionMethod(StrEnum):
    """Selección deportiva; los métodos compuestos requieren datos adicionales."""

    RANK_RANGE = "RANK_RANGE"  # Intervalo inclusivo en la tabla indicada.
    DIVISION_WINNERS = "DIVISION_WINNERS"  # Campeones divisionales del ámbito.
    BEST_REMAINING = "BEST_REMAINING"  # Mejores elegibles tras selecciones/exclusiones.
    CHAMPION = "CHAMPION"  # Campeón del torneo; no líder provisional.
    LOWEST_POINTS_AVERAGE = "LOWEST_POINTS_AVERAGE"  # Menor cociente puntos/partidos entre temporadas.
    LOWEST_POINTS = "LOWEST_POINTS" # Menor cantidad de puntos para una temporada.
    # Declara una asignación compuesta pendiente, no un algoritmo implementado.
    MULTIPLE_QUALIFICATION_PATHS = "MULTIPLE_QUALIFICATION_PATHS"


class StandingsMetric(StrEnum):
    """Métrica principal; conserva las claves utilizadas por las tablas."""

    POINTS = "points"
    WINS = "wins"
    WIN_PERCENTAGE = "pct"


class GroupingMethod(StrEnum):
    """Resolutores existentes de pertenencia a grupos, conferencias o divisiones."""

    LEAGUE_WIDE = "league_wide"
    NBA_CONFERENCE = "nba_conference"
    NBA_DIVISION = "nba_division"
    NFL_CONFERENCE = "nfl_conference"
    NFL_DIVISION = "nfl_division"
    MLB_LEAGUE = "mlb_league"
    MLB_DIVISION = "mlb_division"
    NHL_CONFERENCE = "nhl_conference"
    MLS_CONFERENCE = "mls_conference"
    AHL_DIVISION = "ahl_division"
    ARGENTINA_ZONES = "argentina_zones"


class BerthResolution(StrEnum):
    CONFIRMED = "CONFIRMED"
    PROJECTED = "PROJECTED"
    UNRESOLVED = "UNRESOLVED"
    UNAWARDED = "UNAWARDED"


class BerthType(StrEnum):
    TABLE = "TABLE"
    TOURNAMENT = "TOURNAMENT"


class BerthIssue(StrEnum):
    INCOMPLETE_CUP_EVIDENCE = "incomplete_cup_evidence"
    FINALIST_ELIGIBILITY_UNKNOWN = "finalist_eligibility_unknown"
    INCOMPLETE_CUP_BRACKET = "incomplete_cup_bracket"
    CUP_REPLACEMENT_ANNUAL_TIEBREAK_PENDING = "cup_replacement_annual_tiebreak_pending"
    # A higher-priority tournament berth may still change who remains eligible.
    EARLIER_BERTH_PENDING = "earlier_berth_pending"
    INSUFFICIENT_ELIGIBLE_PARTICIPANTS = "insufficient_eligible_participants"
    RANKING_UNRESOLVED = "ranking_unresolved"
    SHARED_POSITION = "shared_position"
    TOURNAMENT_WINNER_ALREADY_QUALIFIED = "tournament_winner_already_qualified"
    NO_ELIGIBLE_CUP_PARTICIPANT = "no_eligible_cup_participant"
    WINNER_NOT_AVAILABLE = "winner_not_available"


class TournamentFinalStatus(StrEnum):
    DECIDED = "DECIDED"
    FINAL_ABSENT = "final_absent"
    FINAL_FUTURE = "final_future"
    NO_OBSERVATION_BEFORE_CUTOFF = "no_observation_before_cutoff"
    LEGACY_RESULT_NOT_KNOWN_AT_CUTOFF = "legacy_result_not_known_at_cutoff"
    FINAL_UNFINISHED = "final_unfinished"
    MISSING_PARTICIPANT_IDENTITY = "missing_participant_identity"
    FINAL_AMBIGUOUS = "final_ambiguous"
    INVALID_SCORE = "invalid_score"
    CONTRADICTORY_EXTRA_TIME = "contradictory_extra_time"
    CONTRADICTORY_PENALTIES = "contradictory_penalties"
    UNDECIDED_PENALTIES = "undecided_penalties"
    NO_DECISIVE_RESULT = "no_decisive_result"
    CONTRADICTORY_WINNER = "contradictory_winner"


class TournamentFinalDecision(StrEnum):
    PENALTIES = "penalties"
    EXTRA_TIME = "extra_time"
    REGULATION = "regulation"


class RelegationIssue(StrEnum):
    INCOMPLETE_MULTISEASON_RECORD = "incomplete_multiseason_record"
    AVERAGE_RELEGATION_DECIDER_REQUIRED = "average_relegation_decider_required"
    RELEGATION_DECIDER_REQUIRED = "relegation_decider_required"
    INSUFFICIENT_RELEGATION_CANDIDATES = "insufficient_relegation_candidates"


class RelegationResolution(StrEnum):
    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"


class RankingStatus(StrEnum):
    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"
    SHARED_POSITION = "SHARED_POSITION"


class RankingIssue(StrEnum):
    AHL_IN_SEASON_TIEBREAK_UNCONFIRMED = "AHL_IN_SEASON_TIEBREAK_UNCONFIRMED"
    CURRENT_CBA_TIEBREAK_REGULATION_UNCONFIRMED = "CURRENT_CBA_TIEBREAK_REGULATION_UNCONFIRMED"
    EXTRA_TIME_OR_PENALTIES_PENDING = "EXTRA_TIME_OR_PENALTIES_PENDING"
    INCOMPLETE_HEAD_TO_HEAD = "INCOMPLETE_HEAD_TO_HEAD"
    MISSING_ALL_STAR_BREAK_BOUNDARY = "MISSING_ALL_STAR_BREAK_BOUNDARY"
    MISSING_FINAL_RECORD_VS_WINNING_TEAMS = "MISSING_FINAL_RECORD_VS_WINNING_TEAMS"
    MISSING_GROUP_MEMBERSHIP = "MISSING_GROUP_MEMBERSHIP"
    MISSING_MLB_GROUP_MEMBERSHIP = "MISSING_MLB_GROUP_MEMBERSHIP"
    MISSING_MULTISEASON_QUOTIENT = "MISSING_MULTISEASON_QUOTIENT"
    MISSING_NBA_GROUP_MEMBERSHIP = "MISSING_NBA_GROUP_MEMBERSHIP"
    MISSING_NET_TOUCHDOWNS = "MISSING_NET_TOUCHDOWNS"
    MISSING_NFL_GROUP_MEMBERSHIP = "MISSING_NFL_GROUP_MEMBERSHIP"
    MISSING_PREVIOUS_SEASON_RANK = "MISSING_PREVIOUS_SEASON_RANK"
    MISSING_REGULATION_SCORES = "MISSING_REGULATION_SCORES"
    OFFICIAL_DECIDER_REQUIRED = "OFFICIAL_DECIDER_REQUIRED"
    OFFICIAL_DRAW_PENDING = "OFFICIAL_DRAW_PENDING"
    OFFICIAL_RESOLUTION_PENDING = "OFFICIAL_RESOLUTION_PENDING"
    OFFICIAL_TIEBREAK_PENDING = "OFFICIAL_TIEBREAK_PENDING"
    OFFICIAL_TIEBREAK_UNRESOLVED = "OFFICIAL_TIEBREAK_UNRESOLVED"
    PFL_TIEBREAK_ORDER_UNCONFIRMED = "PFL_TIEBREAK_ORDER_UNCONFIRMED"
    PLAYOFF_MATCH_PENDING = "PLAYOFF_MATCH_PENDING"
    MISSING_AHL_CONFERENCE_POINTS_PERCENTAGE = "MISSING_AHL_CONFERENCE_POINTS_PERCENTAGE"
    MISSING_AHL_H2H = "MISSING_AHL_H2H"
    MISSING_ALPHABETICAL_ORDER = "MISSING_ALPHABETICAL_ORDER"
    MISSING_AWAY_GOALS = "MISSING_AWAY_GOALS"
    MISSING_AWAY_GOAL_DIFF = "MISSING_AWAY_GOAL_DIFF"
    MISSING_AWAY_WINS = "MISSING_AWAY_WINS"
    MISSING_BALANCED_AWAY_GOALS = "MISSING_BALANCED_AWAY_GOALS"
    MISSING_FAIR_PLAY_POINTS = "MISSING_FAIR_PLAY_POINTS"
    MISSING_FAIR_PLAY_POINTS_PER_GAME = "MISSING_FAIR_PLAY_POINTS_PER_GAME"
    MISSING_FAIR_PLAY_RED_CARDS = "MISSING_FAIR_PLAY_RED_CARDS"
    MISSING_GAMES_PLAYED = "MISSING_GAMES_PLAYED"
    MISSING_GAME_SCORE_QUOTIENTS = "MISSING_GAME_SCORE_QUOTIENTS"
    MISSING_GOALS_FOR = "MISSING_GOALS_FOR"
    MISSING_GOAL_DIFF = "MISSING_GOAL_DIFF"
    MISSING_H2H_AWAY_GOALS = "MISSING_H2H_AWAY_GOALS"
    MISSING_H2H_GOALS_FOR = "MISSING_H2H_GOALS_FOR"
    MISSING_H2H_GOAL_DIFF = "MISSING_H2H_GOAL_DIFF"
    MISSING_H2H_POINTS = "MISSING_H2H_POINTS"
    MISSING_H2H_REGULATION_WINS = "MISSING_H2H_REGULATION_WINS"
    MISSING_H2H_WINS = "MISSING_H2H_WINS"
    MISSING_H2H_WIN_PERCENTAGE = "MISSING_H2H_WIN_PERCENTAGE"
    MISSING_HOME_GOALS = "MISSING_HOME_GOALS"
    MISSING_HOME_GOAL_DIFF = "MISSING_HOME_GOAL_DIFF"
    MISSING_MLB_IN_SEASON_TIEBREAK = "MISSING_MLB_IN_SEASON_TIEBREAK"
    MISSING_MLB_TIEBREAK = "MISSING_MLB_TIEBREAK"
    MISSING_NBA_TIEBREAK = "MISSING_NBA_TIEBREAK"
    MISSING_NFL_IN_SEASON_TIEBREAK = "MISSING_NFL_IN_SEASON_TIEBREAK"
    MISSING_NFL_TIEBREAK = "MISSING_NFL_TIEBREAK"
    MISSING_NHL_H2H = "MISSING_NHL_H2H"
    MISSING_RED_CARDS = "MISSING_RED_CARDS"
    MISSING_REGULATION_OVERTIME_WINS = "MISSING_REGULATION_OVERTIME_WINS"
    MISSING_REGULATION_WINS = "MISSING_REGULATION_WINS"
    MISSING_WINS = "MISSING_WINS"
    MISSING_YELLOW_CARDS = "MISSING_YELLOW_CARDS"


class RankingMetric(StrEnum):
    AHL_CONFERENCE_POINTS_PERCENTAGE = "ahl_conference_points_percentage"
    AHL_H2H = "ahl_h2h"
    ALPHABETICAL_ORDER = "alphabetical_order"
    AWAY_GOAL_DIFF = "away_goal_diff"
    AWAY_GOALS = "away_goals"
    AWAY_WINS = "away_wins"
    BALANCED_AWAY_GOALS = "balanced_away_goals"
    GAME_SCORE_QUOTIENTS = "game_score_quotients"
    GOAL_DIFF = "goal_diff"
    GOALS_FOR = "goals_for"
    H2H_AWAY_GOALS = "h2h_away_goals"
    H2H_GOAL_DIFF = "h2h_goal_diff"
    H2H_GOALS_FOR = "h2h_goals_for"
    H2H_POINTS = "h2h_points"
    H2H_REGULATION_WINS = "h2h_regulation_wins"
    H2H_WIN_PERCENTAGE = "h2h_win_percentage"
    H2H_WINS = "h2h_wins"
    HOME_GOAL_DIFF = "home_goal_diff"
    HOME_GOALS = "home_goals"
    MLB_IN_SEASON_TIEBREAK = "mlb_in_season_tiebreak"
    MLB_TIEBREAK = "mlb_tiebreak"
    NBA_TIEBREAK = "nba_tiebreak"
    NEGATIVE_FAIR_PLAY_POINTS = "negative_fair_play_points"
    NEGATIVE_FAIR_PLAY_POINTS_PER_GAME = "negative_fair_play_points_per_game"
    NEGATIVE_FAIR_PLAY_RED_CARDS = "negative_fair_play_red_cards"
    NEGATIVE_GAMES_PLAYED = "negative_games_played"
    NEGATIVE_RED_CARDS = "negative_red_cards"
    NEGATIVE_YELLOW_CARDS = "negative_yellow_cards"
    NFL_IN_SEASON_TIEBREAK = "nfl_in_season_tiebreak"
    NFL_TIEBREAK = "nfl_tiebreak"
    NHL_H2H = "nhl_h2h"
    REGULATION_OVERTIME_WINS = "regulation_overtime_wins"
    REGULATION_WINS = "regulation_wins"


class IncompleteMeetingsAction(StrEnum):
    SKIP = "skip"
    SHARE_POSITION = "share_position"
    USE_IN_SEASON = "use_in_season"


class RestartSequence(StrEnum):
    PAIR_AFTER_REAPPLY = "pair_after_reapply"
    PAIR = "pair"
    COMPLETE_MEETINGS = "complete_meetings"


class TournamentRound(StrEnum):
    FINAL = "final"
    SEMIFINAL = "semifinal"
    QUARTERFINAL = "quarterfinal"
    ROUND_OF_16 = "round_of_16"
    ROUND_OF_32 = "round_of_32"
    ROUND_OF_64 = "round_of_64"


class FairPlayMetric(StrEnum):
    YELLOW_CARDS = "yellow_cards"
    RED_CARDS = "red_cards"
    STANDALONE_YELLOW_CARDS = "standalone_yellow_cards"
    SECOND_YELLOW_DISMISSALS = "second_yellow_dismissals"
    DIRECT_RED_CARDS = "direct_red_cards"
