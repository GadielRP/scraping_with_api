"""Configured standings orders used by current competition profiles.

Each policy declares the sequence used in season and the one used from the 85 %
closing projection. When the closing sequence is omitted both are the same.
Metrics are evaluated by standings_rules, not by the catalogue. An unavailable
step records an actual data/implementation limit, rather than a substitute rule.
Sporting boundaries and outstanding verification are reviewed separately.
"""

from dataclasses import replace

from .competitive_rules import RankingCriterion as Criterion, StandingsRules
from .rule_terms import StandingsMetric


def _standings(name, primary_metric, in_season, *, closing=None, **options):
    return StandingsRules(
        name, primary_metric, in_season, in_season if closing is None else closing, **options,
    )


def _metric_criterion(name):
    return Criterion((name,))


def _unavailable_criterion(reason):
    return Criterion((), unavailable=reason)


GOAL_DIFFERENCE_CRITERION = _metric_criterion("goal_diff")
GOALS_FOR_CRITERION = _metric_criterion("goals_for")
WINS_CRITERION = _metric_criterion(StandingsMetric.WINS)
FAIR_PLAY_CRITERION = _metric_criterion("negative_fair_play_points")
HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE = Criterion(("h2h_points", "h2h_goal_diff"), meetings=2)

LALIGA = _standings("LALIGA", StandingsMetric.POINTS, (
    Criterion(("h2h_goal_diff",), meetings=2, only_two=True),
    Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_multiple=True, reapply=True),
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, FAIR_PLAY_CRITERION,
), restart_sequence="pair_after_reapply",
    fair_play_weights=(("standalone_yellow_cards", 1), ("second_yellow_dismissals", 2), ("direct_red_cards", 3)),
    note="Internal card-based fair play; compares available match scores.")

SERIE_A = _standings("SERIE_A", StandingsMetric.POINTS, (HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE, GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION))

PREMIER_LEAGUE = _standings("PREMIER_LEAGUE", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, Criterion(("h2h_points", "h2h_away_goals"), meetings=2, only_across_boundaries=True),
),
    note="C.7/C.17: head-to-head applies only across title, relegation or qualification boundaries.")

BUNDESLIGA = _standings("BUNDESLIGA", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, Criterion(("h2h_goal_diff", "h2h_away_goals"), meetings=2, incomplete_meetings_action="share_position"),
    _metric_criterion("away_goals"),
))

LIGUE_1 = _standings("LIGUE_1", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE, GOALS_FOR_CRITERION, WINS_CRITERION, _metric_criterion("away_wins"), FAIR_PLAY_CRITERION,
), fair_play_weights=(("yellow_cards", 1), ("red_cards", 3)),
    note="LFP art. 518 TER: fewer disciplinary points wins.")

LIGA_PORTUGAL = _standings("LIGA_PORTUGAL", StandingsMetric.POINTS, (
    Criterion(("h2h_points",)), Criterion(("h2h_goal_diff",), meetings=2),
    GOAL_DIFFERENCE_CRITERION, WINS_CRITERION, GOALS_FOR_CRITERION,
))

EREDIVISIE = _standings("EREDIVISIE", StandingsMetric.POINTS, (
    _metric_criterion("negative_games_played"), GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
    Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for", "h2h_away_goals")),
    _metric_criterion("alphabetical_order"),
), note="KNVB art. 17 table in season: fewer games played first, then goal difference, goals, head-to-head and alphabetical order.")

SAUDI_PRO_LEAGUE = _standings("SAUDI_PRO_LEAGUE", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, _unavailable_criterion("MISSING_PREVIOUS_SEASON_RANK"),
),
    fair_play_weights=(("standalone_yellow_cards", 1),
                       ("second_yellow_dismissals", 3),
                       ("direct_red_cards", 3)),
    note="SPL table in season: goal difference, goals scored, then previous-season rank (not available). Card weights (yellow 1, second-yellow dismissal 3, direct red 3; SPL 2025/26 Appendix 2) are kept for reporting.")

BRASILEIRAO = _standings("BRASILEIRAO", StandingsMetric.POINTS, (
    WINS_CRITERION, GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_two=True),
    _metric_criterion("negative_red_cards"), _metric_criterion("negative_yellow_cards"),
))

LIGA_MX_CLAUSURA = _standings("LIGA_MX_CLAUSURA", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, _metric_criterion("balanced_away_goals"), FAIR_PLAY_CRITERION,
    _unavailable_criterion("MISSING_MULTISEASON_QUOTIENT"),
),
    fair_play_weights=(("standalone_yellow_cards", 1),
                       ("second_yellow_dismissals", 3),
                       ("direct_red_cards", 3)),
    note="FMF 2026/27 art. 25: GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, away goals at equal away-matchday counts, Fair Play, quotient, official draw. Unequal away counts require the official matchday cutoff, which the loaded records do not supply.")
LIGA_MX_APERTURA = replace(
    LIGA_MX_CLAUSURA, name="LIGA_MX_APERTURA",
)

_ARGENTINA_FAIR_PLAY = (FAIR_PLAY_CRITERION, _metric_criterion("negative_fair_play_red_cards"))
_ARGENTINA_FAIR_PLAY_WEIGHTS = (
    ("standalone_yellow_cards", 1), ("second_yellow_dismissals", 4), ("direct_red_cards", 5),
)
ARGENTINA_ZONES = _standings("ARGENTINA_ZONES", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for"), reapply=True),
    *_ARGENTINA_FAIR_PLAY,
),
    fair_play_weights=_ARGENTINA_FAIR_PLAY_WEIGHTS, require_complete_fair_play=True,
    note="AFA arts. 10, 16 and 25. Fair play uses the available card totals for the zone phases.")

ARGENTINA_ANNUAL = _standings(
    "ARGENTINA_ANNUAL",
    StandingsMetric.POINTS,
    (GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, *_ARGENTINA_FAIR_PLAY),
    fair_play_weights=_ARGENTINA_FAIR_PLAY_WEIGHTS,
    require_complete_fair_play=True,
    note="AFA arts. 24–25. Internal fair play uses available card totals for both zone phases.",
)

MLS = _standings("MLS", StandingsMetric.POINTS, (
    WINS_CRITERION, GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, Criterion(("h2h_points", "h2h_goal_diff"), reapply=True),
    FAIR_PLAY_CRITERION, _metric_criterion("away_goal_diff"), _metric_criterion("away_goals"),
    _metric_criterion("home_goal_diff"), _metric_criterion("home_goals"),
),
    fair_play_weights=(("standalone_yellow_cards", 3),
                       ("second_yellow_dismissals", 7),
                       ("direct_red_cards", 7)),
    note="Internal card-only fair play: yellow 3, second-yellow dismissal 7, direct red 7. Excludes fouls, technical-staff warnings, supplemental discipline, and player-level incident caps/pairing. Conference standings; HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE applies only inside a conference. Cancellations/PPG require an explicit ruling.")

SHL = _standings("SHL", StandingsMetric.POINTS, (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for")),
), record_percentage=True)

NHL = _standings("NHL", StandingsMetric.POINTS, (
    _metric_criterion("negative_games_played"), _metric_criterion("regulation_wins"),
    _metric_criterion("regulation_overtime_wins"), WINS_CRITERION, _metric_criterion("nhl_h2h"), GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
), record_percentage=True)

AHL = _standings("AHL", StandingsMetric.POINTS, (
    _metric_criterion("regulation_wins"), _metric_criterion("regulation_overtime_wins"), WINS_CRITERION,
    _metric_criterion("ahl_h2h"), GOAL_DIFFERENCE_CRITERION,
    Criterion(("h2h_goals_for",), only_two=True),
    Criterion(("h2h_goal_diff",), only_multiple=True),
    _unavailable_criterion("MISSING_CONFERENCE_POINTS_PERCENTAGE"),
), record_percentage=True, restart_sequence="pair",
    note="Division standings; multi-team ties restart when reduced to two.")

EUROLEAGUE = _standings("EUROLEAGUE", StandingsMetric.WINS, (
    _metric_criterion("negative_games_played"),
    Criterion(("h2h_wins", "h2h_goal_diff"), meetings=2, reapply=True),
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, _metric_criterion("game_score_quotients"),
),
    record_percentage=True, regulation_scores_only=True, restart_sequence="complete_meetings",
    note="Article 19: exclude OT scores; sanctions are not represented in match results.")

NBA = _standings("NBA", StandingsMetric.WIN_PERCENTAGE, (
    _metric_criterion("nba_tiebreak"),
), record_percentage=True,
    note="Two-team HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE precedes division winner; multi-team ties reverse that order. No generic GOAL_DIFFERENCE_CRITERION fallback.")

MLB = _standings("MLB", StandingsMetric.WIN_PERCENTAGE, (
    _metric_criterion("mlb_tiebreak"),
), record_percentage=True,
    note="Two/three/four-club procedures. Last-half comparisons require the actual All-Star break boundary.")

NFL = _standings("NFL", StandingsMetric.WIN_PERCENTAGE, (
    _metric_criterion("nfl_tiebreak"),
), record_percentage=True,
    note="Division and multi-division procedures preserve division seeding and restart after each selected club. Net touchdowns and official draws require additional data.")

CBA = _standings("CBA", StandingsMetric.WIN_PERCENTAGE, (
    _unavailable_criterion("CURRENT_CBA_TIEBREAK_REGULATION_UNCONFIRMED"),
),
    record_percentage=True, verified=False,
    note="Do not import NBA/EuroLeague tiebreaks or mistake a WCBA regulation for CBA.")

# Not tracked: retain basic ordering without claiming a verified current policy.
LIIGA = _standings("LIIGA", StandingsMetric.POINTS, (
    _metric_criterion("regulation_wins"), GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
    Criterion(("h2h_points", "h2h_regulation_wins", "h2h_goal_diff", "h2h_goals_for")),
), record_percentage=True, verified=False,
    note="Liiga competition rules section 27: after the overall criteria, compare HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE points, regulation wins, goal difference and goals scored. Full edition verification remains pending.")
WNBA = _standings("WNBA", StandingsMetric.WIN_PERCENTAGE, (), record_percentage=True, verified=False)
PFL = _standings("PFL", StandingsMetric.POINTS, (GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, WINS_CRITERION), verified=False)
