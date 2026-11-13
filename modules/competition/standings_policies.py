"""Configured standings orders used by current competition profiles.

Metrics are evaluated by standings_rules, not by the catalogue. An unavailable
step records an actual data/implementation limit, rather than a substitute rule.
Sporting boundaries and outstanding verification are reviewed separately.
"""

from dataclasses import replace

from .competitive_rules import RankingCriterion as Criterion, StandingsRules
from .rule_terms import StandingsMetric


def _metric(name):
    return Criterion((name,))


def _missing(reason):
    return Criterion((), unavailable=reason)


GD = _metric("goal_diff")
GF = _metric("goals_for")
WINS = _metric(StandingsMetric.WINS)
FAIR_PLAY = _metric("negative_fair_play_points")
H2H = Criterion(("h2h_points", "h2h_goal_diff"), meetings=2)

LALIGA = StandingsRules("LALIGA", StandingsMetric.POINTS, (
    Criterion(("h2h_goal_diff",), meetings=2, only_two=True),
    Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_multiple=True, reapply=True),
    GD, GF, FAIR_PLAY,
), fair_play_weights=(("standalone_yellow_cards", 1), ("second_yellow_dismissals", 2), ("direct_red_cards", 3)),
    note="Internal card-based fair play; compares available match scores.")

SERIE_A = StandingsRules("SERIE_A", StandingsMetric.POINTS, (H2H, GD, GF))

PREMIER_LEAGUE = StandingsRules("PREMIER_LEAGUE", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_away_goals"), meetings=2),
),
    note="C.7/C.17: head-to-head applies only across title, relegation or qualification boundaries.")

BUNDESLIGA = StandingsRules("BUNDESLIGA", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_goal_diff", "h2h_away_goals"), meetings=2, share_if_incomplete=True),
    _metric("away_goals"),
))

LIGUE_1 = StandingsRules("LIGUE_1", StandingsMetric.POINTS, (
    GD, H2H, GF, WINS, _metric("away_wins"), FAIR_PLAY,
), fair_play_weights=(("yellow_cards", 1), ("red_cards", 3)),
    note="LFP art. 518 TER: fewer disciplinary points wins.")

LIGA_PORTUGAL = StandingsRules("LIGA_PORTUGAL", StandingsMetric.POINTS, (
    Criterion(("h2h_points",)), Criterion(("h2h_goal_diff",), meetings=2),
    GD, WINS, GF,
))

EREDIVISIE = StandingsRules("EREDIVISIE", StandingsMetric.POINTS, (
    _metric("negative_games_played"), GD, GF,
    Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for", "h2h_away_goals")),
    _metric("alphabetical_order"),
), note="KNVB art. 17 table in season: fewer games played first, then goal difference, goals, head-to-head and alphabetical order.")

SAUDI_PRO_LEAGUE = StandingsRules("SAUDI_PRO_LEAGUE", StandingsMetric.POINTS, (
    GD, GF, _missing("MISSING_PREVIOUS_SEASON_RANK"),
),
    fair_play_weights=(("standalone_yellow_cards", 1),
                       ("second_yellow_dismissals", 3),
                       ("direct_red_cards", 3)),
    note="SPL table in season: goal difference, goals scored, then previous-season rank (not available). Card weights (yellow 1, second-yellow dismissal 3, direct red 3; SPL 2025/26 Appendix 2) are kept for reporting.")

BRASILEIRAO = StandingsRules("BRASILEIRAO", StandingsMetric.POINTS, (
    WINS, GD, GF, Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_two=True),
    _metric("negative_red_cards"), _metric("negative_yellow_cards"),
))

LIGA_MX_CLAUSURA = StandingsRules("LIGA_MX_CLAUSURA", StandingsMetric.POINTS, (
    GD, GF, _metric("balanced_away_goals"), FAIR_PLAY,
    _missing("MISSING_MULTISEASON_QUOTIENT"),
),
    fair_play_weights=(("standalone_yellow_cards", 1),
                       ("second_yellow_dismissals", 3),
                       ("direct_red_cards", 3)),
    note="FMF 2026/27 art. 25: GD, GF, away goals at equal away-matchday counts, Fair Play, quotient, official draw. Unequal away counts require the official matchday cutoff, which the loaded records do not supply.")
LIGA_MX_APERTURA = replace(
    LIGA_MX_CLAUSURA, name="LIGA_MX_APERTURA",
)

_ARGENTINA_FAIR_PLAY = (FAIR_PLAY, _metric("negative_fair_play_red_cards"))
_ARGENTINA_FAIR_PLAY_WEIGHTS = (
    ("standalone_yellow_cards", 1), ("second_yellow_dismissals", 4), ("direct_red_cards", 5),
)
ARGENTINA_ZONES = StandingsRules("ARGENTINA_ZONES", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for"), reapply=True),
    *_ARGENTINA_FAIR_PLAY,
),
    fair_play_weights=_ARGENTINA_FAIR_PLAY_WEIGHTS, require_complete_fair_play=True,
    note="AFA arts. 10, 16 and 25. Fair play uses the available card totals for the zone phases.")

ARGENTINA_ANNUAL = StandingsRules(
    "ARGENTINA_ANNUAL",
    StandingsMetric.POINTS,
    (GD, GF, *_ARGENTINA_FAIR_PLAY),
    fair_play_weights=_ARGENTINA_FAIR_PLAY_WEIGHTS,
    require_complete_fair_play=True,
    note="AFA arts. 24–25. Internal fair play uses available card totals for both zone phases.",
)

MLS = StandingsRules("MLS", StandingsMetric.POINTS, (
    WINS, GD, GF, Criterion(("h2h_points", "h2h_goal_diff"), reapply=True),
    FAIR_PLAY, _metric("away_goal_diff"), _metric("away_goals"),
    _metric("home_goal_diff"), _metric("home_goals"),
),
    fair_play_weights=(("standalone_yellow_cards", 3),
                       ("second_yellow_dismissals", 7),
                       ("direct_red_cards", 7)),
    note="Internal card-only fair play: yellow 3, second-yellow dismissal 7, direct red 7. Excludes fouls, technical-staff warnings, supplemental discipline, and player-level incident caps/pairing. Conference standings; H2H applies only inside a conference. Cancellations/PPG require an explicit ruling.")

SHL = StandingsRules("SHL", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for")),
), record_percentage=True)

NHL = StandingsRules("NHL", StandingsMetric.POINTS, (
    _metric("negative_games_played"), _metric("regulation_wins"),
    _metric("regulation_overtime_wins"), WINS, _metric("nhl_h2h"), GD, GF,
), record_percentage=True)

AHL = StandingsRules("AHL", StandingsMetric.POINTS, (
    _metric("regulation_wins"), _metric("regulation_overtime_wins"), WINS,
    _metric("ahl_h2h"), GD,
    Criterion(("h2h_goals_for",), only_two=True),
    Criterion(("h2h_goal_diff",), only_multiple=True),
    _missing("MISSING_CONFERENCE_POINTS_PERCENTAGE"),
), record_percentage=True,
    note="Division standings; multi-team ties restart when reduced to two.")

EUROLEAGUE = StandingsRules("EUROLEAGUE", StandingsMetric.WINS, (
    _metric("negative_games_played"),
    Criterion(("h2h_wins", "h2h_goal_diff"), meetings=2, reapply=True),
    GD, GF, _metric("game_score_quotients"),
),
    record_percentage=True, regulation_scores_only=True,
    note="Article 19: exclude OT scores; sanctions are not represented in match results.")

NBA = StandingsRules("NBA", StandingsMetric.WIN_PERCENTAGE, (
    _metric("nba_tiebreak"),
), record_percentage=True,
    note="Two-team H2H precedes division winner; multi-team ties reverse that order. No generic GD fallback.")

MLB = StandingsRules("MLB", StandingsMetric.WIN_PERCENTAGE, (
    _metric("mlb_tiebreak"),
), record_percentage=True,
    note="Two/three/four-club procedures. Last-half comparisons require the actual All-Star break boundary.")

NFL = StandingsRules("NFL", StandingsMetric.WIN_PERCENTAGE, (
    _metric("nfl_tiebreak"),
), record_percentage=True,
    note="Division and multi-division procedures preserve division seeding and restart after each selected club. Net touchdowns and official draws require additional data.")

CBA = StandingsRules("CBA", StandingsMetric.WIN_PERCENTAGE, (
    _missing("CURRENT_CBA_TIEBREAK_REGULATION_UNCONFIRMED"),
),
    record_percentage=True, verified=False,
    note="Do not import NBA/EuroLeague tiebreaks or mistake a WCBA regulation for CBA.")

# Not tracked: retain basic ordering without claiming a verified current policy.
LIIGA = StandingsRules("LIIGA", StandingsMetric.POINTS, (
    _metric("regulation_wins"), GD, GF,
    Criterion(("h2h_points", "h2h_regulation_wins", "h2h_goal_diff", "h2h_goals_for")),
), record_percentage=True, verified=False,
    note="Liiga competition rules section 27: after the overall criteria, compare H2H points, regulation wins, goal difference and goals scored. Full edition verification remains pending.")
WNBA = StandingsRules("WNBA", StandingsMetric.WIN_PERCENTAGE, (), record_percentage=True, verified=False)
PFL = StandingsRules("PFL", StandingsMetric.POINTS, (GD, GF, WINS), verified=False)
