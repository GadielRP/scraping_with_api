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
PENDING_FAIR_PLAY = _missing("MISSING_DISCIPLINARY_RECORD")
DECIDER = Criterion((), unavailable="OFFICIAL_DECIDER_REQUIRED", final_only=True)
H2H = Criterion(("h2h_points", "h2h_goal_diff"), meetings=2)

LALIGA = StandingsRules("LALIGA", StandingsMetric.POINTS, (
    Criterion(("h2h_goal_diff",), meetings=2, only_two=True),
    Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_multiple=True, reapply=True),
    GD, GF, PENDING_FAIR_PLAY, DECIDER,
))

SERIE_A = StandingsRules("SERIE_A", StandingsMetric.POINTS, (H2H, GD, GF, DECIDER), note="Final title and 17th/18th-place ties can require a decider; UEFA-finalist exception applies.")

PREMIER_LEAGUE = StandingsRules("PREMIER_LEAGUE", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_away_goals"), meetings=2), DECIDER,
),
    in_season_criteria=(GD, GF),
    note="C.7/C.17: final H2H is required only across title, relegation or qualification boundaries; cup/EPS reallocations require the effective qualification boundaries.")

BUNDESLIGA = StandingsRules("BUNDESLIGA", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_goal_diff", "h2h_away_goals"), meetings=2, share_if_incomplete=True),
    _metric("away_goals"), DECIDER,
))

LIGUE_1 = StandingsRules("LIGUE_1", StandingsMetric.POINTS, (
    GD, H2H, GF, WINS, _metric("away_wins"), FAIR_PLAY, DECIDER,
), fair_play_weights=(("yellow_cards", 1), ("red_cards", 3)),
    note="LFP art. 518 TER: fewer disciplinary points wins.")

LIGA_PORTUGAL = StandingsRules("LIGA_PORTUGAL", StandingsMetric.POINTS, (
    Criterion(("h2h_points",)), Criterion(("h2h_goal_diff",), meetings=2),
    GD, WINS, GF, DECIDER,
))

EREDIVISIE = StandingsRules("EREDIVISIE", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for", "h2h_away_goals")), DECIDER,
),
    in_season_criteria=(
        _metric("negative_games_played"), GD, GF,
        Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for", "h2h_away_goals")),
        _metric("alphabetical_order"),
    ), note="Interim and final tables use different criteria.")

SAUDI_PRO_LEAGUE = StandingsRules("SAUDI_PRO_LEAGUE", StandingsMetric.POINTS, (
    Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for"), reapply=True),
    GD, GF, PENDING_FAIR_PLAY, DECIDER,
),
    in_season_criteria=(GD, GF, _missing("MISSING_PREVIOUS_SEASON_RANK")))

BRASILEIRAO = StandingsRules("BRASILEIRAO", StandingsMetric.POINTS, (
    WINS, GD, GF, Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_two=True),
    _metric("negative_red_cards"), _metric("negative_yellow_cards"), DECIDER,
))

LIGA_MX_CLAUSURA = StandingsRules("LIGA_MX_CLAUSURA", StandingsMetric.POINTS, (
    GD, GF, _metric("balanced_away_goals"), PENDING_FAIR_PLAY,
    _missing("MISSING_MULTISEASON_QUOTIENT"), DECIDER,
),
    note="FMF 2026/27 art. 25: GD, GF, away goals at equal away-matchday counts, Fair Play, quotient, official draw. Unequal away counts require the official matchday cutoff, which the loaded records do not supply.")
LIGA_MX_APERTURA = replace(
    LIGA_MX_CLAUSURA, name="LIGA_MX_APERTURA",
)

ARGENTINA_ZONES = StandingsRules("ARGENTINA_ZONES", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for"), reapply=True),
    PENDING_FAIR_PLAY, DECIDER,
),
    note="Articles 10/16 apply to zones only; annual/relegation tables are different.")

MLS = StandingsRules("MLS", StandingsMetric.POINTS, (
    WINS, GD, GF, Criterion(("h2h_points", "h2h_goal_diff"), reapply=True),
    PENDING_FAIR_PLAY, _metric("away_goal_diff"), _metric("away_goals"),
    _metric("home_goal_diff"), _metric("home_goals"), DECIDER,
),
    note="Conference standings; H2H applies only inside a conference. Cancellations/PPG require an explicit ruling.")

SHL = StandingsRules("SHL", StandingsMetric.POINTS, (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for")), DECIDER,
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
