"""Configured standings orders used by current competition profiles.

Each policy declares the sequence used in season (the published table) and the one
used once the whole season is played (100 %), which defines the final classification.
Metrics are evaluated by standings_rules, not by the catalogue. An unavailable step
records an actual data/implementation limit or an official resolution at the exact
point where it would apply; later steps are only declared when they can be reached.
Sporting boundaries and outstanding verification are reviewed separately.
"""


from dataclasses import replace

from .competitive_rules import RankingCriterion as Criterion, StandingsRules
from .rule_terms import (
    FairPlayMetric,
    IncompleteMeetingsAction,
    RankingIssue,
    RankingMetric,
    RestartSequence,
    StandingsMetric,
)


# ``closing=SAME_AS_IN_SEASON`` declares a policy whose final order repeats the in-season one.
SAME_AS_IN_SEASON = object()


def _standings(name, primary_metric, *, in_season, closing, **options):
    """Policy with both sequences declared: ``in_season`` (partial table) and ``closing`` (complete season)."""
    return StandingsRules(
        name, primary_metric, in_season, in_season if closing is SAME_AS_IN_SEASON else closing, **options,
    )


def _metric_criterion(name):
    return Criterion((name,))


def _unavailable_criterion(reason, **options):
    return Criterion((), unavailable=reason, **options)


def _use_in_season_if_incomplete(criterion):
    """Closing criterion that hands the clubs still tied to the in-season order if a meeting is missing."""
    return replace(criterion, incomplete_meetings_action=IncompleteMeetingsAction.USE_IN_SEASON)


GOAL_DIFFERENCE_CRITERION = _metric_criterion(RankingMetric.GOAL_DIFF)
GOALS_FOR_CRITERION = _metric_criterion(RankingMetric.GOALS_FOR)
WINS_CRITERION = _metric_criterion(StandingsMetric.WINS)
FAIR_PLAY_CRITERION = _metric_criterion(RankingMetric.NEGATIVE_FAIR_PLAY_POINTS)
HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE = Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF), meetings=2)

OFFICIAL_DRAW_PENDING = _unavailable_criterion(RankingIssue.OFFICIAL_DRAW_PENDING)
PLAYOFF_MATCH_PENDING = _unavailable_criterion(RankingIssue.PLAYOFF_MATCH_PENDING)
OFFICIAL_TIEBREAK_PENDING = _unavailable_criterion(RankingIssue.OFFICIAL_TIEBREAK_PENDING)

_LALIGA_HEAD_TO_HEAD = (
    Criterion((RankingMetric.H2H_GOAL_DIFF,), meetings=2, only_two=True),
    Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF), meetings=2, only_multiple=True, reapply=True),
)
LALIGA = _standings(
    "LALIGA", StandingsMetric.POINTS,
    in_season=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION),
    closing=(
        *map(_use_in_season_if_incomplete, _LALIGA_HEAD_TO_HEAD),
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, FAIR_PLAY_CRITERION,
    ),
    restart_sequence=RestartSequence.PAIR_AFTER_REAPPLY,
    fair_play_weights=((FairPlayMetric.STANDALONE_YELLOW_CARDS, 1), (FairPlayMetric.SECOND_YELLOW_DISMISSALS, 2), (FairPlayMetric.DIRECT_RED_CARDS, 3)),
    note="In season: published table (points, goal difference, goals scored). Closing: RFEF art. 10 with internal card-based fair play; pending when sanctions are missing.",
)

SERIE_A = _standings(
    "SERIE_A", StandingsMetric.POINTS,
    in_season=(GOAL_DIFFERENCE_CRITERION,),
    closing=(
        _use_in_season_if_incomplete(replace(HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE, pending_across_boundaries=True)),
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, OFFICIAL_DRAW_PENDING,
    ),
    note="In season: published table. Closing: a points tie across the title or the 17th/18th line orders its candidates by the mini-table and leaves their places to the official resolution.",
)

PREMIER_LEAGUE = _standings(
    "PREMIER_LEAGUE", StandingsMetric.POINTS,
    in_season=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION),
    closing=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        _use_in_season_if_incomplete(
            Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_AWAY_GOALS), meetings=2, only_across_boundaries=True),
        ),
        _unavailable_criterion(RankingIssue.PLAYOFF_MATCH_PENDING, only_across_boundaries=True),
    ),
    note="C.7/C.17: head-to-head applies only across title, relegation or qualification boundaries; elsewhere the place is shared.",
)

BUNDESLIGA = _standings(
    "BUNDESLIGA", StandingsMetric.POINTS,
    in_season=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION),
    closing=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        Criterion((RankingMetric.H2H_GOAL_DIFF, RankingMetric.H2H_AWAY_GOALS), meetings=2, incomplete_meetings_action=IncompleteMeetingsAction.SHARE_POSITION),
        _metric_criterion(RankingMetric.AWAY_GOALS), PLAYOFF_MATCH_PENDING,
    ),
)

_LIGUE_1_IN_SEASON = (
    GOAL_DIFFERENCE_CRITERION, HEAD_TO_HEAD_POINTS_AND_GOAL_DIFFERENCE, GOALS_FOR_CRITERION,
    WINS_CRITERION, _metric_criterion(RankingMetric.AWAY_WINS), FAIR_PLAY_CRITERION,
)
LIGUE_1 = _standings(
    "LIGUE_1", StandingsMetric.POINTS,
    in_season=_LIGUE_1_IN_SEASON,
    closing=(*_LIGUE_1_IN_SEASON, OFFICIAL_DRAW_PENDING),
    fair_play_weights=((FairPlayMetric.YELLOW_CARDS, 1), (FairPlayMetric.RED_CARDS, 3)),
    note="LFP art. 518 TER: without both legs the head-to-head is skipped; fewer disciplinary points wins.",
)

LIGA_PORTUGAL = _standings(
    "LIGA_PORTUGAL", StandingsMetric.POINTS, in_season=(
        Criterion((RankingMetric.H2H_POINTS,)), Criterion((RankingMetric.H2H_GOAL_DIFF,), meetings=2),
        GOAL_DIFFERENCE_CRITERION, WINS_CRITERION, GOALS_FOR_CRITERION,
    ),
    closing=(
        _use_in_season_if_incomplete(Criterion((RankingMetric.H2H_POINTS,), meetings=2)),
        _use_in_season_if_incomplete(Criterion((RankingMetric.H2H_GOAL_DIFF,), meetings=2)),
        GOAL_DIFFERENCE_CRITERION, WINS_CRITERION, GOALS_FOR_CRITERION, OFFICIAL_TIEBREAK_PENDING,
    ),
)

_EREDIVISIE_HEAD_TO_HEAD = Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF, RankingMetric.H2H_GOALS_FOR, RankingMetric.H2H_AWAY_GOALS))
EREDIVISIE = _standings(
    "EREDIVISIE", StandingsMetric.POINTS, in_season=(
        _metric_criterion(RankingMetric.NEGATIVE_GAMES_PLAYED), GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        _EREDIVISIE_HEAD_TO_HEAD, _metric_criterion(RankingMetric.ALPHABETICAL_ORDER),
    ),
    closing=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        _use_in_season_if_incomplete(replace(_EREDIVISIE_HEAD_TO_HEAD, meetings=2)),
        PLAYOFF_MATCH_PENDING,
    ),
    note="KNVB art. 17: in season fewer games played first and alphabetical order last; closing uses complete head-to-head and a play-off.",
)

SAUDI_PRO_LEAGUE = _standings(
    "SAUDI_PRO_LEAGUE", StandingsMetric.POINTS, in_season=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, _unavailable_criterion(RankingIssue.MISSING_PREVIOUS_SEASON_RANK),
    ),
    closing=(
        Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF, RankingMetric.H2H_GOALS_FOR), reapply=True, complete_block=True),
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        _unavailable_criterion(RankingIssue.EXTRA_TIME_OR_PENALTIES_PENDING, only_across_boundaries=True),
        FAIR_PLAY_CRITERION, OFFICIAL_TIEBREAK_PENDING,
    ),
    fair_play_weights=((FairPlayMetric.STANDALONE_YELLOW_CARDS, 1),
                       (FairPlayMetric.SECOND_YELLOW_DISMISSALS, 3),
                       (FairPlayMetric.DIRECT_RED_CARDS, 3)),
    note="SPL §9: the closing order applies once the whole season is played. The extra-time or penalty step applies only across a sporting boundary. Card weights: yellow 1, second-yellow dismissal 3, direct red 3 (2025/26 Appendix 2).",
)

_BRASILEIRAO_IN_SEASON = (WINS_CRITERION, GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION)
BRASILEIRAO = _standings(
    "BRASILEIRAO", StandingsMetric.POINTS, in_season=_BRASILEIRAO_IN_SEASON,
    closing=(
        *_BRASILEIRAO_IN_SEASON,
        _use_in_season_if_incomplete(Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF), meetings=2, only_two=True)),
        _metric_criterion(RankingMetric.NEGATIVE_RED_CARDS), _metric_criterion(RankingMetric.NEGATIVE_YELLOW_CARDS),
        OFFICIAL_DRAW_PENDING,
    ),
)

LIGA_MX_CLAUSURA = _standings(
    "LIGA_MX_CLAUSURA", StandingsMetric.POINTS, in_season=(GOAL_DIFFERENCE_CRITERION,),
    closing=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, _metric_criterion(RankingMetric.BALANCED_AWAY_GOALS),
        FAIR_PLAY_CRITERION, _unavailable_criterion(RankingIssue.MISSING_MULTISEASON_QUOTIENT),
    ),
    fair_play_weights=((FairPlayMetric.STANDALONE_YELLOW_CARDS, 1),
                       (FairPlayMetric.SECOND_YELLOW_DISMISSALS, 3),
                       (FairPlayMetric.DIRECT_RED_CARDS, 3)),
    note="FMF 2026/27 art. 25: GD, GF, away goals at equal away-matchday counts, Fair Play, quotient, official draw. Unequal away counts require the official matchday cutoff, which the loaded records do not supply. The draw follows the pending quotient.",
)
LIGA_MX_APERTURA = replace(
    LIGA_MX_CLAUSURA, name="LIGA_MX_APERTURA",
)
LIGA_MX_ANNUAL = replace(
    LIGA_MX_CLAUSURA,
    name="LIGA_MX_ANNUAL",
    in_season_criteria=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION),
    closing_projection_criteria=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        _unavailable_criterion(RankingIssue.MISSING_BALANCED_AWAY_GOALS),
    ),
    fair_play_weights=(),
    note="Concacaf aggregate of Apertura and Clausura. Visitor matchday evidence is unavailable in the aggregate.",
)

_ARGENTINA_FAIR_PLAY = (FAIR_PLAY_CRITERION, _metric_criterion(RankingMetric.NEGATIVE_FAIR_PLAY_RED_CARDS))
_ARGENTINA_FAIR_PLAY_WEIGHTS = (
    (FairPlayMetric.STANDALONE_YELLOW_CARDS, 1), (FairPlayMetric.SECOND_YELLOW_DISMISSALS, 4), (FairPlayMetric.DIRECT_RED_CARDS, 5),
)
ARGENTINA_ZONES = _standings(
    "ARGENTINA_ZONES", StandingsMetric.POINTS, in_season=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION),
    closing=(
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF, RankingMetric.H2H_GOALS_FOR), reapply=True),
        *_ARGENTINA_FAIR_PLAY, OFFICIAL_DRAW_PENDING,
    ),
    fair_play_weights=_ARGENTINA_FAIR_PLAY_WEIGHTS, require_complete_fair_play=True,
    note="AFA arts. 10, 16 and 25. Fair play uses the available card totals for the zone phases.",
)

ARGENTINA_ANNUAL = _standings(
    "ARGENTINA_ANNUAL", StandingsMetric.POINTS, in_season=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION),
    closing=(GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, *_ARGENTINA_FAIR_PLAY, OFFICIAL_DRAW_PENDING),
    fair_play_weights=_ARGENTINA_FAIR_PLAY_WEIGHTS,
    require_complete_fair_play=True,
    note="AFA arts. 24–25. Internal fair play uses available card totals for both zone phases.",
)

_MLS_IN_SEASON = (WINS_CRITERION, GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION)
MLS = _standings(
    "MLS", StandingsMetric.POINTS, in_season=_MLS_IN_SEASON,
    closing=(
        *_MLS_IN_SEASON, _metric_criterion(RankingMetric.NEGATIVE_FAIR_PLAY_POINTS_PER_GAME),
        _metric_criterion(RankingMetric.AWAY_GOAL_DIFF), _metric_criterion(RankingMetric.AWAY_GOALS),
        _metric_criterion(RankingMetric.HOME_GOAL_DIFF), _metric_criterion(RankingMetric.HOME_GOALS),
        OFFICIAL_DRAW_PENDING,
    ),
    fair_play_weights=((FairPlayMetric.STANDALONE_YELLOW_CARDS, 3),
                       (FairPlayMetric.SECOND_YELLOW_DISMISSALS, 7),
                       (FairPlayMetric.DIRECT_RED_CARDS, 7)),
    note="Internal card-only discipline per match: yellow 3, second-yellow dismissal 7, direct red 7. Fouls, technical-staff warnings and supplemental discipline are not loaded, so the step is pending when card totals are missing. Conference standings. Cancellations/PPG require an explicit ruling.",
)

_SHL_IN_SEASON = (
    GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
    Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_GOAL_DIFF, RankingMetric.H2H_GOALS_FOR)),
)
SHL = _standings(
    "SHL", StandingsMetric.POINTS, in_season=_SHL_IN_SEASON,
    closing=(*_SHL_IN_SEASON, OFFICIAL_TIEBREAK_PENDING), record_percentage=True,
)

NHL = _standings(
    "NHL", StandingsMetric.POINTS, in_season=(
        _metric_criterion(RankingMetric.NEGATIVE_GAMES_PLAYED), _metric_criterion(RankingMetric.REGULATION_WINS),
        _metric_criterion(RankingMetric.REGULATION_OVERTIME_WINS), WINS_CRITERION, _metric_criterion(RankingMetric.NHL_H2H),
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
    ),
    closing=SAME_AS_IN_SEASON,
    record_percentage=True,
)

AHL = _standings(
    "AHL", StandingsMetric.POINTS, in_season=(_unavailable_criterion(RankingIssue.AHL_IN_SEASON_TIEBREAK_UNCONFIRMED),),
    closing=(
        _metric_criterion(RankingMetric.REGULATION_WINS), _metric_criterion(RankingMetric.REGULATION_OVERTIME_WINS), WINS_CRITERION,
        _metric_criterion(RankingMetric.AHL_H2H), GOAL_DIFFERENCE_CRITERION,
        Criterion((RankingMetric.H2H_GOALS_FOR,), only_two=True),
        Criterion((RankingMetric.H2H_GOAL_DIFF,), only_multiple=True),
        _metric_criterion(RankingMetric.AHL_CONFERENCE_POINTS_PERCENTAGE),
    ),
    record_percentage=True, restart_sequence=RestartSequence.PAIR,
    note="Division standings; multi-team ties restart when reduced to two. The first in-season tiebreaker is provisional and unconfirmed.",
)

EUROLEAGUE = _standings(
    "EUROLEAGUE", StandingsMetric.WINS, in_season=(
        _metric_criterion(RankingMetric.NEGATIVE_GAMES_PLAYED),
        Criterion((RankingMetric.H2H_WINS, RankingMetric.H2H_GOAL_DIFF), meetings=2, reapply=True),
        GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION, _metric_criterion(RankingMetric.GAME_SCORE_QUOTIENTS),
    ),
    closing=SAME_AS_IN_SEASON,
    record_percentage=True, regulation_scores_only=True, restart_sequence=RestartSequence.COMPLETE_MEETINGS,
    note="Article 19: scores compared are the four regulation quarters; sanctions are not represented in match results and must be documented before they take precedence.",
)

NBA = _standings(
    "NBA", StandingsMetric.WIN_PERCENTAGE, in_season=(Criterion((RankingMetric.H2H_WIN_PERCENTAGE,), only_two=True),),
    closing=(_metric_criterion(RankingMetric.NBA_TIEBREAK),),
    record_percentage=True,
    note="In season only a two-team head-to-head breaks a tie. Closing: two-team H2H precedes division winner; multi-team ties reverse that order. No generic GD fallback.",
)

MLB = _standings(
    "MLB", StandingsMetric.WIN_PERCENTAGE, in_season=(_metric_criterion(RankingMetric.MLB_IN_SEASON_TIEBREAK),),
    closing=(_metric_criterion(RankingMetric.MLB_TIEBREAK),),
    record_percentage=True,
    note="Two/three/four-club procedures. The closing step that goes back from the All-Star break is pending without the edition's official date.",
)

NFL = _standings(
    "NFL", StandingsMetric.WIN_PERCENTAGE, in_season=(_metric_criterion(RankingMetric.NFL_IN_SEASON_TIEBREAK),),
    closing=(_metric_criterion(RankingMetric.NFL_TIEBREAK),),
    record_percentage=True,
    note="Division and multi-division procedures preserve division seeding and restart after each selected club. In season the branch stops at the common-game record; net touchdowns and official draws require additional data.",
)

CBA = _standings(
    "CBA", StandingsMetric.POINTS, in_season=(_unavailable_criterion(RankingIssue.CURRENT_CBA_TIEBREAK_REGULATION_UNCONFIRMED),),
    closing=SAME_AS_IN_SEASON,
    record_percentage=True, verified=False,
    note="Classification points: two per win, one per ordinary loss. Do not import NBA/EuroLeague tiebreaks or mistake a WCBA regulation for CBA.",
)

# Not tracked: retain basic ordering without claiming a verified current policy.
LIIGA = _standings(
    "LIIGA", StandingsMetric.POINTS, in_season=(
        _metric_criterion(RankingMetric.REGULATION_WINS), GOAL_DIFFERENCE_CRITERION, GOALS_FOR_CRITERION,
        Criterion((RankingMetric.H2H_POINTS, RankingMetric.H2H_REGULATION_WINS, RankingMetric.H2H_GOAL_DIFF, RankingMetric.H2H_GOALS_FOR)),
    ),
    closing=SAME_AS_IN_SEASON,
    record_percentage=True, verified=False,
    note="Liiga competition rules section 27: after the overall criteria, compare H2H points, regulation wins, goal difference and goals scored. Full edition verification remains pending.",
)
WNBA = _standings(
    "WNBA", StandingsMetric.WIN_PERCENTAGE, in_season=(Criterion((RankingMetric.H2H_WIN_PERCENTAGE,)),),
    closing=(
        Criterion((RankingMetric.H2H_WIN_PERCENTAGE,), reapply=True),
        _unavailable_criterion(RankingIssue.MISSING_FINAL_RECORD_VS_WINNING_TEAMS),
    ),
    record_percentage=True, verified=False,
    note="Closing: after head-to-head the record against clubs finishing .500 or better needs the final records; point difference in head-to-head games and overall point difference follow it.",
)
PFL = _standings(
    "PFL", StandingsMetric.POINTS, in_season=(_unavailable_criterion(RankingIssue.PFL_TIEBREAK_ORDER_UNCONFIRMED),),
    closing=SAME_AS_IN_SEASON, verified=False,
)
