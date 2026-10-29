"""Published standings orders used by current competition profiles.

Metrics are evaluated by standings_rules, not by the catalogue. An unavailable
step records an actual data/implementation limit, rather than a substitute rule.
Source editions and outstanding verification are documented in the audit.
"""

from dataclasses import replace

from .competitive_rules import RankingCriterion as Criterion, StandingsRules


def _metric(name):
    return Criterion((name,))


def _missing(reason):
    return Criterion((), unavailable=reason)


GD = _metric("goal_diff")
GF = _metric("goals_for")
WINS = _metric("wins")
FAIR_PLAY = _missing("MISSING_DISCIPLINARY_RECORD")
DECIDER = Criterion((), unavailable="OFFICIAL_DECIDER_REQUIRED", final_only=True)
H2H = Criterion(("h2h_points", "h2h_goal_diff"), meetings=2)

LALIGA = StandingsRules("LALIGA", "points", (
    Criterion(("h2h_goal_diff",), meetings=2, only_two=True),
    Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_multiple=True, reapply=True),
    GD, GF, FAIR_PLAY, DECIDER,
), ("https://rfef.es/sites/default/files/2025-05/1262_Reglamento_General__Futbol__CD_24.04.2025__1_.pdf",))

SERIE_A = StandingsRules("SERIE_A", "points", (H2H, GD, GF, DECIDER), (
    "https://files.figc.it/version/c:ZmUzYzk0MzUtMzU0Zi00:YTk3YzZiZGEtYmExYi00/244%20-%20Deroga%20art.%2051%20NOIF%20-%20Determinazione%20classifica%20Campionato%20Serie%20A%20ss%202026%20-%202027.pdf",
), note="Final title and 17th/18th-place ties can require a decider; UEFA-finalist exception applies.")

PREMIER_LEAGUE = StandingsRules("PREMIER_LEAGUE", "points", (
    GD, GF, Criterion(("h2h_points", "h2h_away_goals"), meetings=2), DECIDER,
), ("https://www.premierleague.com/en/news/4638196/could-the-premier-league-title-be-won-on-goal-difference",),
    in_season_criteria=(GD, GF))

BUNDESLIGA = StandingsRules("BUNDESLIGA", "points", (
    GD, GF, Criterion(("h2h_goal_diff", "h2h_away_goals"), meetings=2, share_if_incomplete=True),
    _metric("away_goals"), DECIDER,
), ("https://media.dfl.de/sites/2/2026/06/Spielordnung-SpOL-2026-06-11-Stand.pdf",))

LIGUE_1 = StandingsRules("LIGUE_1", "points", (
    GD, H2H, GF, WINS, _metric("away_wins"), FAIR_PLAY, DECIDER,
), ("https://www.lfp.fr/assets/26_27_Reglement_Competitions_08_09_2026_45d0881157.pdf",))

LIGA_PORTUGAL = StandingsRules("LIGA_PORTUGAL", "points", (
    Criterion(("h2h_points",)), Criterion(("h2h_goal_diff",), meetings=2),
    GD, WINS, GF, DECIDER,
), ("https://www.ligaportugal.pt/backoffice/assets/20260701_RC_2026_27_f53785bcd4.pdf",))

EREDIVISIE = StandingsRules("EREDIVISIE", "points", (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for", "h2h_away_goals")), DECIDER,
), ("https://www.knvb.nl/downloads/bestand/29888/reglementen-betaald-voetbal-seizoen-2025-26versie-januari-2026",),
    in_season_criteria=(
        _metric("negative_games_played"), GD, GF,
        Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for", "h2h_away_goals")),
        _metric("alphabetical_order"),
    ), note="July 2026 contents despite the historical URL; article 17 distinguishes interim/final tables.")

SAUDI_PRO_LEAGUE = StandingsRules("SAUDI_PRO_LEAGUE", "points", (
    Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for"), reapply=True),
    GD, GF, FAIR_PLAY, DECIDER,
), ("https://images.spl.com.sa/image/private/fl_attachment/prd/yn2olap5ftvbexvoarxu.pdf",),
    in_season_criteria=(GD, GF, _missing("MISSING_PREVIOUS_SEASON_RANK")))

BRASILEIRAO = StandingsRules("BRASILEIRAO", "points", (
    WINS, GD, GF, Criterion(("h2h_points", "h2h_goal_diff"), meetings=2, only_two=True),
    _missing("MISSING_RED_CARDS"), _missing("MISSING_YELLOW_CARDS"), DECIDER,
), ("https://stcbfsiteprdimgbrs.blob.core.windows.net/img-site/cdn/REC_Brasileiro_Serie_A_2026_c984f8cf05.pdf",))

LIGA_MX_CLAUSURA = StandingsRules("LIGA_MX_CLAUSURA", "points", (
    GD, GF, _metric("away_goals"), Criterion(("h2h_points", "h2h_goal_diff")),
    _missing("MISSING_MULTISEASON_QUOTIENT"), FAIR_PLAY, DECIDER,
), ("https://fmf.mx/docs/reglamentos/483.pdf",),
    note="Verified Clausura 2026 (article 14).")
LIGA_MX_APERTURA = replace(
    LIGA_MX_CLAUSURA, name="LIGA_MX_APERTURA", verified=False,
    note="Provisional criteria from Clausura 2026; Apertura 2026 regulation requires confirmation.",
)

ARGENTINA_ZONES = StandingsRules("ARGENTINA_ZONES", "points", (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for"), reapply=True),
    FAIR_PLAY, DECIDER,
), ("https://www.ligaprofesional.ar/wp-content/uploads/2026/01/Reglamento-Torneos-LPF-Primera-2026-1.pdf",),
    note="Articles 10/16 apply to zones only; annual/relegation tables are different.")

MLS = StandingsRules("MLS", "points", (
    WINS, GD, GF, Criterion(("h2h_points", "h2h_goal_diff"), reapply=True),
    FAIR_PLAY, _metric("away_goal_diff"), _metric("away_goals"),
    _metric("home_goal_diff"), _metric("home_goals"), DECIDER,
), ("https://www.mlssoccer.com/league-reports/competition-guidelines/",),
    note="Conference standings; H2H applies only inside a conference. Cancellations/PPG require an explicit ruling.")

SHL = StandingsRules("SHL", "points", (
    GD, GF, Criterion(("h2h_points", "h2h_goal_diff", "h2h_goals_for")), DECIDER,
), ("https://www.swehockey.se/media/hbzpznld/taevlingsbestaemmelser-2026-2027.pdf",), record_percentage=True)

NHL = StandingsRules("NHL", "points", (
    _metric("negative_games_played"), _metric("regulation_wins"),
    _metric("regulation_overtime_wins"), WINS, _metric("nhl_h2h"), GD, GF,
), ("https://www.nhl.com/info/standings-info/tie-breaking-procedure",), record_percentage=True)

AHL = StandingsRules("AHL", "points", (
    _metric("regulation_wins"), _metric("regulation_overtime_wins"), WINS,
    _metric("ahl_h2h"), GD,
    Criterion(("h2h_goals_for",), only_two=True),
    Criterion(("h2h_goal_diff",), only_multiple=True),
    _missing("MISSING_CONFERENCE_POINTS_PERCENTAGE"),
), ("https://theahl.com/stats/standings",), record_percentage=True,
    note="Division standings; multi-team ties restart when reduced to two.")

EUROLEAGUE = StandingsRules("EUROLEAGUE", "wins", (
    _metric("negative_games_played"),
    Criterion(("h2h_wins", "h2h_goal_diff"), meetings=2, reapply=True),
    GD, GF, _metric("game_score_quotients"),
), ("https://ftpserver.euroleague.net/general/2026_27_EuroLeague_Bylaws.pdf",),
    record_percentage=True, regulation_scores_only=True,
    note="Article 19: exclude OT scores; sanctions are not represented in match results.")

NBA = StandingsRules("NBA", "pct", (
    _metric("nba_tiebreak"),
), ("https://www.nba.com/standings",), record_percentage=True,
    note="Two-team H2H precedes division winner; multi-team ties reverse that order. No generic GD fallback.")

MLB = StandingsRules("MLB", "pct", (
    _metric("mlb_tiebreak"),
), ("https://www.mlb.com/news/mlb-playoff-tiebreaker-rules",), record_percentage=True,
    note="Two/three/four-club procedures. Last-half comparisons require the actual All-Star break boundary.")

NFL = StandingsRules("NFL", "pct", (
    _metric("nfl_tiebreak"),
), ("https://www.nfl.com/standings/tie-breaking-procedures",), record_percentage=True,
    note="Division and multi-division procedures preserve division seeding and restart after each selected club. Net touchdowns and official draws require additional data.")

CBA = StandingsRules("CBA", "pct", (
    _missing("CURRENT_CBA_TIEBREAK_REGULATION_UNCONFIRMED"),
), ("https://cbaleague.com/", "https://www.cba.net.cn/cbapxb/index.jhtml?competitionId=54&seasonId=1474"),
    record_percentage=True, verified=False,
    note="Do not import NBA/EuroLeague tiebreaks or mistake a WCBA regulation for CBA.")

# Not tracked: retain basic ordering without claiming a verified current policy.
LIIGA = StandingsRules("LIIGA", "points", (
    _metric("regulation_wins"), GD, GF,
), ("https://liiga.fi/",), record_percentage=True, verified=False)
WNBA = StandingsRules("WNBA", "pct", (), ("https://www.wnba.com/standings",), record_percentage=True, verified=False)
PFL = StandingsRules("PFL", "points", (GD, GF, WINS), ("repository:previous-competition-configuration",), verified=False)
