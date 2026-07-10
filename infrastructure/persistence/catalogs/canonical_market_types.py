"""Seed catalog for the canonical market type reference table."""

from __future__ import annotations

FULL_TIME_PERIODS = {"match", "full time", "full-time"}
FIRST_HALF_PERIODS = {"1st half", "p1"}
SECOND_HALF_PERIODS = {"2nd half", "p2"}
FIRST_QUARTER_PERIODS = {"1st quarter", "p1"}

# Fields persisted into the canonical_market_types table.
PERSISTED_SEED_FIELDS = frozenset(
    {
        "canonical_market_name",
        "canonical_market_group",
        "canonical_market_period",
        "market_family",
        "requires_choice_group",
        "enabled_for_ingestion",
        "enabled_for_trajectory",
        "display_order",
    }
)


def _seed(
    name,
    group,
    period,
    family,
    requires_group,
    trajectory,
    order,
    sofascore_match=None,
    oddspapi_match=None,
):
    return {
        "canonical_market_name": name,
        "canonical_market_group": group,
        "canonical_market_period": period,
        "market_family": family,
        "requires_choice_group": requires_group,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": trajectory,
        "display_order": order,
        "sofascore_match": sofascore_match,
        "oddspapi_match": oddspapi_match,
    }


def persisted_seed_values(values: dict) -> dict:
    """Return only DB-column fields from a catalog seed entry."""
    return {key: values[key] for key in PERSISTED_SEED_FIELDS if key in values}


# Applied by seed/migration before upserting CANONICAL_MARKET_TYPE_SEEDS.
# Maps deprecated primary keys -> current canonical keys.
CANONICAL_MARKET_KEY_RENAMES = {
    "corners_2_way_full_time": "total_corners_full_time",
}


CANONICAL_MARKET_TYPE_SEEDS = {
    "1x2_full_time": _seed(
        name="1X2 Full Time",
        group="1X2",
        period="Full Time",
        family="side_3way",
        requires_group=False,
        trajectory=True,
        order=10,
        sofascore_match={
            "market_name": {"full time"},
            "market_group": {"1x2"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            # moneyline is intentionally excluded: old resolver rejected 3-way moneylines.
            "market_type": {"1x2", "homeaway", "matchwinner", "winner"},
            "period_lock": "Full Time",
        },
    ),
    "1x2_1st_half": _seed(
        name="1X2 1st Half",
        group="1X2",
        period="1st Half",
        family="side_3way",
        requires_group=False,
        trajectory=True,
        order=11,
        sofascore_match={
            "market_name": {"1st half"},
            "market_group": {"1x2"},
            "market_period": FIRST_HALF_PERIODS,
        },
        oddspapi_match={
            "market_type": {"1x2", "homeaway", "matchwinner", "winner"},
            "period_lock": "1st Half",
        },
    ),
    "1x2_1st_quarter": _seed(
        name="1X2 1st Quarter",
        group="1X2",
        period="1st Quarter",
        family="side_3way",
        requires_group=False,
        trajectory=False,
        order=12,
        sofascore_match={
            "market_name": {"1st quarter winner"},
            "market_group": {"home/away"},
            "market_period": FIRST_QUARTER_PERIODS,
        },
        oddspapi_match={
            "market_type": {"1stquarterwinner"},
            "period_lock": "1st Quarter",
        },
    ),
    "home_away_full_time": _seed(
        name="Home/Away Full Time",
        group="Home/Away",
        period="Full Time",
        family="side_2way",
        requires_group=False,
        trajectory=True,
        order=20,
        sofascore_match={
            "market_name": {"full time"},
            "market_group": {"home/away"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"moneyline", "homeaway", "matchwinner", "winner"},
            # OT / extras variants are either dedicated seeds or intentionally unsupported.
            "exclude_market_names": {
                "winner (incl. overtime)",
                "full time (including overtime)",
                "winner (incl. overtime and penalties)",
                "winner (incl. extra innings)",
                "winner (incl. super over)",
                "second half winner (incl. overtime)",
                "fourth quarter winner (incl. overtime)",
                "first map winner (incl. overtime)",
                "second map winner (incl. overtime)",
                "third map winner (incl. overtime)",
                "fourth map winner (incl. overtime)",
                "fifth map winner (incl. overtime)",
            },
            "period_lock": "Full Time",
        },
    ),
    "home_away_1st_half": _seed(
        name="Home/Away 1st Half",
        group="Home/Away",
        period="1st Half",
        family="side_2way",
        requires_group=False,
        trajectory=True,
        order=21,
        sofascore_match={
            "market_name": {"1st half"},
            "market_group": {"home/away"},
            "market_period": FIRST_HALF_PERIODS,
        },
        oddspapi_match={
            "market_type": {"moneyline", "homeaway", "matchwinner", "winner"},
            "period_lock": "1st Half",
        },
    ),
    "home_away_1st_quarter": _seed(
        name="Home/Away 1st Quarter",
        group="Home/Away",
        period="1st Quarter",
        family="side_2way",
        requires_group=False,
        trajectory=False,
        order=22,
        sofascore_match={
            "market_name": {"1st quarter winner"},
            "market_group": {"home/away"},
            "market_period": FIRST_QUARTER_PERIODS,
        },
        oddspapi_match={
            "market_type": {"moneyline", "homeaway", "matchwinner", "winner"},
            "period_lock": "1st Quarter",
        },
    ),
    "home_away_full_time_including_overtime": _seed(
        name="Home/Away Full Time Including Overtime",
        group="Home/Away",
        period="Full Time Including Overtime",
        family="side_2way",
        requires_group=False,
        trajectory=False,
        order=23,
        sofascore_match={
            "market_name": {"full time (including overtime)"},
            "market_group": {"full time (including overtime)"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"moneyline", "fulltime(includingovertime)"},
            "market_name": {
                "winner (incl. overtime)",
                "full time (including overtime)",
            },
            "period_lock": "Full Time",
        },
    ),
    "first_set_winner_1st_set": _seed(
        name="First Set Winner 1st Set",
        group="First Set Winner",
        period="1st Set",
        family="side_2way",
        requires_group=False,
        trajectory=False,
        order=24,
        oddspapi_match={
            "market_type": {"firstsetwinner"},
            "period_lock": "1st Set",
        },
    ),
    "current_set_winner_current_set": _seed(
        name="Current Set Winner Current Set",
        group="Current Set Winner",
        period="Current Set",
        family="side_2way",
        requires_group=False,
        trajectory=False,
        order=25,
        oddspapi_match={
            "market_type": {"currentsetwinner"},
            "period_lock": "Current Set",
        },
    ),
    "over_under_full_time": _seed(
        name="Over/Under Full Time",
        group="Over/Under",
        period="Full Time",
        family="total",
        requires_group=True,
        trajectory=True,
        order=30,
        sofascore_match={
            "market_name": {"game total", "total points", "match goals"},
            "market_group": {"over/under", "match goals"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {
                "totals",
                "gametotal",
                "totalpoints",
                "matchgoals",
                "totals-points",
            },
            # OT / extras totals are not regulation full-time O/U.
            "exclude_market_names": {
                "over under (incl. overtime)",
                "total (incl. overtime)",
                "total (incl. overtime and penalties)",
                "over under (incl. extra innings)",
                "over under frames",
                "over under rounds",
                "total sets over under",
                "total sets",
            },
            "period_lock": "Full Time",
        },
    ),
    "sets_over_under_full_time": _seed(
        name="Sets Over/Under Full Time",
        group="Total Sets",
        period="Full Time",
        family="total",
        requires_group=True,
        trajectory=False,
        order=31,
        sofascore_match={
            "market_name": {"total sets", "match sets", "sets total"},
            "market_group": {"over/under", "total sets", "sets total"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"totals"},
            "market_name": {
                "total sets over under",
                "total sets",
            },
            "period_lock": "Full Time",
        },
    ),
    "over_under_full_time_including_overtime": _seed(
        name="Over/Under Full Time Including Overtime",
        group="Over/Under",
        period="Full Time Including Overtime",
        family="total",
        requires_group=True,
        trajectory=False,
        order=31,
        oddspapi_match={
            "market_type": {"totals"},
            "market_name": {
                "over under (incl. overtime)",
                "total (incl. overtime)",
            },
            "period_lock": "Full Time",
        },
    ),
    "over_under_1st_half": _seed(
        name="Over/Under 1st Half",
        group="Over/Under",
        period="1st Half",
        family="total",
        requires_group=True,
        trajectory=True,
        order=32,
        oddspapi_match={
            "market_type": {"totals"},
            "period_lock": "1st Half",
        },
    ),
    "over_under_1st_quarter": _seed(
        name="Over/Under 1st Quarter",
        group="Over/Under",
        period="1st Quarter",
        family="total",
        requires_group=True,
        trajectory=True,
        order=33,
        oddspapi_match={
            "market_type": {"totals", "totals-points"},
            "period_lock": "1st Quarter",
        },
    ),
    "over_under_1st_period": _seed(
        name="Over/Under 1st Period",
        group="Over/Under",
        period="1st Period",
        family="total",
        requires_group=True,
        trajectory=False,
        order=34,
        oddspapi_match={
            "market_type": {"1stperiodgoals"},
            "period_lock": "1st Period",
        },
    ),
    "total_cards_full_time": _seed(
        name="Total Cards Full Time",
        group="Total Cards",
        period="Full Time",
        family="total",
        requires_group=True,
        trajectory=False,
        order=34,
        sofascore_match={
            "market_name": {"cards in match"},
            "market_group": {"total cards"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"totalcards", "totals-bookings"},
            "period_lock": "Full Time",
        },
    ),
    "total_corners_full_time": _seed(
        name="Total Corners Full Time",
        group="Total Corners",
        period="Full Time",
        family="total",
        requires_group=True,
        trajectory=False,
        order=35,
        sofascore_match={
            # Provider (SofaScore) still uses "Corners 2-Way" naming.
            "market_name": {"corners 2 way"},
            "market_group": {"corners 2 way"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"corners2way", "totals-corners"},
            "period_lock": "Full Time",
        },
    ),
    "total_sets_games_extra_time": _seed(
        name="Total Sets/Games Extra Time",
        group="Total Sets/Games",
        period="Extra Time",
        family="total",
        requires_group=True,
        trajectory=False,
        order=36,
        oddspapi_match={
            "market_type": {"totalsets/games"},
            "period_lock": "Extra Time",
        },
    ),
    "team_total_home_full_time": _seed(
        name="Team Total Home Full Time",
        group="Over/Under Team 1",
        period="Full Time",
        family="team_total",
        requires_group=True,
        trajectory=True,
        order=37,
        oddspapi_match={
            "market_type": {"teamtotals-team1"},
            "market_name": {
                "over under team 1",
                "team 1 total",
            },
            "period_lock": "Full Time",
        },
    ),
    "team_total_away_full_time": _seed(
        name="Team Total Away Full Time",
        group="Over/Under Team 2",
        period="Full Time",
        family="team_total",
        requires_group=True,
        trajectory=True,
        order=38,
        oddspapi_match={
            "market_type": {"teamtotals-team2"},
            "market_name": {
                "over under team 2",
                "team 2 total",
            },
            "period_lock": "Full Time",
        },
    ),
    "team_total_home_full_time_including_overtime": _seed(
        name="Team Total Home Full Time Including Overtime",
        group="Over/Under Team 1",
        period="Full Time Including Overtime",
        family="team_total",
        requires_group=True,
        trajectory=False,
        order=39,
        oddspapi_match={
            "market_type": {"teamtotals-team1"},
            "market_name": {
                "over under team 1 (incl. overtime)",
            },
            "period_lock": "Full Time",
        },
    ),
    "team_total_away_full_time_including_overtime": _seed(
        name="Team Total Away Full Time Including Overtime",
        group="Over/Under Team 2",
        period="Full Time Including Overtime",
        family="team_total",
        requires_group=True,
        trajectory=False,
        order=40,
        oddspapi_match={
            "market_type": {"teamtotals-team2"},
            "market_name": {
                "over under team 2 (incl. overtime)",
            },
            "period_lock": "Full Time",
        },
    ),
    "asian_handicap_full_time": _seed(
        name="Asian Handicap Full Time",
        group="Asian Handicap",
        period="Full Time",
        family="spread_2way",
        requires_group=True,
        trajectory=True,
        order=50,
        sofascore_match={
            "market_name": {"asian handicap", "point spread"},
            "market_group": {"asian handicap", "point spread"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"spreads", "spreads-points"},
            "market_name": {
                "asian handicap",
                "point handicap",
            },
            "period_lock": "Full Time",
        },
    ),
    "asian_handicap_1st_half": _seed(
        name="Asian Handicap 1st Half",
        group="Asian Handicap",
        period="1st Half",
        family="spread_2way",
        requires_group=True,
        trajectory=True,
        order=51,
        oddspapi_match={
            "market_type": {"spreads"},
            "market_name": {
                "asian handicap first half",
            },
            "period_lock": "1st Half",
        },
    ),
    "asian_handicap_full_time_including_overtime": _seed(
        name="Asian Handicap Full Time Including Overtime",
        group="Asian Handicap",
        period="Full Time Including Overtime",
        family="spread_2way",
        requires_group=True,
        trajectory=False,
        order=52,
        oddspapi_match={
            "market_type": {"spreads"},
            "market_name": {
                "asian handicap (incl. overtime)",
            },
            "period_lock": "Full Time",
        },
    ),
    "european_handicap_full_time": _seed(
        name="European Handicap Full Time",
        group="European Handicap",
        period="Full Time",
        family="side_3way",
        requires_group=True,
        trajectory=False,
        order=53,
        oddspapi_match={
            "market_type": {"spreads-european"},
            "market_name": {"european handicap"},
            "exclude_market_names": {
                "european handicap (incl. overtime and penalties)",
            },
            "period_lock": "Full Time",
        },
    ),
    "draw_no_bet_full_time": _seed(
        name="Draw No Bet Full Time",
        group="Draw No Bet",
        period="Full Time",
        family="side_2way",
        requires_group=False,
        trajectory=False,
        order=60,
        sofascore_match={
            "market_name": {"draw no bet"},
            "market_group": {"draw no bet"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"drawnobet"},
            "period_lock": "Full Time",
        },
    ),
    "double_chance_full_time": _seed(
        name="Double Chance Full Time",
        group="Double Chance",
        period="Full Time",
        family="side_combo",
        requires_group=False,
        trajectory=False,
        order=61,
        sofascore_match={
            "market_name": {"double chance"},
            "market_group": {"double chance"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"doublechance"},
            "period_lock": "Full Time",
        },
    ),
    "both_teams_to_score_full_time": _seed(
        name="Both Teams To Score Full Time",
        group="Both Teams To Score",
        period="Full Time",
        family="decision",
        requires_group=False,
        trajectory=False,
        order=62,
        sofascore_match={
            "market_name": {"both teams to score"},
            "market_group": {"both teams to score"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"bothteamsscore"},
            "period_lock": "Full Time",
        },
    ),
    "both_teams_to_score_full_time_including_overtime": _seed(
        name="Both Teams To Score Full Time (Incl. Overtime)",
        group="Both Teams To Score",
        period="Full Time Including Overtime",
        family="decision",
        requires_group=False,
        trajectory=False,
        order=63,
        sofascore_match={
            "market_name": {"both teams to score (incl. overtime and penalties)"},
            "market_group": {"both teams to score"},
            "market_period": {"Full Time"},
        },
        oddspapi_match={
            "market_type": {"bothteamsscore"},
            "market_name": {"both teams to score (incl. overtime and penalties)"},
            "period_lock": "Full Time",
        },
    ),
    "first_goal_full_time": _seed(
        name="First Goal Full Time",
        group="First Goal",
        period="Full Time",
        family="goal_team",
        requires_group=False,
        trajectory=False,
        order=70,
        oddspapi_match={
            "exclude_market_names": {"First Goal (incl. overtime and penalties)"},
            "market_type": {"firstgoal"},
            "period_lock": "Full Time",
            "outcome_role_sets": [{"1", "no_goal", "2"}],
        },
    ),
    "last_goal_full_time": _seed(
        name="Last Goal Full Time",
        group="Last Goal",
        period="Full Time",
        family="goal_team",
        requires_group=False,
        trajectory=False,
        order=71,
        oddspapi_match={
            "exclude_market_names": {"last goal (incl. overtime and penalties)"},
            "market_type": {"lastteamtoscore", "lastgoal"},
            "period_lock": "Full Time",
            "outcome_role_sets": [{"1", "no_goal", "2"}],
        },
    ),
    "first_team_to_score_full_time": _seed(
        name="First Team To Score Full Time",
        group="First Team To Score",
        period="Full Time",
        family="goal_team",
        requires_group=False,
        trajectory=False,
        order=72,
        sofascore_match={
            "market_name": {"first team to score"},
            "market_group": {"first team to score"},
            "market_period": FULL_TIME_PERIODS,
        },
        oddspapi_match={
            "market_type": {"firstteamtoscore", "firsttoscorearun"},
            "period_lock": "Full Time",
            "skip_outcome_validation": True,
        },
    ),
    "next_goal_full_time": _seed(
        name="Next Goal Full Time",
        group="Next Goal",
        period="Full Time",
        family="goal_team",
        requires_group=False,
        trajectory=False,
        order=73,
        oddspapi_match={
            "market_type": {"nextgoal"},
            "period_lock": "Full Time",
            "skip_outcome_validation": True,
        },
    ),
    "tie_break_in_match_extra_time": _seed(
        name="Tie Break In Match Extra Time",
        group="Tie Break In Match",
        period="Extra Time",
        family="decision",
        requires_group=False,
        trajectory=False,
        order=80,
        oddspapi_match={
            "market_type": {"tiebreakinmatch"},
            "period_lock": "Extra Time",
            "outcome_role_sets": [{"1", "2"}, {"yes", "no"}],
        },
    ),
}


def get_canonical_market_type_seed(canonical_market_key: str) -> dict | None:
    seed = CANONICAL_MARKET_TYPE_SEEDS.get(canonical_market_key)
    return dict(seed) if seed is not None else None


def get_canonical_market_type_seeds() -> dict:
    return {key: dict(value) for key, value in CANONICAL_MARKET_TYPE_SEEDS.items()}
