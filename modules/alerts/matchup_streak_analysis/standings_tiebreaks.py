"""Branching tiebreak procedures which cannot be represented by one sort key.

All inputs come from the already loaded regular-season matches. No repositories,
network calls, qualification projections or invented last-resort ordering.
"""

from collections import defaultdict
from fractions import Fraction

from .constants import get_team_group


class TieBreakRecords:
    """One indexed view of the match records used by the three procedures."""

    def __init__(self, matches, scoring):
        self.games = defaultdict(list)
        for match in matches:
            home, away = match["home_team"], match["away_team"]
            hs, aws = match["home_score"], match["away_score"]
            winner = match.get("winner") or ("1" if hs > aws else "2" if aws > hs else "X")
            overtime = match.get("result_subtype") in {"OT", "SO"}
            win = scoring.overtime_win if overtime else scoring.regulation_win
            loss = scoring.overtime_loss if overtime else scoring.loss
            maximum = Fraction(str(scoring.maximum_per_game))
            home_win = Fraction(str(scoring.draw if winner == "X" else win if winner == "1" else loss)) / maximum
            away_win = Fraction(str(scoring.draw if winner == "X" else win if winner == "2" else loss)) / maximum
            self.games[home].append((away, home_win, hs, aws))
            self.games[away].append((home, away_win, aws, hs))

    def against(self, team, opponents=None):
        return [game for game in self.games[team] if opponents is None or game[0] in opponents]

    def percentage(self, team, opponents=None):
        games = self.against(team, opponents)
        return sum((game[1] for game in games), Fraction(0)) / len(games) if games else Fraction(0)

    def members(self, method, group):
        return {team for team in self.games if get_team_group(team, method) == group}

    def net_points(self, team, opponents=None):
        return sum(scored - allowed for _, _, scored, allowed in self.against(team, opponents))


def _resolve(names, criteria_factory):
    """Reapply the procedure to remaining tied clubs after a partial split."""
    if len(names) == 1:
        return [(names, None)]
    for compare in criteria_factory(names):
        values, reason = compare()
        if reason:
            return [(names, reason)]
        buckets = defaultdict(list)
        for name in names:
            buckets[values[name]].append(name)
        if len(buckets) > 1:
            return [cluster for value in sorted(buckets, reverse=True)
                    for cluster in _resolve(buckets[value], criteria_factory)]
    return [(names, "OFFICIAL_TIEBREAK_UNRESOLVED")]


def _comparison(names, measure):
    def compare():
        values = {team: measure(team) for team in names}
        return values, None
    return compare


def _unavailable(reason):
    return lambda: ({}, reason)


def nba_tiebreak(names, records):
    """NBA two-/multi-team orders, including division winners and eligible teams."""
    def group(team):
        return get_team_group(team, "nba_conference")

    def division(team):
        return get_team_group(team, "nba_division")

    def eligible(conference):
        members = records.members("nba_conference", conference)
        percentages = sorted((records.percentage(team) for team in members), reverse=True)
        cutoff = percentages[min(9, len(percentages) - 1)] if percentages else Fraction(0)
        return {team for team in members if records.percentage(team) >= cutoff}

    def division_winner(team):
        members = records.members("nba_division", division(team))
        best = max((records.percentage(name) for name in members), default=Fraction(0))
        leaders = [name for name in members if records.percentage(name) == best]
        if team not in leaders:
            return 0
        resolved = _resolve(leaders, lambda tied: criteria(tied, choosing_division=True))
        first, reason = resolved[0]
        return None if reason or len(first) > 1 else int(team == first[0])

    def criteria(tied, choosing_division=False):
        h2h = _comparison(tied, lambda team: records.percentage(team, tied))
        if any(not group(team) or not division(team) for team in tied):
            return ([h2h] if len(tied) == 2 else []) + [_unavailable("MISSING_NBA_GROUP_MEMBERSHIP")]

        def leaders():
            values = {team: division_winner(team) for team in tied}
            return values, "UNRESOLVED_NBA_DIVISION_WINNER" if None in values.values() else None

        sequence = []
        if len(tied) == 2:
            sequence.append(h2h)
        if not choosing_division:
            sequence.append(leaders)
        if len(tied) > 2:
            sequence.append(h2h)
        if len({division(team) for team in tied}) == 1:
            sequence.append(_comparison(tied, lambda team: records.percentage(team, records.members("nba_division", division(team)))))
        sequence.extend((
            _comparison(tied, lambda team: records.percentage(team, records.members("nba_conference", group(team)))),
            _comparison(tied, lambda team: records.percentage(team, eligible(group(team)))),
        ))
        if len(tied) == 2:
            sequence.append(_comparison(tied, lambda team: records.percentage(team, eligible("Western" if group(team) == "Eastern" else "Eastern"))))
        sequence.append(_comparison(tied, records.net_points))
        return sequence

    return _resolve(list(names), criteria)


def mlb_tiebreak(names, records):
    """Two-/three-/four-club orders up to the All-Star-break-dependent step."""
    def criteria(tied):
        if len(tied) > 4:
            return [_unavailable("UNIMPLEMENTED_MLB_FIVE_PLUS_TEAM_PROCEDURE")]
        if any(not get_team_group(team, "mlb_division") or not get_team_group(team, "mlb_league") for team in tied):
            return [_comparison(tied, lambda team: records.percentage(team, tied)), _unavailable("MISSING_MLB_GROUP_MEMBERSHIP")]
        def beats_each(team):
            return int(all(records.against(team, {opponent}) and records.percentage(team, {opponent}) > Fraction(1, 2)
                           for opponent in tied if opponent != team))

        return ([_comparison(tied, beats_each)] if len(tied) > 2 else []) + [
            _comparison(tied, lambda team: records.percentage(team, tied)),
            _comparison(tied, lambda team: records.percentage(team, records.members("mlb_division", get_team_group(team, "mlb_division")))),
            _comparison(tied, lambda team: records.percentage(team, records.members("mlb_league", get_team_group(team, "mlb_league")))),
            _unavailable("MISSING_ALL_STAR_BREAK_BOUNDARY"),
        ]
    remaining, ranked = list(names), []
    while remaining:
        first, reason = _resolve(remaining, criteria)[0]
        if reason or len(first) != 1:
            return ranked + [(remaining, reason or "OFFICIAL_TIEBREAK_UNRESOLVED")]
        ranked.append((list(first), None))
        remaining.remove(first[0])
    return ranked


def nfl_tiebreak(names, records):
    """Division and wild-card procedures; advance one club, then restart."""
    def division(team):
        return get_team_group(team, "nfl_division")

    def conference(team):
        return get_team_group(team, "nfl_conference")

    def opponent_strength(team, victories_only):
        opponents = [opponent for opponent, win, _, _ in records.games[team]
                     if not victories_only or win == 1]
        games = [game for opponent in opponents for game in records.games[opponent]]
        return sum((game[1] for game in games), Fraction(0)) / len(games) if games else Fraction(0)

    def combined_rank(team, same_conference):
        members = records.members("nfl_conference", conference(team)) if same_conference else set(records.games)
        scored = {name: sum(game[2] for game in records.games[name]) for name in members}
        allowed = {name: sum(game[3] for game in records.games[name]) for name in members}
        return -(2 + sum(value > scored[team] for value in scored.values())
                 + sum(value < allowed[team] for value in allowed.values()))

    def criteria(tied):
        if any(not division(team) or not conference(team) for team in tied):
            return [_unavailable("MISSING_NFL_GROUP_MEMBERSHIP")]
        same_division = len({division(team) for team in tied}) == 1
        common = set.intersection(*(set(game[0] for game in records.games[team]) for team in tied))
        common_metric = _comparison(tied, lambda team: records.percentage(team, common))
        conf_metric = _comparison(tied, lambda team: records.percentage(team, records.members("nfl_conference", conference(team))))
        def sweep(team):
            against = {opponent: records.against(team, {opponent}) for opponent in tied if opponent != team}
            if all(games and all(game[1] == 1 for game in games) for games in against.values()):
                return 1
            if all(games and all(game[1] == 0 for game in games) for games in against.values()):
                return -1
            return 0

        sequence = [_comparison(tied, sweep if not same_division and len(tied) > 2
                                else lambda team: records.percentage(team, tied))]
        if same_division:
            sequence.extend((
                _comparison(tied, lambda team: records.percentage(team, records.members("nfl_division", division(team)))),
                common_metric, conf_metric,
            ))
        else:
            sequence.append(conf_metric)
            if all(len(records.against(team, common)) >= 4 for team in tied):
                sequence.append(common_metric)
        sequence.extend((
            _comparison(tied, lambda team: opponent_strength(team, True)),
            _comparison(tied, lambda team: opponent_strength(team, False)),
            _comparison(tied, lambda team: combined_rank(team, True)),
            _comparison(tied, lambda team: combined_rank(team, False)),
            _comparison(tied, lambda team: records.net_points(team, common if same_division else records.members("nfl_conference", conference(team)))),
            _comparison(tied, records.net_points),
            _unavailable("MISSING_NET_TOUCHDOWNS"),
        ))
        return sequence

    def one_at_a_time(tied):
        remaining, ranked = list(tied), []
        while remaining:
            first, reason = _resolve(remaining, criteria)[0]
            if reason or len(first) != 1:
                return ranked + [(remaining, reason or "OFFICIAL_TIEBREAK_UNRESOLVED")]
            ranked.append((list(first), None))
            remaining.remove(first[0])
        return ranked

    if any(not division(team) for team in names):
        return [(list(names), "MISSING_NFL_GROUP_MEMBERSHIP")]
    by_division = defaultdict(list)
    for team in names:
        by_division[division(team)].append(team)
    if len(by_division) == 1:
        return one_at_a_time(names)
    # Keep the original division seeding for later wild-card selections. Only
    # its highest remaining club can enter a cross-division comparison.
    queues = {group: one_at_a_time(teams) for group, teams in by_division.items()}
    ranked = []
    while queues:
        candidates = []
        for queue in queues.values():
            first, reason = queue[0]
            if reason or len(first) != 1:
                remaining = [team for q in queues.values() for teams, _ in q for team in teams]
                return ranked + [(remaining, reason or "UNRESOLVED_NFL_DIVISION_SEED")]
            candidates.extend(first)
        first, reason = _resolve(candidates, criteria)[0]
        if reason or len(first) != 1:
            remaining = [team for q in queues.values() for teams, _ in q for team in teams]
            return ranked + [(remaining, reason or "OFFICIAL_TIEBREAK_UNRESOLVED")]
        ranked.append((list(first), None))
        group = division(first[0])
        queues[group].pop(0)
        if not queues[group]:
            del queues[group]
    return ranked


COMPLEX_TIEBREAKS = {"nba_tiebreak": nba_tiebreak, "mlb_tiebreak": mlb_tiebreak, "nfl_tiebreak": nfl_tiebreak}
