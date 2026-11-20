"""Pure evaluation of the ordered standings criteria in a competition profile."""


from collections import Counter, defaultdict
from fractions import Fraction
from itertools import combinations

from modules.competition.rule_terms import (
    GroupingMethod,
    IncompleteMeetingsAction,
    ObjectiveType,
    RankingIssue,
    RankingMetric,
    RankingStatus,
    RestartSequence,
    StandingsMetric,
)

from modules.competition.competition_group_memberships import AHL_CONFERENCE_BY_DIVISION, get_team_group
from .tiebreaks import COMPLEX_TIEBREAKS, TieBreakRecords


def normalize_result_subtype(raw_subtype, winner):
    """Normalize source result subtypes to REG/OT/SO/DRAW."""
    if winner == "X":
        return "DRAW"
    value = str(raw_subtype or "").strip().upper()
    if value in {"DRAW", "X", "TIE"}:
        return "DRAW"
    if "SO" in value or "SHOOT" in value:
        return "SO"
    if "OT" in value or "EXTRA" in value:
        return "OT"
    return "REG"


def build_head_to_head_stats(team_names, match_records, scoring, *, regulation_scores_only=False):
    """Summarize supplied games using shared scoring and the actual game winner."""
    result = {name: dict(points=0, wins=0, draws=0, losses=0, ot_so_wins=0,
                         ot_losses=0, matches=0, goals_for=0, goal_diff=0,
                         away_goals=0, score_quotients=Fraction(0), complete_scores=True)
              for name in team_names}
    for match in match_records:
        home, away = match["home_team"], match["away_team"]
        home_score, away_score = match["home_score"], match["away_score"]
        winner = match.get("winner") or ("1" if home_score > away_score else "2" if away_score > home_score else "X")
        subtype = normalize_result_subtype(match.get("result_subtype"), winner)
        complete_scores = True
        # A record that carries its regulation tally (even an unknown one) is compared by it: the source
        # view cannot flag basketball overtime, so the subtype alone would pass final scores as regulation.
        if regulation_scores_only and ("regulation_home_score" in match or subtype in {"OT", "SO"}):
            home_score, away_score = match.get("regulation_home_score"), match.get("regulation_away_score")
            complete_scores = home_score is not None and away_score is not None
        for team, won_code, scored, conceded, is_away in (
            (home, "1", home_score, away_score, False), (away, "2", away_score, home_score, True),
        ):
            if team not in result:
                continue
            stats = result[team]
            stats["matches"] += 1
            if winner == "X":
                stats["draws"] += 1
            elif winner == won_code:
                stats["wins"] += 1
                stats["ot_so_wins"] += subtype in {"OT", "SO"}
            else:
                stats["losses"] += 1
                stats["ot_losses"] += subtype in {"OT", "SO"}
            stats["complete_scores"] &= complete_scores
            if complete_scores:
                stats[RankingMetric.GOALS_FOR] += scored
                stats[RankingMetric.GOAL_DIFF] += scored - conceded
                stats[RankingMetric.AWAY_GOALS] += scored if is_away else 0
                if conceded:
                    stats["score_quotients"] += Fraction(scored, conceded)
    for stats in result.values():
        stats["points"] = scoring.points(stats)
    return result


def _complete_home_and_away(names, records):
    counts = Counter((m["home_team"], m["away_team"]) for m in records)
    return all(counts[a, b] and counts[b, a] for a, b in combinations(names, 2))


def _without_nhl_odd_games(names, records):
    """Exclude the first game in the city hosting the extra encounter, per pair."""
    excluded = set()
    for a, b in combinations(names, 2):
        home_a = [i for i, m in enumerate(records) if m["home_team"] == a and m["away_team"] == b]
        home_b = [i for i, m in enumerate(records) if m["home_team"] == b and m["away_team"] == a]
        if (len(home_a) + len(home_b)) % 2:
            extra_home = home_a if len(home_a) > len(home_b) else home_b
            if extra_home:
                excluded.add(extra_home[0])
    return [m for i, m in enumerate(records) if i not in excluded]


CLOSING_PROJECTION_PERCENT = 100


def select_standings_policy(games_played_by_team, *, number_of_teams, games_per_team):
    """Choose the ranking sequence of one cut from the whole competition's progress.

    Returns ``(season_completion_ratio, standings_policy)``. An unknown, non-positive
    or incompatible format (more clubs or more games per club than configured) has no
    ratio and keeps the in-season sequence.
    """
    games = list(games_played_by_team)
    if (not number_of_teams or not games_per_team or number_of_teams < 1 or games_per_team < 1
            or len(games) > number_of_teams or any(g < 0 or g > games_per_team for g in games)):
        return None, "in_season"
    expected, played = number_of_teams * games_per_team, sum(games)
    closing = played * 100 >= expected * CLOSING_PROJECTION_PERCENT
    return played / expected, "closing_projection" if closing else "in_season"


PENDING_RESOLUTION_REASON = RankingIssue.OFFICIAL_RESOLUTION_PENDING


def _sporting_boundaries(profile, *, retention_only=False):
    """Ranks r such that places r and r + 1 sit on opposite sides of a sporting line."""
    ranks = {1}
    # Annual aggregate profiles carry no objectives and therefore no sporting lines.
    for rule in getattr(profile, "objectives", None) or ():
        reference = rule.rank_selection.reference_rank
        if reference is not None and (not retention_only or rule.objective_type == ObjectiveType.DIVISION_RETENTION):
            ranks.add(reference)
    return ranks


def _crosses(boundaries, first_place, size):
    """Whether a tie occupying ``size`` places from ``first_place`` straddles any boundary."""
    return any(first_place <= boundary < first_place + size - 1 for boundary in boundaries)


def rank_standings(items, profile, match_records, *, standings_policy):
    """Evaluate rules in order; missing data stops a tie rather than skipping it.

    ``standings_policy`` (``in_season`` or ``closing_projection``) is chosen by the
    caller from the whole competition, never inferred from a zone or conference.
    A closing criterion that needs a home and away meeting hands the clubs still tied
    to the in-season sequence when one is missing; their rows record that fallback.

    Names stabilize display only. They cannot break a sporting tie unless the
    policy explicitly prescribes alphabetical order (interim KNVB).
    """
    rows = dict(items)
    records = match_records or []
    policy = profile.standings
    closing = standings_policy == "closing_projection"
    in_season = policy.in_season_criteria
    criteria = policy.closing_projection_criteria if closing else in_season
    all_boundaries = _sporting_boundaries(profile)
    summaries = {}
    complex_records = None

    def summary(names, head_to_head=False, nhl=False):
        key = (tuple(sorted(names)), head_to_head, nhl)
        if key not in summaries:
            games = [m for m in records if m["home_team"] in names and m["away_team"] in names] if head_to_head else records
            if nhl:
                games = _without_nhl_odd_games(names, games)
            summaries[key] = build_head_to_head_stats(
                names, games, profile.scoring, regulation_scores_only=policy.regulation_scores_only,
            )
        return summaries[key]

    def conference_percentage(team):
        """Points percentage against clubs of the team's own conference (AHL)."""
        def conference(name):
            return AHL_CONFERENCE_BY_DIVISION.get(get_team_group(name, GroupingMethod.AHL_DIVISION))

        own = conference(team)
        if own is None:
            return None
        games = [m for m in records if team in (m["home_team"], m["away_team"])
                 and conference(m["away_team"] if m["home_team"] == team else m["home_team"]) == own]
        stats = build_head_to_head_stats((team,), games, profile.scoring)[team]
        if not stats["matches"]:
            return None
        return Fraction(str(stats["points"])) / Fraction(str(profile.scoring.maximum_per_game)) / stats["matches"]

    def metric(name, team, names):
        stats = rows[team]
        if name == RankingMetric.NEGATIVE_GAMES_PLAYED:
            return -stats["games_played"]
        if name in {RankingMetric.NEGATIVE_RED_CARDS, RankingMetric.NEGATIVE_YELLOW_CARDS}:
            count = stats.get(name.removeprefix("negative_"))
            return -count if count is not None else None
        if name == RankingMetric.NEGATIVE_FAIR_PLAY_POINTS:
            points = policy.fair_play_points(stats)
            return -points if points is not None else None
        if name == RankingMetric.NEGATIVE_FAIR_PLAY_POINTS_PER_GAME:
            points = policy.fair_play_points(stats)
            games = stats["games_played"]
            return -Fraction(points, games) if points is not None and games else None
        if name == RankingMetric.NEGATIVE_FAIR_PLAY_RED_CARDS:
            return -(stats["second_yellow_dismissals"] + stats["direct_red_cards"])
        if name == RankingMetric.ALPHABETICAL_ORDER:
            # The trailing 0 ranks a name ahead of every longer name it prefixes.
            return tuple(-ord(c) for c in team.casefold()) + (0,)
        if name == StandingsMetric.WIN_PERCENTAGE:
            games = stats["games_played"]
            return Fraction(str(stats["points"])) / Fraction(str(profile.scoring.maximum_per_game)) / games if games else Fraction(0)
        if name == RankingMetric.BALANCED_AWAY_GOALS:
            # FMF art. 25: totals are comparable only at equal away counts.
            # With unequal counts, the official matchday cutoff is required;
            # chronological truncation is wrong when matches were postponed.
            totals = summary(names)
            if any(totals[t]["matches"] != rows[t]["games_played"] for t in names):
                return None
            away_counts = Counter(m["away_team"] for m in records)
            if len({away_counts[t] for t in names}) != 1:
                return None
            return totals[team][RankingMetric.AWAY_GOALS]
        if name == RankingMetric.AHL_CONFERENCE_POINTS_PERCENTAGE:
            return conference_percentage(team)
        if name in {"nhl_h2h", "ahl_h2h"}:
            direct = summary(names, True, nhl=name == "nhl_h2h")[team]
            if not direct["matches"]:
                return 0
            return Fraction(str(direct["points"])) / direct["matches"] if len(names) > 2 else direct["points"]
        if name.startswith("h2h_"):
            direct = summary(names, True)[team]
            measure = name[4:]
            if measure == RankingMetric.REGULATION_WINS:
                return direct["wins"] - direct["ot_so_wins"]
            if measure == "win_percentage":
                return Fraction(direct["wins"], direct["matches"]) if direct["matches"] else Fraction(0)
            if measure in {RankingMetric.GOAL_DIFF, RankingMetric.GOALS_FOR, RankingMetric.AWAY_GOALS} and not direct["complete_scores"]:
                return None
            return direct.get(measure)
        if policy.regulation_scores_only and name in {RankingMetric.GOAL_DIFF, RankingMetric.GOALS_FOR, RankingMetric.GAME_SCORE_QUOTIENTS}:
            scores = summary(names)[team]
            if not scores["complete_scores"]:
                return None
            return round(scores["score_quotients"], 5) if name == RankingMetric.GAME_SCORE_QUOTIENTS else scores[name]
        return stats.get(name)

    def restarts(step, names, subgroup):
        """Whether a split sends the clubs still tied back to the full sequence."""
        if policy.restart_sequence == RestartSequence.PAIR_AFTER_REAPPLY:
            return step.reapply and len(subgroup) == 2
        if policy.restart_sequence == RestartSequence.PAIR:
            return len(subgroup) == 2
        if policy.restart_sequence == RestartSequence.COMPLETE_MEETINGS:
            return _complete_home_and_away(names, records)
        return False

    def separate(names, steps, path, full, fallback, first_place):
        """Order ``names`` (tied so far, starting at ``first_place``) by the remaining steps.

        Returns ``(clubs, rank key, pending reason, fallback reason)`` clusters in table order.
        """
        nonlocal complex_records

        def settled(reason=None):
            return [(names, path, reason, fallback)]

        def continue_with(next_steps):
            return separate(names, next_steps, path, full, fallback, first_place)

        if len(names) == 1 or not steps:
            return settled()
        step, *remaining = steps
        if len(step.metrics) == 1 and step.metrics[0] in COMPLEX_TIEBREAKS:
            if complex_records is None:
                complex_records = TieBreakRecords(records, profile.scoring)
            return [(cluster, path + ((-index,),), reason, fallback)
                    for index, (cluster, reason) in enumerate(COMPLEX_TIEBREAKS[step.metrics[0]](names, complex_records))]
        if (step.only_two and len(names) != 2) or (step.only_multiple and len(names) <= 2):
            return continue_with(remaining)
        if step.only_across_boundaries and not _crosses(all_boundaries, first_place, len(names)):
            return continue_with(remaining)
        if step.unavailable:
            return settled(step.unavailable)
        if step.meetings:
            direct_games = [m for m in records if m["home_team"] in names and m["away_team"] in names]
            if not _complete_home_and_away(names, direct_games):
                action = step.incomplete_meetings_action
                if action == IncompleteMeetingsAction.SHARE_POSITION:
                    return settled()
                if action == IncompleteMeetingsAction.USE_IN_SEASON and closing and fallback is None:
                    return separate(names, in_season, path, in_season, RankingIssue.INCOMPLETE_HEAD_TO_HEAD, first_place)
                return continue_with(remaining)
        values_by_team = {team: () for team in names}
        for measure in step.metrics:
            for team in names:
                value = metric(measure, team, names)
                if value is None:
                    reason = (RankingIssue.MISSING_REGULATION_SCORES if policy.regulation_scores_only
                              else RankingIssue("MISSING_" + measure.removeprefix("negative_").upper()))
                    return settled(reason)
                values_by_team[team] += (value,)
            if step.reapply and not step.complete_block and len(set(values_by_team.values())) > 1:
                break
        buckets = defaultdict(list)
        for team, values in values_by_team.items():
            buckets[values].append(team)
        result, place = [], first_place
        for values in sorted(buckets, reverse=True):
            subgroup = buckets[values]
            next_steps = steps if step.reapply and len(buckets) > 1 else remaining
            if len(buckets) > 1 and restarts(step, names, subgroup):
                next_steps = full
            result.extend(separate(subgroup, next_steps, path + (values,), full, fallback, place))
            place += len(subgroup)
        return result

    pending_across_boundaries = any(step.pending_across_boundaries for step in criteria)
    retention_boundaries = _sporting_boundaries(profile, retention_only=True)

    by_primary = defaultdict(list)
    for team in rows:
        by_primary[metric(policy.primary_metric, team, tuple(rows))].append(team)
    clusters, place = [], 1
    for primary in sorted(by_primary, reverse=True):
        group = by_primary[primary]
        found = separate(group, criteria, ((primary,),), criteria, None, place)
        if pending_across_boundaries and _crosses(retention_boundaries, place, len(group)):
            # The mini-table still orders the candidates; the official resolution assigns the places.
            # A group handed to the in-season sequence keeps that table's order and is not pending.
            found = [(names, key, reason or PENDING_RESOLUTION_REASON, fallback) if fallback is None
                     else (names, key, reason, fallback) for names, key, reason, fallback in found]
        clusters.extend(found)
        place += len(group)

    ordered, positions, offset = [], {}, 1
    for names, key, reason, fallback in clusters:
        for team in sorted(names, key=str.casefold):
            ordered.append((team, rows[team]))
            positions[team] = {
                "position": None if reason else offset,
                "ranking_order": offset,
                "is_primary_tie": len(names) > 1,
                "primary_rank_key": tuple(tuple(float(value) if isinstance(value, Fraction) else value for value in part) for part in key),
                "ranking_status": RankingStatus.UNRESOLVED if reason else RankingStatus.SHARED_POSITION if len(names) > 1 else RankingStatus.RESOLVED,
                "ranking_reason": reason,
                "ranking_rules_verified": policy.verified,
            }
            if fallback:
                positions[team].update(ranking_policy="in_season", ranking_policy_reason=fallback)
        offset += len(names)
    return ordered, positions
