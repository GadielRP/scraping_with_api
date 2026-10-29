"""Pure evaluation of the ordered standings criteria in a competition profile."""

from collections import Counter, defaultdict
from fractions import Fraction
from itertools import combinations

from .standings_tiebreaks import COMPLEX_TIEBREAKS, TieBreakRecords


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
        if regulation_scores_only and subtype in {"OT", "SO"}:
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
                stats["goals_for"] += scored
                stats["goal_diff"] += scored - conceded
                stats["away_goals"] += scored if is_away else 0
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


def rank_standings(items, profile, match_records, *, season_complete=False):
    """Evaluate rules in order; missing data stops a tie rather than skipping it.

    Names stabilize display only. They cannot break a sporting tie unless the
    policy explicitly prescribes alphabetical order (interim KNVB).
    """
    rows = dict(items)
    records = match_records or []
    policy = profile.standings
    criteria = (policy.in_season_criteria if not season_complete and policy.in_season_criteria is not None
                else policy.criteria)
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

    def metric(name, team, names):
        stats = rows[team]
        if name == "negative_games_played":
            return -stats["games_played"]
        if name == "alphabetical_order":
            return tuple(-ord(c) for c in team.casefold())
        if name == "pct":
            games = stats["games_played"]
            return Fraction(str(stats["points"])) / Fraction(str(profile.scoring.maximum_per_game)) / games if games else Fraction(0)
        if name in {"nhl_h2h", "ahl_h2h"}:
            direct = summary(names, True, nhl=name == "nhl_h2h")[team]
            if not direct["matches"]:
                return 0
            return Fraction(str(direct["points"])) / direct["matches"] if len(names) > 2 else direct["points"]
        if name.startswith("h2h_"):
            direct = summary(names, True)[team]
            measure = name[4:]
            if measure == "win_percentage":
                return Fraction(direct["wins"], direct["matches"]) if direct["matches"] else Fraction(0)
            if measure in {"goal_diff", "goals_for", "away_goals"} and not direct["complete_scores"]:
                return None
            return direct.get(measure)
        if policy.regulation_scores_only and name in {"goal_diff", "goals_for", "game_score_quotients"}:
            scores = summary(names)[team]
            if not scores["complete_scores"]:
                return None
            return round(scores["score_quotients"], 5) if name == "game_score_quotients" else scores[name]
        return stats.get(name)

    def separate(names, steps, path=()):
        nonlocal complex_records
        if len(names) == 1 or not steps:
            return [(names, path, None)]
        step, *remaining = steps
        if len(step.metrics) == 1 and step.metrics[0] in COMPLEX_TIEBREAKS:
            if complex_records is None:
                complex_records = TieBreakRecords(records, profile.scoring)
            return [(cluster, path + ((-index,),), reason)
                    for index, (cluster, reason) in enumerate(COMPLEX_TIEBREAKS[step.metrics[0]](names, complex_records))]
        if step.final_only and not season_complete:
            return separate(names, remaining, path)
        if (step.only_two and len(names) != 2) or (step.only_multiple and len(names) <= 2):
            return separate(names, remaining, path)
        if step.unavailable:
            return [(names, path, step.unavailable)]
        if step.meetings:
            direct_games = [m for m in records if m["home_team"] in names and m["away_team"] in names]
            if not _complete_home_and_away(names, direct_games):
                if season_complete:
                    return [(names, path, "MISSING_HEAD_TO_HEAD_MATCHES")]
                if step.share_if_incomplete:
                    return [(names, path, None)]
                return separate(names, remaining, path)
        values_by_team = {team: () for team in names}
        for measure in step.metrics:
            for team in names:
                value = metric(measure, team, names)
                if value is None:
                    reason = "MISSING_REGULATION_SCORES" if policy.regulation_scores_only else "MISSING_" + measure.upper()
                    return [(names, path, reason)]
                values_by_team[team] += (value,)
            # On a partial H2H split, reapply its first metric to the remaining
            # clubs before considering the next one (not a lexicographic key).
            if step.reapply and len(set(values_by_team.values())) > 1:
                break
        buckets = defaultdict(list)
        for team, values in values_by_team.items():
            buckets[values].append(team)
        result = []
        for values in sorted(buckets, reverse=True):
            subgroup = buckets[values]
            next_steps = steps if step.reapply and len(buckets) > 1 else remaining
            if policy.name == "LALIGA" and step.reapply and len(buckets) > 1 and len(subgroup) == 2:
                next_steps = policy.criteria
            euro_complete = policy.name == "EUROLEAGUE" and _complete_home_and_away(names, records)
            if len(buckets) > 1 and (euro_complete or (policy.name == "AHL" and len(subgroup) == 2)):
                next_steps = criteria
            result.extend(separate(subgroup, next_steps, path + (values,)))
        return result

    by_primary = defaultdict(list)
    for team in rows:
        by_primary[metric(policy.primary_metric, team, tuple(rows))].append(team)
    clusters = []
    for primary in sorted(by_primary, reverse=True):
        clusters.extend(separate(by_primary[primary], criteria, ((primary,),)))

    ordered, positions, offset = [], {}, 1
    for names, key, reason in clusters:
        for team in sorted(names, key=str.casefold):
            ordered.append((team, rows[team]))
            positions[team] = {
                "position": None if reason else offset,
                "is_primary_tie": len(names) > 1,
                "primary_rank_key": tuple(tuple(float(value) if isinstance(value, Fraction) else value for value in part) for part in key),
                "ranking_status": "UNRESOLVED" if reason else "SHARED_POSITION" if len(names) > 1 else "RESOLVED",
                "ranking_reason": reason,
                "ranking_rules_verified": policy.verified,
            }
        offset += len(names)
    if season_complete and policy.name == "SERIE_A":
        # Mini-table order seeds a decider; it cannot determine its winner.
        for names in by_primary.values():
            occupied = [i + 1 for i, (team, _) in enumerate(ordered) if team in names]
            if len(names) > 1 and (1 in occupied or (17 in occupied and 18 in occupied)):
                for team in names:
                    positions[team].update(position=None, ranking_status="UNRESOLVED",
                                           ranking_reason="SERIE_A_DECIDER_OR_UEFA_EXCEPTION_REQUIRED")
    return ordered, positions
