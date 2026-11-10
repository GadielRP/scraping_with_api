"""Pure, bounded evaluation of normalized facts, independent of their arrival order."""
from collections import Counter
from collections.abc import Iterable

from modules.competition.competitive_rules import DisciplineRules
from .contracts import (
    Acquisition, DisciplineFact, FactKind, IncidentDraft, IncidentType, Origin, Outcome,
)
from infrastructure.settings.discipline import DisciplineSettings


class DisciplineLimitExceeded(ValueError):
    """No partial snapshot may be published after a configured limit is reached."""


def football_fair_play_card_counts(yellow_cards: int | None, red_cards: int | None,
                                   second_yellow_dismissals: int | None) -> dict[str, int] | None:
    """Convert inclusive totals to the three separate scoring categories."""
    if yellow_cards is None or red_cards is None or second_yellow_dismissals is None:
        return None
    standalone_yellow_cards = yellow_cards - 2 * second_yellow_dismissals
    direct_red_cards = red_cards - second_yellow_dismissals
    if standalone_yellow_cards < 0 or direct_red_cards < 0:
        return None
    return {
        "standalone_yellow_cards": standalone_yellow_cards,
        "second_yellow_dismissals": second_yellow_dismissals,
        "direct_red_cards": direct_red_cards,
    }


def evaluate_facts(sport: str, facts: Iterable[DisciplineFact], rules: DisciplineRules | None,
                   limits: DisciplineSettings) -> Acquisition:
    reported = ({FactKind.YELLOW_CARD: IncidentType.YELLOW_CARD,
                 FactKind.RED_CARD: IncidentType.RED_CARD,
                 FactKind.RED_CARD_SECOND_YELLOW: IncidentType.RED_CARD_SECOND_YELLOW} if sport == "Football"
                else {FactKind.EJECTION: IncidentType.EJECTION})
    managed = set(reported.values())
    incidents, identities, players = [], {}, {}
    counts = Counter()
    derivable = sport == "Basketball" and rules is not None
    for fact in facts:
        if fact.identity is not None:
            previous = identities.get(fact.identity)
            if previous is not None:
                if previous != fact:
                    return Acquisition(Outcome.INVALID, reason="conflicting_comment_identity")
                continue
            if len(identities) >= limits.max_relevant_ids:
                raise DisciplineLimitExceeded("relevant_ids")
            identities[fact.identity] = fact
        if fact.kind in reported:
            if fact.side is None:
                return Acquisition(Outcome.INVALID, reason="reported_incident_missing_side")
            incidents.append(IncidentDraft(reported[fact.kind], Origin.REPORTED,
                                           fact.source_player_id, fact.player_name, fact.side))
        elif fact.kind in (FactKind.UNKNOWN_FOUL, FactKind.UNSUPPORTED_EXCEPTION):
            derivable = False
        elif fact.kind == FactKind.PERSONAL_FOUL and rules is not None:
            if fact.source_player_id is None or fact.side is None:
                derivable = False
                continue
            # Provider-scoped player identity, never a team FK or a name match.
            key = fact.source_player_id
            if key not in players:
                if len(players) >= limits.max_players:
                    raise DisciplineLimitExceeded("players")
                players[key] = fact
            elif players[key].side != fact.side:
                derivable = False
            counts[key] += 1
            # More than the limit signals an unsupported eligibility exception.
            if counts[key] > rules.personal_foul_limit:
                derivable = False
        if len(incidents) > limits.max_incidents:
            raise DisciplineLimitExceeded("incidents")
    if derivable:
        managed.add(IncidentType.FOUL_OUT)
        for key, count in sorted(counts.items()):
            if count == rules.personal_foul_limit:
                fact = players[key]
                incidents.append(IncidentDraft(IncidentType.FOUL_OUT, Origin.DERIVED,
                                               key, fact.player_name, fact.side))
        if len(incidents) > limits.max_incidents:
            raise DisciplineLimitExceeded("incidents")
    reason = "foul_out_not_replaceable" if rules is not None and not derivable else None
    return Acquisition(Outcome.READY, tuple(incidents), frozenset(managed), reason)
