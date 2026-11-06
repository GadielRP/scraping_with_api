"""Round classification shared by single and batch event persistence."""

from modules.competition.competition_ids import NBA_CUP_FINAL


SPECIFIC_KNOCKOUT_ROUNDS = frozenset({'final', 'semifinal', 'quarterfinal'})
KNOCKOUT_ROUNDS = ('knockouts/playoffs', *sorted(SPECIFIC_KNOCKOUT_ROUNDS))


def round_family(round_name: str | None) -> str | None:
    """Retain the broad knockout grouping for filters and historical queries."""
    return 'knockouts/playoffs' if round_name in KNOCKOUT_ROUNDS else round_name


def resolve_event_round(
    source: str,
    source_tournament_id: int | None,
    incoming: str | None,
    existing: str | None,
) -> str | None:
    # Cup games that count for the NBA record use tournament 177 instead.
    if (source, source_tournament_id) == (NBA_CUP_FINAL.source, NBA_CUP_FINAL.source_tournament_id):
        incoming = 'final'
    if not incoming:
        return existing
    # Sparse provider responses must not erase a previously identified phase.
    existing_key = (existing or '').lower()
    incoming_key = str(incoming).lower()
    if (existing_key in KNOCKOUT_ROUNDS and incoming_key == 'regular_season'
            or existing_key in SPECIFIC_KNOCKOUT_ROUNDS and incoming_key == 'knockouts/playoffs'):
        return existing
    return incoming
