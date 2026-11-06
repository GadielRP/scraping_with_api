"""Round classification shared by single and batch event persistence."""

SOFASCORE_NBA_CUP_FINAL_TOURNAMENT_ID = 124629


def resolve_event_round(
    source: str,
    source_tournament_id: int | None,
    incoming: str | None,
    existing: str | None,
) -> str | None:
    # Cup games that count for the NBA record use tournament 177 instead.
    if (source, source_tournament_id) == ('sofascore', SOFASCORE_NBA_CUP_FINAL_TOURNAMENT_ID):
        incoming = 'knockouts/playoffs'
    if not incoming or ((existing or '').lower() == 'knockouts/playoffs'
                        and str(incoming).lower() == 'regular_season'):
        return existing
    return incoming
