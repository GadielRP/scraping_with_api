"""Select a unique contract without merging lines, providers or containers."""

from dataclasses import dataclass

from .market_snapshot_extractor import (
    MarketCandidate,
    MarketSnapshotExtraction,
    MarketSnapshotRequest,
)


@dataclass(frozen=True, slots=True)
class CandidateSelection:
    candidate: MarketCandidate | None = None
    missing: frozenset[str] = frozenset()
    invalid: frozenset[str] = frozenset()
    ambiguous: frozenset[str] = frozenset()


def select_market_candidate(
    extraction: MarketSnapshotExtraction,
    request: MarketSnapshotRequest,
    *,
    allow_partial: bool = True,
) -> CandidateSelection:
    names = {choice.input_name for choice in request.choices}
    if request.line_input_name is not None:
        names.add(request.line_input_name)
    ambiguous = names.intersection(extraction.ambiguous_inputs)
    if extraction.container_ambiguities or ambiguous:
        return CandidateSelection(ambiguous=frozenset(names | ambiguous))

    # Retain only one candidate of each kind; no additional candidate lists.
    complete = partial = None
    complete_count = partial_count = 0
    for candidate in extraction.candidates:
        if candidate.is_complete(request):
            complete, complete_count = candidate, complete_count + 1
        elif candidate.line is not None or any(
            point is not None for point in candidate.choices.values()
        ):
            partial, partial_count = candidate, partial_count + 1
    if complete_count > 1 or (not complete_count and partial_count > 1):
        return CandidateSelection(ambiguous=frozenset(names))
    if complete is not None:
        return CandidateSelection(candidate=complete)

    invalid = names.intersection(extraction.invalid_inputs)
    missing = set(names)
    if partial is not None:
        for choice in request.choices:
            if partial.choices.get(choice.key) is not None:
                missing.discard(choice.input_name)
        if partial.line is not None:
            missing.discard(request.line_input_name)
    return CandidateSelection(
        candidate=partial if allow_partial else None,
        missing=frozenset(missing - invalid),
        invalid=frozenset(invalid),
    )
