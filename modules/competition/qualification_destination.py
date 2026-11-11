"""Shared destination facts for league and cup qualification rules."""

from __future__ import annotations

from dataclasses import dataclass

from .competition_identity import CompetitionIdentity
from .rule_terms import DestinationPhase, DestinationRound, QualificationPath


@dataclass(frozen=True, slots=True)
class QualificationDestination:
    """The competition phase reached and the route used to reach it.

    Internal phases may omit provider identity and name. Pending external
    destinations may also omit facts that have not yet been resolved.
    """

    destination_competition_identity: CompetitionIdentity | None = None
    destination_competition_name: str | None = None
    destination_phase: DestinationPhase | None = None
    destination_round: DestinationRound | None = None
    qualification_path: QualificationPath | None = None

    def __post_init__(self) -> None:
        identity = self.destination_competition_identity
        if isinstance(identity, dict):
            identity = CompetitionIdentity(**identity)
            object.__setattr__(self, "destination_competition_identity", identity)
        if identity is not None and not isinstance(identity, CompetitionIdentity):
            raise ValueError("destination_competition_identity requires a provider identity")
        if self.destination_competition_name is not None:
            if not isinstance(self.destination_competition_name, str) or not self.destination_competition_name.strip():
                raise ValueError("destination_competition_name must be a non-empty string")
        if self.destination_phase is not None:
            object.__setattr__(self, "destination_phase", DestinationPhase(self.destination_phase))
        if self.destination_round is not None:
            object.__setattr__(self, "destination_round", DestinationRound(self.destination_round))
            if self.destination_phase is None:
                raise ValueError("destination_round requires destination_phase")
        if self.qualification_path is not None:
            object.__setattr__(self, "qualification_path", QualificationPath(self.qualification_path))
