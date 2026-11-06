"""Provider identity, independent of database primary keys."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CompetitionIdentity:
    """A provider's unique tournament and specific tournament record.

    A tournament ID is required to distinguish main and qualification records.
    Collection routing may omit it when it intentionally covers a whole family.
    """

    source_unique_tournament_id: int
    source_tournament_id: int | None = None
    source: str = "sofascore"

    def __post_init__(self) -> None:
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("competition identity requires a source")
        for name in ("source_unique_tournament_id", "source_tournament_id"):
            value = getattr(self, name)
            if value is None and name == "source_tournament_id":
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
