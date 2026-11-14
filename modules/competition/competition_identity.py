"""Provider identity, independent of database primary keys."""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class CompetitionIdentity:
    """A provider's unique tournament and specific tournament record.

    A tournament ID identifies a specific provider record. ``None`` scopes the
    identity to the provider's tournament family when no single record exists.
    The competition name is descriptive metadata excluded from identity matching.
    """

    source_unique_tournament_id: int
    source_tournament_id: int | None = None
    source: str = "sofascore"
    competition_name: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("competition identity requires a source")
        if self.competition_name is not None and (
            not isinstance(self.competition_name, str) or not self.competition_name.strip()
        ):
            raise ValueError("competition_name must be a non-empty string")
        for name in ("source_unique_tournament_id", "source_tournament_id"):
            value = getattr(self, name)
            if value is None and name == "source_tournament_id":
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
