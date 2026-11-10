"""Load cup evidence after standings, before P1's existing immutable capture."""

from dataclasses import replace

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.cup_final_repository import read_cup_final
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.competition_profiles import QUALIFICATION_DEPENDENCIES
from modules.competition.qualification_resolution import resolve_laliga_qualification
from shared.temporal import as_utc, utc_now


POLICY_SOURCES = (
    "https://rfef.es/sites/default/files/2026-06/1._CNL_Primera_y_Segunda_Division.pdf#page=5",
    "https://rfef.es/sites/default/files/2026-07/5._Torneos_Federacion.pdf#page=12",
    "https://rankinguefa.pl/wp-content/uploads/2026/09/20260909_Circular_2026_54_en_enclosure_2_2027-28-access-list.pdf",
    "https://documents.uefa.com/r/Regulations-of-the-UEFA-Europa-League-2026/27/Article-3-Entries-for-the-competition-Online",
)


def prepare_qualification_resolution(event, competition, streak, *, as_of=None):
    identity = CompetitionIdentity(competition.source_unique_tournament_id,
                                   competition.source_tournament_id, competition.source)
    dependency = QUALIFICATION_DEPENDENCIES.get(identity)
    if dependency is None or competition.competitive_rules is None:
        return replace(competition, qualification_resolution=None)
    # Freeze a knowledge instant for this read. Historical evaluations cannot use
    # legacy results first read today. Capture retains the result and read time.
    now = utc_now()
    cutoff = (min(as_utc(as_of or now), as_utc(event.starts_at))
              if as_of is not None or as_utc(event.starts_at) <= now else None)
    with db_manager.get_session() as session:
        evidence = read_cup_final(session, dependency, event.season_year, cutoff)
    cutoff = cutoff or min(utc_now(), as_utc(event.starts_at))
    resolution = resolve_laliga_qualification(
        competition.competitive_rules, getattr(streak, "current_standings", None),
        evidence, event.season_year, cutoff, policy_sources=POLICY_SOURCES)
    return replace(competition, qualification_resolution=resolution)
