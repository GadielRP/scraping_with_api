"""Shared composition for manual collection and the midnight post-result step."""
from functools import partial

from infrastructure.persistence.advisory_lock import exclusive_slot
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.disciplinary_incident_repository import DisciplinaryIncidentRepository
from modules.discipline.providers.sofascore import SofaScoreDisciplineProvider
from modules.discipline.service import DisciplineService
from modules.sofascore import api_client
from .run_discipline_collection import run_discipline_collection


def build_discipline_runner(settings):
    repository = DisciplinaryIncidentRepository(db_manager)
    service = DisciplineService(SofaScoreDisciplineProvider(api_client, settings), repository,
                                partial(exclusive_slot, 736106))
    return partial(run_discipline_collection, service, repository, settings)
