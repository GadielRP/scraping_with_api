"""Competition slugs reach both debug writers without additional lookups."""

import json
from types import SimpleNamespace

from infrastructure.persistence.models import Competition, Event, Participant
from infrastructure.persistence.repositories.event_repository import EventRepository
from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import (
    select_oddspapi_pre_start_candidates,
)
from modules.jobs.pre_start_check_job.providers.sofascore import odds_phase
from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)
from modules.odds_ingestion import ProviderOddsSummary
from modules.odds_ingestion.fetch_result import OddsFetchResult
from scripts.diagnostics.diagnose_sofascore_pre_start_responses import discover_responses


def test_event_data_and_oddspapi_candidate_keep_loaded_competition_slug():
    event = Event(
        id=238868,
        competition_id=10,
        sport="Football",
        competition_ref=Competition(slug="liga-mx-apertura", display_name="Liga MX"),
        home_participant=Participant(name="Home"),
        away_participant=Participant(name="Away"),
    )
    # An unpersisted model has no session: serializing it cannot query the DB.
    event_data = EventRepository._build_event_data_with_legacy_fallback(event)
    candidates = select_oddspapi_pre_start_candidates([
        {
            "event_id": event.id,
            "event_data": event_data,
            "minutes_until_start": 5,
            "should_extract_odds": True,
        }
    ])

    assert event_data["competition_slug"] == "liga-mx-apertura"
    assert candidates[0].competition_slug == "liga-mx-apertura"
    assert event_data["sport"] == "Football"
    assert candidates[0].sport == "Football"


def test_sofascore_phase_saves_raw_response_in_competition_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(SofaScoreDebugResponseWriter, "OUTPUT_DIRECTORY", tmp_path)
    payload = {"markets": [], "raw": True}
    candidate = {
        "event_id": 238868,
        "event_data": {
            "competition_id": 10,
            "competition_slug": "Liga-MX-Apertura",
            "sport": "Football",
            "slug": "home-away",
            "home_team": "Home",
            "away_team": "Away",
        },
        "sofascore_event_id": 9001,
        "minutes_until_start": 5,
        "should_extract_odds": True,
    }
    fetcher = SimpleNamespace(
        fetch_odds=lambda *args, **kwargs: OddsFetchResult.from_payload(
            payload, raw_payload=payload,
        )
    )

    def run_phase(candidates, source_states, **kwargs):
        assert kwargs["fetch"](candidates[0]).raw_payload == payload
        return ProviderOddsSummary(requests_attempted=1)

    monkeypatch.setattr(odds_phase, "run_provider_odds_phase", run_phase)
    odds_phase.run_sofascore_pre_start_odds(
        [candidate], {}, debug_mode=True, odds_fetcher=fetcher,
        tracked_competition_ids=[10],
    )

    path = tmp_path / "football" / "liga_mx_apertura" / "238868_Home_Away" / "238868_9001_t_5.json"
    assert json.loads(path.read_text(encoding="utf-8")) == payload
    responses, errors = discover_responses(tmp_path)
    assert not errors
    assert len(responses[238868][5]) == 1


def test_sofascore_writer_without_competition_slug(tmp_path, monkeypatch):
    monkeypatch.setattr(SofaScoreDebugResponseWriter, "OUTPUT_DIRECTORY", tmp_path)
    path = SofaScoreDebugResponseWriter.save(
        event_id=238868, source_event_id=9001, minutes_until_start=5,
        payload={"markets": []},
    )
    assert path == tmp_path / "unknown_sport" / "unknown_competition" / "238868" / "238868_9001_t_5.json"


def test_oddspapi_diagnostics_find_nested_event_responses(tmp_path):
    from scripts.development.diagnose_oddspapi_debug_responses import collect_response_files

    path = tmp_path / "basketball" / "nba_preseason" / "379309_Home_Away" / "379309_id123_t_5_odds.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"raw": true}', encoding="utf-8")
    assert collect_response_files(tmp_path) == [(5, path)]


def test_sofascore_writer_normalizes_sport_from_memory(tmp_path, monkeypatch):
    monkeypatch.setattr(SofaScoreDebugResponseWriter, "OUTPUT_DIRECTORY", tmp_path)
    path = SofaScoreDebugResponseWriter.save(
        event_id=374280, source_event_id=16546314, minutes_until_start=120,
        payload={"markets": []}, sport="Ice hockey", competition_slug="nhl",
        home_participant="Boston Bruins", away_participant="Ottawa Senators",
    )
    assert path == (
        tmp_path / "ice_hockey" / "nhl" / "374280_Boston_Bruins_Ottawa_Senators"
        / "374280_16546314_t_120.json"
    )
