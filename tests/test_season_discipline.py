"""Season CLI handoff uses confirmed results and the shared discipline collector."""
from types import SimpleNamespace

import pytest

from scripts import sport_seasons_processing as script
from modules.jobs.results_collection_job.contracts import CommittedResultEvent


def test_flush_delivers_only_confirmed_references_after_commit(monkeypatch):
    results = [(1, {}), (2, {})]
    refs = [CommittedResultEvent(i, "Basketball", 176) for i in (1, 2)]
    saved = []
    def persist(rows):
        saved.append(tuple(rows))
        return {1}
    def collect(events):
        assert saved and results == [] and refs == []
        assert events == (CommittedResultEvent(1, "Basketball", 176),)
    monkeypatch.setattr(script.ResultRepository, "batch_upsert_results", persist)
    assert script._flush_results(results, refs, collect) == 1


@pytest.mark.parametrize("with_discipline", [False, True])
def test_season_uses_bounded_result_batches_and_shared_collector(monkeypatch, with_discipline, caplog):
    caplog.set_level("INFO")
    events = [dict(event=dict(id=i, startTimestamp=i, homeTeam="A", awayTeam="B"),
                   _raw_event=dict(id=i)) for i in (1, 2, 3)]
    received, reconciled = [], []
    monkeypatch.setattr(script, "DisciplineSettings", lambda: SimpleNamespace(full_feed_verified=True))
    monkeypatch.setattr(script, "JobExecutionSettings", lambda: SimpleNamespace(event_write_batch_size=2))
    monkeypatch.setattr(script, "fetch_season_events", lambda *_: events)
    monkeypatch.setattr(script, "parse_event_result", lambda _: SimpleNamespace(kind="finished", result={"home_score": 1, "away_score": 0}))
    monkeypatch.setattr(script.EventRepository, "batch_upsert_events", lambda rows, *args: SimpleNamespace(
        events={"x": SimpleNamespace(id=rows[0]["event"]["id"], sport="Basketball", competition_id=176)},
        inserted=1, updated=0))
    sizes = []
    def persist(rows):
        sizes.append(len(rows))
        return {eid for eid, _ in rows}
    monkeypatch.setattr(script.ResultRepository, "batch_upsert_results", persist)
    def build(**kwargs):
        assert kwargs["origin"] == "season"
        assert kwargs["enabled"] is True
        callback = kwargs["on_event"]
        def collect(batch):
            received.extend(batch)
            for event in batch:
                callback(
                    SimpleNamespace(event_id=event.event_id),
                    SimpleNamespace(
                        outcome=script.Outcome.UPDATED,
                        acquisition=SimpleNamespace(
                            outcome=script.Outcome.READY,
                            incidents=(object(),),
                            comments_count=4,
                        ),
                    ),
                )
        return collect
    monkeypatch.setattr(script, "build_post_results_collector", build)
    def reconcile(**kwargs):
        reconciled.append(kwargs["collect_discipline"])
        return dict(reconciled_metadata=0, results_inserted_or_updated=0,
                    queued_for_deletion=0, still_pending=0, failed_direct_fetch_count=0)
    monkeypatch.setattr(script, "reconcile_existing_season_events", reconcile)
    script.process_season(132, 100772, fetch_odds=False, with_discipline=with_discipline)
    assert sizes == [2, 1]
    assert [event.event_id for event in received] == ([1, 2, 3] if with_discipline else [])
    assert (reconciled[0] is not None) == with_discipline
    if with_discipline:
        assert "discipline_comments_fetched: 12" in caplog.text
        assert "discipline_events_with_incident_insertions: 3" in caplog.text


@pytest.mark.parametrize("args", [["100772", "-t", "132"], ["--auto"], ["--season_name", "NBA"]])
def test_cli_passes_flag_in_each_mode(monkeypatch, args):
    import sys
    calls = []
    monkeypatch.setattr(sys, "argv", ["sport_seasons_processing", *args, "--with-discipline", "--skip-db-init", "--skip-odds"])
    monkeypatch.setattr(script, "season_to_process", [dict(season_name="NBA", season_id=100772, tournament_id=132)])
    monkeypatch.setattr(script, "process_season", lambda *a, **kw: calls.append(kw))
    script.main()
    assert calls == [dict(fetch_odds=False, with_discipline=True)]
