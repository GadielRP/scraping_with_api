"""Persist one event through discipline collection and return its primary response."""
import json
import sys
from contextlib import ExitStack, nullcontext
from types import SimpleNamespace

from infrastructure.settings.discipline import DisciplineSettings
from modules.discipline.contracts import Acquisition, DisciplineEndpoint, Outcome, Selection
from modules.discipline.providers.sofascore import SofaScoreDisciplineProvider
from modules.discipline.providers.sofascore_statistics import SofaScoreDisciplineStatisticsProvider
from modules.jobs.discipline_collection_job.composition import build_discipline_runner
from modules.sofascore.event_identity import SofaScoreEventIdentity
from shared.execution_context import WorkDeferred


def persist_discipline(identity: SofaScoreEventIdentity, endpoint: DisciplineEndpoint, client):
    event_id = identity.canonical_event_id
    if event_id is None:
        raise SystemExit(f"No canonical mapping for SofaScore event {identity.sofascore_event_id}")

    settings = DisciplineSettings()
    is_comments = endpoint == DisciplineEndpoint.COMMENTS
    if is_comments and not settings.full_feed_verified:
        raise SystemExit("Comments persistence requires DisciplineSettings.full_feed_verified=True")

    with ExitStack() as stack:
        document = None
        open_endpoint = client.open_event_comments if is_comments else client.open_event_statistics

        def open_response(source_event_id):
            # Keep the same bounded download for stdout and the debug writer after ingestion.
            nonlocal document
            document = stack.enter_context(open_endpoint(source_event_id))
            return nullcontext(document)

        if is_comments:
            provider = SofaScoreDisciplineProvider(SimpleNamespace(
                open_event_comments=open_response,
                open_event_statistics=client.open_event_statistics,
            ), settings)
        else:
            provider = SofaScoreDisciplineStatisticsProvider(SimpleNamespace(open_event_statistics=open_response))

        def acquire(context):
            if int(context.source_event_id) != identity.sofascore_event_id:
                return Acquisition(Outcome.MAPPING_CONFLICT, reason="source_event_id_changed")
            return provider.acquire(context)

        report = None

        def record_report(_, event_report):
            nonlocal report
            report = event_report

        run = build_discipline_runner(settings, provider=SimpleNamespace(acquire=acquire))
        try:
            summary = run(selection=Selection(event_id=event_id), limit=1, commit=True, on_event=record_report)
        except WorkDeferred as exc:
            raise SystemExit(f"Discipline deferred for event {event_id}: {exc}") from None

        if report is None:
            reason = next((key for key in ("skipped_unsupported_sport", "skipped_untracked", "failed")
                           if summary[key]), "no eligible event with a persisted result")
            raise SystemExit(f"Discipline {endpoint.value} for event {event_id}: {reason}")
        reason = report.acquisition.reason if report.acquisition else None
        message = f"Discipline {endpoint.value} for event {event_id}: {report.outcome.value}"
        if reason:
            message += f" ({reason})"
        if report.outcome not in (Outcome.UPDATED, Outcome.UNCHANGED):
            raise SystemExit(message)
        print(message, file=sys.stderr)

        if document is not None:
            document.seek(0)
            try:
                return json.load(document)
            except (json.JSONDecodeError, UnicodeDecodeError):
                # A usable statistics fallback may have committed after invalid comments.
                return None
        return None
