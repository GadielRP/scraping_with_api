from types import SimpleNamespace

from modules.alerts.dual_process.run_dual_process import (
    ComparisonVerdict,
    DualProcessRunner,
)


def test_evaluate_dual_process_can_build_report_without_predictions(monkeypatch):
    event = SimpleNamespace(
        id=293729,
        home_team="Paris FC",
        away_team="RC Strasbourg",
        competition="Ligue 1",
        sport="football",
        discovery_source="daily_discovery",
    )
    runner = DualProcessRunner()

    monkeypatch.setattr(
        runner,
        "_execute_process1",
        lambda *args, **kwargs: (None, None, "no_candidates"),
    )
    monkeypatch.setattr(
        runner,
        "_execute_process2",
        lambda *args, **kwargs: (None, None, "no_formulas_activated"),
    )

    report = runner.evaluate_dual_process(event)

    assert report.verdict is ComparisonVerdict.PARTIAL
    assert report.final_prediction is None
    assert report.process1_status == "no_candidates"
    assert report.process2_status == "no_formulas_activated"
    assert report.timestamp
