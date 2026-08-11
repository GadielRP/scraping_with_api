"""CLI validation tests for backfill_market_choice_quotes."""

from scripts.maintenance.backfill_market_choice_quotes import (
    DEFAULT_CHECKPOINT_FILE,
    DEFAULT_OUTPUT_JSON,
    DEFAULT_OUTPUT_REJECTIONS,
    apply_default_artifact_paths,
    build_parser,
    validate_args,
)


def test_default_is_dry_run_with_event_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_CHECKPOINT_FILE",
        tmp_path / "checkpoint.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_JSON",
        tmp_path / "output.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_REJECTIONS",
        tmp_path / "rejections.ndjson",
    )
    parser = build_parser()
    args = parser.parse_args(["--event-id", "158955", "--fresh-artifacts"])
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is None
    assert args.dry_run is True
    assert args.commit is False
    assert args.output_json == tmp_path / "output.json"
    assert args.output_rejections == tmp_path / "rejections.ndjson"
    assert args.checkpoint_file == tmp_path / "checkpoint.json"
    assert args.append_rejections is False
    assert args.resume_from is None


def test_rejects_unbounded_scope():
    parser = build_parser()
    args = parser.parse_args(["--fresh-artifacts"])
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is not None
    assert "unbounded" in error


def test_commit_requires_confirm(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_CHECKPOINT_FILE",
        tmp_path / "checkpoint.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_JSON",
        tmp_path / "output.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_REJECTIONS",
        tmp_path / "rejections.ndjson",
    )
    parser = build_parser()
    args = parser.parse_args(["--commit", "--event-id", "1", "--fresh-artifacts"])
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is not None
    assert "confirm-ingestion-paused" in error


def test_commit_ok_with_defaults(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_CHECKPOINT_FILE",
        tmp_path / "checkpoint.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_JSON",
        tmp_path / "output.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_REJECTIONS",
        tmp_path / "rejections.ndjson",
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "--commit",
            "--confirm-ingestion-paused",
            "--event-id",
            "1",
            "--fresh-artifacts",
        ]
    )
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is None
    assert args.dry_run is False
    assert args.checkpoint_file == tmp_path / "checkpoint.json"


def test_event_id_exclusive_with_range():
    parser = build_parser()
    args = parser.parse_args(
        ["--event-id", "1", "--event-id-min", "1", "--fresh-artifacts"]
    )
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is not None
    assert "mutually exclusive" in error


def test_batch_size_hard_cap():
    parser = build_parser()
    args = parser.parse_args(
        ["--event-id", "1", "--batch-size", "1001", "--fresh-artifacts"]
    )
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is not None
    assert "hard cap" in error


def test_unknown_source_rejected():
    parser = build_parser()
    args = parser.parse_args(
        ["--event-id", "1", "--source", "nope", "--fresh-artifacts"]
    )
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is not None
    assert "source" in error


def test_commit_purge_requires_confirm_purge(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_CHECKPOINT_FILE",
        tmp_path / "checkpoint.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_JSON",
        tmp_path / "output.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_REJECTIONS",
        tmp_path / "rejections.ndjson",
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "--commit",
            "--confirm-ingestion-paused",
            "--purge-oddspapi-null-mainline-lines",
            "--event-id",
            "1",
            "--fresh-artifacts",
        ]
    )
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is not None
    assert "confirm-purge" in error


def test_commit_purge_ok_when_confirmed(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_CHECKPOINT_FILE",
        tmp_path / "checkpoint.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_JSON",
        tmp_path / "output.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_REJECTIONS",
        tmp_path / "rejections.ndjson",
    )
    parser = build_parser()
    args = parser.parse_args(
        [
            "--commit",
            "--confirm-ingestion-paused",
            "--confirm-purge",
            "--purge-oddspapi-null-mainline-lines",
            "--event-id",
            "1",
            "--fresh-artifacts",
        ]
    )
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is None
    assert args.purge_oddspapi_null_mainline_lines is True


def test_resume_flag_with_path_appends_rejections(tmp_path, monkeypatch):
    ckpt = tmp_path / "checkpoint.json"
    ckpt.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_CHECKPOINT_FILE",
        tmp_path / "checkpoint-default.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_JSON",
        tmp_path / "output.json",
    )
    monkeypatch.setattr(
        "scripts.maintenance.backfill_market_choice_quotes.DEFAULT_OUTPUT_REJECTIONS",
        tmp_path / "rejections.ndjson",
    )
    parser = build_parser()
    args = parser.parse_args(["--max-events", "10", "--resume-from", str(ckpt)])
    apply_default_artifact_paths(args)
    error = validate_args(args)
    assert error is None
    assert args.resume_from == ckpt
    assert args.append_rejections is True


def test_module_default_paths_point_at_shared_root():
    assert DEFAULT_CHECKPOINT_FILE.name == "checkpoint.json"
    assert DEFAULT_OUTPUT_JSON.name == "output.json"
    assert DEFAULT_OUTPUT_REJECTIONS.name == "rejections.ndjson"
    assert DEFAULT_CHECKPOINT_FILE.parent == DEFAULT_OUTPUT_JSON.parent
    assert "market_choice_quote_backfill" in str(DEFAULT_CHECKPOINT_FILE)
