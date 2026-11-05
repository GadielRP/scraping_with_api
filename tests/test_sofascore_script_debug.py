import argparse
import json

from scripts.sofascore.debug import (
    add_debug_file_argument,
    log_debug_file,
    save_response_for_debug,
)


def test_debug_file_argument_is_enabled_by_default_and_can_be_disabled():
    parser = argparse.ArgumentParser()
    add_debug_file_argument(parser)

    assert parser.parse_args([]).save_debug is True
    assert parser.parse_args(["--save-debug"]).save_debug is True
    assert parser.parse_args(["--no-save-debug"]).save_debug is False


def test_save_response_for_debug_writes_json_and_logs_absolute_path(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    payload = {"comments": [{"text": "Good game"}]}

    path = save_response_for_debug("get_event_comments", payload)
    log_debug_file(path)

    assert json.loads(path.read_text(encoding="utf-8")) == payload
    assert str(path.resolve()) in capsys.readouterr().err
