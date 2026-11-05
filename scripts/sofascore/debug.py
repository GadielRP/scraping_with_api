import argparse
from datetime import datetime
import json
from pathlib import Path
import sys


def add_debug_file_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--save-debug",
        dest="save_debug",
        action="store_true",
        help="Save the response as a debug JSON file (default)",
    )
    parser.add_argument(
        "--no-save-debug",
        dest="save_debug",
        action="store_false",
        help="Do not save a debug JSON file",
    )
    parser.set_defaults(save_debug=True)


def save_response_for_debug(script_name: str, payload: object) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    path = (
        Path("debug")
        / "sofascore"
        / "scripts"
        / script_name
        / f"{timestamp}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, ensure_ascii=False, indent=2, default=str)
        file_handle.write("\n")
    return path


def log_debug_file(path: Path | None) -> None:
    if path is not None:
        print(f"Debug file: {path.resolve()}", file=sys.stderr)
