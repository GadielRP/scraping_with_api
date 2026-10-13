"""Capture effective, non-secret calculation configuration and source identity."""

import hashlib
from pathlib import Path
import subprocess
import sys
from decimal import Decimal
from infrastructure.settings import Config
from .fingerprint import content_hash
from .serialization import to_json_value


def calculation_versions():
    root = Path(__file__).resolve().parents[3]
    digest = hashlib.sha256()
    for directory in (
        "modules/pillars",
        "modules/alerts/matchup_streak_analysis",
        "modules/jobs/pre_start_check_job",
        "modules/competition",
        "modules/sofascore",
        "infrastructure/settings",
        "infrastructure/persistence",
        "shared",
    ):
        for path in sorted((root / directory).rglob("*.py")):
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
    requirements = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in ("requirements.txt", "requirements_cloud.txt")
    }
    # Production images exclude .git and do not install the Git executable.
    # The effective source fingerprint remains authoritative in that deployment.
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        commit = None
    configuration = {}
    for name, module in tuple(sys.modules.items()):
        if module is None or not name.startswith("modules.pillars."):
            continue
        values = {}
        for key, value in vars(module).items():
            if not key.isupper() or not isinstance(
                value, (int, float, Decimal, str, tuple, list, dict, set, frozenset)
            ):
                continue
            try:
                values[key] = to_json_value(value)
            except (TypeError, ValueError):
                continue
        if values:
            configuration[name] = values
    configuration["pipeline"] = {
        key: to_json_value(getattr(Config, key))
        for key in dir(Config)
        if key.startswith(("P5_PRICE_MEMORY_", "PRE_START_ODDS_", "MATCHUP_"))
        or key == "PILLAR_PIPELINE_ENABLED_PILLARS"
    }
    return configuration, {
        "contract_version": "pillar-instrumentation-v3-source-reconstruction",
        "commit_sha": commit,
        "code_sha256": digest.hexdigest(),
        "configuration_sha256": content_hash(configuration),
        "requirements_sha256": requirements,
        "python_version": sys.version,
    }
