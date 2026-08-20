"""Global test isolation for external services."""

import pytest

from infrastructure.settings import Config
from modules.oddspapi.runtime import reset_oddspapi_runtime_for_tests


@pytest.fixture(autouse=True)
def _disable_oddspapi_account_refresh(monkeypatch):
    """Unit tests must never call the real OddsPapi account endpoint."""
    reset_oddspapi_runtime_for_tests()
    monkeypatch.setattr(
        Config,
        "ENABLE_ODDSPAPI_ACCOUNT_USAGE_REFRESH",
        False,
    )
    yield
    reset_oddspapi_runtime_for_tests()
