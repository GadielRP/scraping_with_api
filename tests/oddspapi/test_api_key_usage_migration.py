from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import OddspapiApiKeyUsage


def test_usage_schema_setup_never_downgrades_timestamptz_to_naive():
    calls = []
    manager = DatabaseManager.__new__(DatabaseManager)
    manager._create_table_and_indexes = (
        lambda model, indexes: calls.append((model, indexes))
    )

    manager._migrate_oddspapi_api_key_usage()

    assert calls == [(OddspapiApiKeyUsage, [])]
