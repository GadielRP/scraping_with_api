from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Competition
from infrastructure.persistence.repositories.event_batch_writer import _upsert_references


def test_source_uses_one_physical_column_for_reads_and_upserts():
    engine = create_engine("sqlite://")
    try:
        Competition.__table__.create(engine)
        columns = {column["name"] for column in inspect(engine).get_columns("competitions")}
        assert "league_config_source" not in columns
        assert "competition_config_source" in columns

        with Session(engine) as session:
            rows = {(23, 33): {
                "source": "sofascore", "source_tournament_id": 33,
                "source_unique_tournament_id": 23,
                "canonical_name": "Serie A", "display_name": "Serie A",
                "competition_config_source": "manual_config",
            }}
            keys = ["source", "source_tournament_id"]
            saved = _upsert_references(session, Competition, rows, keys)
            assert saved[("sofascore", 33)].competition_config_source == "manual_config"
            rows[(23, 33)]["competition_config_source"] = "competition_rules"
            saved = _upsert_references(session, Competition, rows, keys)
            assert saved[("sofascore", 33)].competition_config_source == "competition_rules"
            session.expire_all()
            assert session.query(Competition).one().competition_config_source == "competition_rules"
    finally:
        engine.dispose()
