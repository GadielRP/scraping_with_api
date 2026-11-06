"""Historical playoff samples include events with a known knockout phase."""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from modules.alerts.basketball_4q.predictor import Basketball4QPredictor


def test_playoff_history_includes_specific_rounds_and_excludes_regular_season():
    with Session(create_engine("sqlite://")) as session:
        session.execute(text("""
            CREATE TABLE basketball_results (
                round TEXT,
                quarter_1_home INT, quarter_2_home INT, quarter_3_home INT,
                quarter_1_away INT, quarter_2_away INT, quarter_3_away INT
            )
        """))
        for stage, score in (("knockouts/playoffs", 4), ("final", 6),
                             ("semifinal", 8), ("quarterfinal", 10), ("regular_season", 100)):
            session.execute(text("INSERT INTO basketball_results VALUES (:stage, :s, :s, :s, :s, :s, :s)"),
                            {"stage": stage, "s": score})
        predictor = Basketball4QPredictor()
        assert predictor.get_historical_q1_q3_combined_avg("playoffs", session) == pytest.approx(42)
        assert predictor.get_historical_q1_q3_combined_avg("regular_season", session) == pytest.approx(600)
