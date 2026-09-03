"""Logs must distinguish profile blockers from optional or ambiguous inputs."""

import logging

import pytest

from modules.pillars.extraction_logging import log_extraction_diagnostics


@pytest.mark.parametrize("pillar", ["P2", "P3"])
def test_period_logs_keep_optional_absence_out_of_required_blockers(caplog, pillar):
    caplog.set_level(logging.INFO)
    log_extraction_diagnostics(
        logging.getLogger(__name__), pillar=pillar, event_id=232565, target_minute=5,
        full_time_requirement="books", debug_mode=False,
        periods={
            "full_time": {"status": "AMBIGUOUS", "missing_inputs": ["LINE", "PRICE"],
                          "ambiguous_inputs": ["LINE"], "invalid_inputs": ["PRICE"]},
            "exchange": {"status": "INCOMPLETE", "missing_inputs": ["OPTIONAL_BF_PRICE"]},
        },
    )
    required, optional = [record.getMessage() for record in caplog.records]
    assert "required=True | blocks_profile=True" in required
    assert "missing_only=[] | invalid=['PRICE'] | ambiguous=['LINE']" in required
    assert "OPTIONAL_BF_PRICE" not in required
    assert "required=False | blocks_profile=False" in optional
    assert "missing_only=['OPTIONAL_BF_PRICE']" in optional
