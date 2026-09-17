import json
from pathlib import Path

from modules.sofascore.event_normalizer import get_event_information


def test_get_event_information_returns_normalized_entities_for_sofascore_event():
    response_path = Path(__file__).resolve().parents[1] / "response.json"
    response = json.loads(response_path.read_text(encoding="utf-8"))

    event_data = get_event_information(response["event"])

    assert event_data["event"]["id"] == 15951870
    assert event_data["home_participant"]["source_participant_id"] == 257523
    assert event_data["away_participant"]["source_participant_id"] == 3675
    assert event_data["competition_ref"]["source_tournament_id"] == 10560
    assert event_data["competition_ref"]["source_unique_tournament_id"] == 234
    assert event_data["competition_ref"]["canonical_name"] == "NHL"
    assert event_data["competition_ref"]["display_name"] == "NHL, Playoffs"
    assert event_data["competition_ref"]["category_id"] == 37
    assert event_data["competition_ref"]["category_name"] == "USA"

    assert event_data["event"]["homeTeam"] == "Vegas Golden Knights"
    assert event_data["event"]["awayTeam"] == "Anaheim Ducks"
    assert event_data["event"]["competition"] == "USA, NHL, Playoffs"
