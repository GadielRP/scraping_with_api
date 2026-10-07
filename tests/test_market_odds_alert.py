from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from infrastructure.persistence.repositories.market.market_odds_read_models import (
    ChoiceOddsState, MarketOddsState, MarketOddsReadResult, OddsPrice, QuotePriceOrigin,
)
from modules.alerts.alerts_formatter import odds_alert


def _choice(name, opening, current):
    def price(value, source):
        return None if value is None else OddsPrice(Decimal(value), QuotePriceOrigin(1, source))
    return ChoiceOddsState(1, name, price(opening, "oddsportal"), price(current, "oddspapi"))


def _market(*, bookie="Pinnacle", source=None, side=None, choices=(), line=None):
    return MarketOddsState(
        event_id=7, market_id=100, bookie_id=5, bookie_name=bookie, market_type_id=1,
        canonical_market_key="1x2_full_time", market_name="1X2 Full Time", market_group="1X2",
        market_period="Full Time", market_family="side_3way", line_value=line, is_live=False,
        source=source, exchange_side=side, choices=tuple(choices),
    )


def test_all_bookmakers_share_price_rendering():
    message = odds_alert.format_market_odds([
        _market(choices=(_choice("1", "2.10", "1.95"), _choice("x", "3.20", "3.20"), _choice("2", "4.00", "4.25"))),
        _market(bookie="SofaScore", choices=(_choice("1", "2.00", "2.10"),)),
    ])
    assert "Pinnacle: 1: 2.10→1.95↓ | x: 3.20→3.20= | 2: 4.00→4.25↑" in message
    assert "SofaScore: 1: 2.00→2.10↑" in message
    assert "CONSOLIDATED" not in message


def test_exchange_preserves_provider_side_and_opening_only_prices():
    message = odds_alert.format_market_odds([
        _market(bookie="Betfair Exchange", source="oddsportal", side="back", choices=(_choice("1", "1.89", None),)),
        _market(bookie="Betfair Exchange", source="oddsportal", side="lay", choices=(_choice("1", "1.91", None),)),
    ])
    assert "Betfair Exchange (Back, oddsportal): 1: 1.89→N/A" in message
    assert "Betfair Exchange (Lay, oddsportal): 1: 1.91→N/A" in message


def test_current_only_does_not_fabricate_movement():
    message = odds_alert.format_market_odds([_market(choices=(_choice("1", None, "2.05"),))])
    assert "1: 2.05" in message
    assert "2.05=" not in message


def test_alert_reads_persisted_odds_without_sofascore_payload(monkeypatch):
    calls = []
    result = MarketOddsReadResult(7, (_market(choices=(_choice("1", "2.10", "1.95"),)),))
    monkeypatch.setattr(odds_alert, "MarketOddsReadRepository", lambda: SimpleNamespace(
        get_market_odds_state=lambda event_id: calls.append(event_id) or result))
    monkeypatch.setattr(odds_alert, "is_tracked_competition", lambda _: True)
    monkeypatch.setattr(odds_alert.pre_start_notifier, "telegram_enabled", True)
    monkeypatch.setattr(odds_alert.pre_start_notifier, "send_telegram_message", lambda message: calls.append(message) or True)
    assert odds_alert.send_odds_alert({"id": 7, "home_team": "Home", "away_team": "Away"}, 5)
    assert calls[0] == 7
    assert "Pinnacle: 1: 2.10→1.95↓" in calls[1]


def test_sofascore_single_market_does_not_hide_other_bookmakers(monkeypatch):
    markets = (_market(bookie="SofaScore", choices=(_choice("1", "2", "1.9"),)),
               _market(bookie="bet365", choices=(_choice("1", "2.1", "2"),)))
    monkeypatch.setattr(odds_alert, "MarketOddsReadRepository", lambda: SimpleNamespace(
        get_market_odds_state=lambda _: MarketOddsReadResult(7, markets)))
    monkeypatch.setattr(odds_alert, "is_tracked_competition", lambda _: False)
    monkeypatch.setattr(odds_alert.pre_start_notifier, "telegram_enabled", True)
    sent=[]
    monkeypatch.setattr(odds_alert.pre_start_notifier, "send_telegram_message", lambda message: sent.append(message) or True)
    assert odds_alert.send_odds_alert({"id":7}, 30)
    assert "bet365" in sent[0] and "SofaScore" in sent[0]
