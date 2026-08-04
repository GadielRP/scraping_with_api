import asyncio

from modules.oddsportal.scraper_hover import OddsPortalHoverMixin
from modules.oddsportal.scraper_data import OddsPortalDataMixin
from modules.oddsportal.dataclasses import BookieOdds
from modules.oddsportal.oddsportal_config import (
    build_bookie_identity_groups,
    select_configured_bookies,
)
from modules.oddsportal.oddsportal_routes import flatten_sport_scraping_route
from modules.oddsportal.scraping_settings import ODDSPORTAL_SCRAPING_SETTINGS
from shared.odds_utils import normalize_odds_value


def _history_tooltip(
    *,
    values: list[str],
    opening: str,
    opening_time: str | None = None,
    betfair: bool = False,
) -> str:
    """Build the relevant DOM shape observed in the supplied HTML samples."""
    dates = "".join(
        f'<div class="font-normal">31 Jul, {time}</div>'
        for time in ("13:39", "12:30", "11:10")
    )
    odds = "".join(f'<div class="font-bold">{value}</div>' for value in values)
    volumes = (
        '<div class="font-bold">€123</div><div class="font-bold">+1/50</div>'
        if betfair
        else ""
    )
    opening_date = (
        f'<div class="font-normal">{opening_time}</div>'
        if opening_time
        else ""
    )
    return f"""
        <div>
          <h3>Odds movement</h3>
          <div class="grid">{dates}{odds}{volumes}</div>
          <div class="mt-2 gap-1">
            <div>Opening odds:</div>{opening_date}<div class="font-bold">{opening}</div>
          </div>
        </div>
    """


def test_regular_tooltip_uses_opening_block_value_and_date():
    parser = OddsPortalHoverMixin()

    result = parser._parse_opening_odds_tooltip_html(
        _history_tooltip(
            values=["2.10", "2.08", "2.06"],
            opening="2.05",
            opening_time="30 Jul, 12:58",
        )
    )

    assert result == ("2.05", "30 Jul, 12:58")


def test_betfair_tooltip_uses_opening_block_and_ignores_history_and_volume():
    parser = OddsPortalHoverMixin()

    result = parser._parse_opening_odds_tooltip_html(
        _history_tooltip(
            values=["2.14", "2.12", "2.10"],
            opening="2.12",
            opening_time="30 Jul, 23:38",
            betfair=True,
        )
    )

    assert result == ("2.12", "30 Jul, 23:38")


def test_fractional_opening_value_is_normalized_to_decimal():
    parser = OddsPortalHoverMixin()

    result = parser._parse_opening_odds_tooltip_html(
        _history_tooltip(
            values=["57/50", "28/25", "11/10"],
            opening="28/25",
            opening_time="30 Jul, 23:38",
            betfair=True,
        )
    )

    assert result == ("2.12", "30 Jul, 23:38")


def test_tooltip_without_opening_block_is_rejected():
    parser = OddsPortalHoverMixin()
    tooltip = """
        <div>
          <h3>Odds movement</h3>
          <div class="font-normal">31 Jul, 12:30</div>
          <div class="font-bold">2.05</div>
        </div>
    """

    assert parser._parse_opening_odds_tooltip_html(tooltip) is None


def test_spanish_movement_title_is_supported():
    parser = OddsPortalHoverMixin()
    tooltip = """
        <div>
          <h3>Movimiento de cuotas</h3>
          <div class="columns">
            <div class="font-normal">31 Jul, 10:00</div>
            <div class="font-normal">31 Jul, 09:00</div>
            <div class="font-bold">7/4</div>
            <div class="font-bold">3.00</div>
          </div>
          <div class="mt-2 gap-1">
            <div>Cuotas de apertura:</div>
            <div class="font-bold">9/4</div>
          </div>
        </div>
    """

    assert parser._parse_opening_odds_tooltip_html(tooltip) == (
        "3.25",
        None,
    )


def test_odds_normalizer_auto_detects_format_and_rejects_movement_deltas():
    assert normalize_odds_value("2.10") == "2.1"
    assert normalize_odds_value("57/50") == "2.14"
    assert normalize_odds_value("28/25") == "2.12"
    assert normalize_odds_value("2,25") == "2.25"
    assert normalize_odds_value("+1/50") is None
    assert normalize_odds_value("-0.05") is None
    assert normalize_odds_value("-") is None


def _bookies(*names: str) -> list[BookieOdds]:
    return [BookieOdds(name=name) for name in names]


def test_persistence_selection_supports_page_order_allowlist_and_limit():
    available = _bookies("Unibet", "bet365", "Pinnacle", "1xBet")

    assert [b.name for b in select_configured_bookies(available, None, 2)] == [
        "Unibet",
        "bet365",
    ]
    assert [
        b.name
        for b in select_configured_bookies(
            available,
            ["Pinnacle", "bet365", "Unibet"],
            2,
        )
    ] == ["Pinnacle", "bet365"]


def test_bookie_selection_matches_source_and_database_aliases_exactly():
    available = _bookies("1xBet", "888sport", "Betfair Sportsbook")

    selected = select_configured_bookies(
        available,
        ["888 Sport", "1xbet", "Bet"],
        None,
    )

    assert [bookie.name for bookie in selected] == ["888sport", "1xBet"]


def test_hover_selection_is_the_regular_persistence_selection():
    available = _bookies("Unibet", "bet365", "Pinnacle", "1xBet")
    hovered_and_persisted = select_configured_bookies(
        available,
        ["1xBet", "Pinnacle", "bet365"],
        2,
    )

    assert [bookie.name for bookie in hovered_and_persisted] == [
        "1xBet",
        "Pinnacle",
    ]
    assert select_configured_bookies(available, None, 0) == []
    assert select_configured_bookies(available, [], None) == []
    assert select_configured_bookies(available, (), None) == []


def test_browser_identity_groups_follow_configured_priority():
    assert build_bookie_identity_groups(("Pinnacle", "bet365")) == [
        ["pinnacle"],
        ["bet365"],
    ]


def test_standard_extraction_receives_hover_policy_at_browser_boundary():
    class FakePage:
        def __init__(self):
            self.expression = None
            self.selection_config = None

        async def evaluate(self, expression, selection_config):
            self.expression = expression
            self.selection_config = selection_config
            configured_names = ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_names
            bookies = []
            if configured_names and selection_config["bookmakerLimit"] > 0:
                bookies.append(
                    {
                        "name": configured_names[0],
                        "odds1": "1.70",
                        "oddsX": None,
                        "odds2": "2.10",
                        "payout": "-",
                    }
                )
            return {
                "homeTeam": "Home",
                "awayTeam": "Away",
                "bookies": bookies,
                "betfairBack": None,
                "betfairStatus": "section_not_found",
                "betfairContainerCount": 0,
            }

    page = FakePage()
    result = asyncio.run(OddsPortalDataMixin()._extract_data(page, "match-url"))

    assert page.selection_config == {
        "bookmakerIdentityGroups": build_bookie_identity_groups(
            ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_names
        ),
        "bookmakerLimit": ODDSPORTAL_SCRAPING_SETTINGS.bookmakers.hover_limit,
    }
    assert "selectedRows" in page.expression
    assert len(result.bookie_odds) <= page.selection_config["bookmakerLimit"]


def test_package_settings_satisfy_scrape_policy_contract():
    settings = ODDSPORTAL_SCRAPING_SETTINGS

    assert settings.ui_language in {"en", "es"}
    assert settings.domain == (
        "oddsportal.com"
        if settings.ui_language == "en"
        else "cuotasahora.com"
    )
    assert isinstance(settings.bookmakers.hover_names, tuple)
    assert settings.bookmakers.hover_limit >= 0
    assert isinstance(settings.bookmakers.persist_betfair, bool)
    assert isinstance(settings.bookmakers.hover_betfair, bool)


def test_every_sport_route_contains_only_standard_full_time_market():
    expected = {
        "football": ("1X2", "FULL_TIME", "#1X2;2"),
        "basketball": ("HOME_AWAY", "FT_INC_OT", "#home-away;1"),
        "american-football": ("HOME_AWAY", "FT_INC_OT", "#home-away;1"),
        "baseball": ("HOME_AWAY", "FT_INC_OT", "#home-away;1"),
        "hockey": ("HOME_AWAY", "FT_INC_OT", "#home-away;1"),
    }

    for sport, (group_key, period_key, fragment) in expected.items():
        steps = flatten_sport_scraping_route(sport)
        assert len(steps) == 1
        assert steps[0]["group_key"] == group_key
        assert steps[0]["period_key"] == period_key
        assert steps[0]["db_market_period"] == "Full Time"
        assert steps[0]["fragment"] == fragment
        assert steps[0]["extract_fn"] == "standard"
        assert steps[0]["betfair_enabled"] is True
