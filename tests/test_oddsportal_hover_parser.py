import asyncio
import logging

from modules.oddsportal.scraper_attempt import OddsPortalAttemptMixin
from modules.oddsportal.scraper_hover import OddsPortalHoverMixin
from modules.oddsportal.scraper_hover import TooltipOddsSnapshot
from modules.oddsportal.scraper_data import OddsPortalDataMixin
from modules.oddsportal.dataclasses import BetfairExchangeOdds, BookieOdds
from modules.oddsportal.oddsportal_config import (
    build_bookie_identity_groups,
    select_configured_bookies,
)
from modules.oddsportal.oddsportal_routes import flatten_sport_scraping_route
from modules.oddsportal.scraper_impl import OddsPortalScraper
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


def test_attached_tooltip_selects_latest_timestamped_current_not_visible_value():
    parser = OddsPortalHoverMixin()
    fixture = """
        <div class="tooltip odds-tooltip">
          <h3>Odds movement</h3>
          <div class="flex flex-row gap-3">
            <div class="flex flex-col gap-1">
              <div class="font-normal">05 Aug, 05:50</div>
              <div class="font-normal">05 Aug, 10:01</div>
              <div class="font-normal">05 Aug, 09:53</div>
            </div>
            <div class="flex flex-col gap-1">
              <div class="font-bold">1.48</div>
              <div class="font-bold">1.42</div>
              <div class="font-bold">1.37</div>
            </div>
            <div class="flex flex-col gap-1">
              <div class="font-bold text-green-dark">+0.06</div>
              <div class="font-bold text-green-dark">+0.05</div>
              <div class="font-bold text-red-dark">-0.01</div>
            </div>
          </div>
          <div class="mt-2 gap-1">
            <div class="font-bold">Opening odds:</div>
            <div class="flex gap-1">
              <div class="font-normal">04 Aug, 14:41</div>
              <div class="font-bold">1.86</div>
            </div>
          </div>
        </div>
    """

    result = parser._parse_odds_tooltip_html(fixture)

    assert result == TooltipOddsSnapshot(
        opening_odds="1.86",
        opening_time="04 Aug, 14:41",
        current_odds="1.42",
        current_time="05 Aug, 10:01",
    )
    assert result.movement_odds == ("1.48", "1.42", "1.37")


def test_tooltip_identity_validation_rejects_another_cells_history():
    parser = OddsPortalHoverMixin()
    parsed = TooltipOddsSnapshot(
        opening_odds="1.86",
        current_odds="1.42",
        movement_odds=("1.48", "1.42", "1.37"),
    )

    assert parser._tooltip_matches_visible_cell(parsed, "1.48") is True
    assert parser._tooltip_matches_visible_cell(parsed, "2.85") is False


def test_row_based_exchange_history_selects_latest_timestamped_quote():
    parser = OddsPortalHoverMixin()
    tooltip = """
        <h3>Odds movement</h3>
        <div class="flex flex-col gap-1">
          <div class="flex flex-row gap-3">
            <div class="font-normal">05 Aug, 09:58</div>
            <div class="font-bold">1.48</div>
            <div class="font-normal">(28)</div>
            <div class="font-bold text-red-dark">-0.04</div>
          </div>
          <div class="flex flex-row gap-3">
            <div class="font-normal">05 Aug, 10:02</div>
            <div class="font-bold">1.44</div>
            <div class="font-normal">(31)</div>
            <div class="font-bold text-green-dark">+0.02</div>
          </div>
        </div>
        <div class="mt-2 gap-1">
          <div class="font-bold">Opening odds:</div>
          <div><div class="font-normal">04 Aug, 14:41</div><div class="font-bold">1.86</div></div>
        </div>
    """

    result = parser._parse_odds_tooltip_html(tooltip)

    assert result.current_odds == "1.44"
    assert result.current_time == "05 Aug, 10:02"
    assert result.opening_odds == "1.86"
    assert result.movement_odds == ("1.48", "1.44")


def test_distinct_selection_tooltips_do_not_share_opening_values():
    parser = OddsPortalHoverMixin()
    home = parser._parse_odds_tooltip_html(
        _history_tooltip(
            values=["1.44", "1.48"],
            opening="1.86",
            opening_time="04 Aug, 14:41",
        )
    )
    away = parser._parse_odds_tooltip_html(
        _history_tooltip(
            values=["2.85", "2.90"],
            opening="2.65",
            opening_time="04 Aug, 14:42",
        )
    )

    bookie = BookieOdds(name="bet365", odds_1="1.48", odds_2="2.90")
    OddsPortalAttemptMixin()._apply_tooltip_prices(
        bookie,
        {"1": home, "2": away},
        exchange=False,
    )

    assert bookie.initial_odds_1 == "1.86"
    assert bookie.initial_odds_2 == "2.65"


def test_per_choice_log_reports_exact_opening_and_current(caplog):
    caplog.set_level(logging.INFO)
    prices = {
        "1": TooltipOddsSnapshot(
            opening_odds="1.86",
            opening_time="04 Aug, 14:41",
            current_odds="1.44",
            current_time="05 Aug, 10:02",
        ),
        "2": TooltipOddsSnapshot(
            opening_odds="2.65",
            opening_time="04 Aug, 14:42",
            current_odds="2.85",
            current_time="05 Aug, 10:03",
        ),
    }

    OddsPortalAttemptMixin._log_tooltip_price_details(
        source_label="bet365",
        market_period="Full Time",
        expected_keys=["1", "2"],
        tooltip_prices=prices,
    )

    assert "choice=1 opening=1.86 opening_time=04 Aug, 14:41 current=1.44 current_time=05 Aug, 10:02" in caplog.text
    assert "choice=2 opening=2.65 opening_time=04 Aug, 14:42 current=2.85 current_time=05 Aug, 10:03" in caplog.text


def test_debug_mode_saves_one_accepted_tooltip_per_bookie_choice(tmp_path):
    debug_root = tmp_path / "debug"
    scraper = OddsPortalScraper(
        debug_dir=str(debug_root),
        debug_mode=True,
    )
    scraper.set_debug_event_context(156595)

    regular_path = scraper._save_parsed_tooltip_html(
        tooltip_html="<div>regular tooltip</div>",
        source="bookmaker",
        bookie_name="bet365",
        choice="2",
    )
    exchange_path = scraper._save_parsed_tooltip_html(
        tooltip_html="<div>exchange tooltip</div>",
        source="betfair",
        bookie_name=None,
        choice="lay_1",
    )

    event_dir = debug_root / "oddsportal_156595_tooltips"
    assert regular_path == str(event_dir / "bet365_2_tooltip.html")
    assert exchange_path == str(
        event_dir / "betfair_exchange_lay_1_tooltip.html"
    )
    assert (event_dir / "bet365_2_tooltip.html").read_text(
        encoding="utf-8"
    ) == "<div>regular tooltip</div>"
    assert (event_dir / "betfair_exchange_lay_1_tooltip.html").read_text(
        encoding="utf-8"
    ) == "<div>exchange tooltip</div>"


def test_tooltip_debug_files_are_disabled_without_debug_mode(tmp_path):
    scraper = OddsPortalScraper(
        debug_dir=str(tmp_path / "debug"),
        debug_mode=False,
    )
    scraper.set_debug_event_context(156595)

    saved_path = scraper._save_parsed_tooltip_html(
        tooltip_html="<div>must not be saved</div>",
        source="bookmaker",
        bookie_name="bet365",
        choice="1",
    )

    assert saved_path is None
    assert not (tmp_path / "debug" / "oddsportal_156595_tooltips").exists()


def test_regular_tooltip_current_overwrites_legacy_visible_value():
    merger = OddsPortalAttemptMixin()
    bookie = BookieOdds(name="bet365", odds_1="1.48")

    opening_keys, current_keys = merger._apply_tooltip_prices(
        bookie,
        {
            "1": TooltipOddsSnapshot(
                opening_odds="1.86",
                opening_time="04 Aug, 14:41",
                current_odds="1.42",
                current_time="05 Aug, 10:01",
            )
        },
        exchange=False,
    )

    assert opening_keys == ["1"]
    assert current_keys == ["1"]
    assert bookie.initial_odds_1 == "1.86"
    assert bookie.initial_odds_1_time == "04 Aug, 14:41"
    assert bookie.odds_1 == "1.42"
    assert bookie.odds_1_time == "05 Aug, 10:01"
    assert bookie.movement_odds_time == "05 Aug, 10:01"


def test_missing_tooltip_current_retains_legacy_visible_value():
    merger = OddsPortalAttemptMixin()
    bookie = BookieOdds(name="bet365", odds_1="1.48")

    opening_keys, current_keys = merger._apply_tooltip_prices(
        bookie,
        {
            "1": TooltipOddsSnapshot(
                opening_odds="1.86",
                opening_time="04 Aug, 14:41",
            )
        },
        exchange=False,
    )

    assert opening_keys == ["1"]
    assert current_keys == []
    assert bookie.odds_1 == "1.48"
    assert bookie.odds_1_time is None


def test_exchange_tooltip_updates_back_and_lay_without_second_hover():
    merger = OddsPortalAttemptMixin()
    exchange = BetfairExchangeOdds(back_1="1.48", lay_1="1.50")

    merger._apply_tooltip_prices(
        exchange,
        {
            "back_1": TooltipOddsSnapshot(
                opening_odds="1.86",
                opening_time="04 Aug, 14:41",
                current_odds="1.42",
                current_time="05 Aug, 10:01",
            ),
            "lay_1": TooltipOddsSnapshot(
                opening_odds="1.90",
                opening_time="04 Aug, 14:42",
                current_odds="1.44",
                current_time="05 Aug, 10:02",
            ),
        },
        exchange=True,
    )

    assert exchange.back_1 == "1.42"
    assert exchange.initial_back_1 == "1.86"
    assert exchange.back_1_time == "05 Aug, 10:01"
    assert exchange.lay_1 == "1.44"
    assert exchange.initial_lay_1 == "1.90"
    assert exchange.lay_1_time == "05 Aug, 10:02"
    assert exchange.movement_odds_time == "05 Aug, 10:02"


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
