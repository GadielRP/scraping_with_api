"""Adapt OddsPortal scrape objects into canonical, persistence-ready DTOs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from modules.odds_ingestion.canonical_market_resolver import resolve_oddsportal_key
from modules.oddsportal.oddsportal_routes import flatten_sport_scraping_route
from modules.oddsportal.timestamps import oddsportal_tooltip_time_to_iso
from shared.odds_utils import normalize_odds_value


@dataclass(frozen=True, slots=True)
class CanonicalChoicePayload:
    name: str
    current_odds: str
    initial_odds: str | None = None
    initial_changed_at: str | None = None
    source_collected_at: str | None = None

    def as_repository_dict(self) -> dict:
        payload = {
            "name": self.name,
            "currentOdds": self.current_odds,
        }
        if self.initial_odds is not None:
            payload["initialOdds"] = self.initial_odds
        if self.initial_changed_at:
            payload["initialChangedAt"] = self.initial_changed_at
        if self.source_collected_at:
            payload["sourceCollectedAt"] = self.source_collected_at
        return payload


@dataclass(frozen=True, slots=True)
class CanonicalMarketPayload:
    canonical_market_key: str
    market_name: str
    market_group: str
    market_period: str
    choice_group: str | None
    choices: tuple[CanonicalChoicePayload, ...]
    is_live: bool = False

    def as_repository_dict(self) -> dict:
        return {
            "canonicalMarketKey": self.canonical_market_key,
            "marketName": self.market_name,
            "marketGroup": self.market_group,
            "marketPeriod": self.market_period,
            "choiceGroup": self.choice_group,
            "isLive": self.is_live,
            "choices": [choice.as_repository_dict() for choice in self.choices],
        }


@dataclass(frozen=True, slots=True)
class OddsPortalBookmakerPayload:
    source_name: str
    source_slug: str
    markets: tuple[CanonicalMarketPayload, ...]


@dataclass(frozen=True, slots=True)
class OddsPortalOddsResponse:
    bookmakers: tuple[OddsPortalBookmakerPayload, ...]
    diagnostics: tuple[dict, ...] = ()

    @property
    def markets_detected(self) -> int:
        return sum(len(bookmaker.markets) for bookmaker in self.bookmakers)

    @property
    def choices_detected(self) -> int:
        return sum(
            len(market.choices)
            for bookmaker in self.bookmakers
            for market in bookmaker.markets
        )


class OddsPortalMarketAdapter:
    """Canonicalize source route semantics without performing database I/O."""

    _EXPECTED_ROLES = {
        "side_3way": ("1", "x", "2"),
        "side_2way": ("1", "2"),
        "spread_2way": ("1", "2"),
        "total": ("over", "under"),
        "team_total": ("over", "under"),
    }

    @staticmethod
    def _slugify(name: str) -> str:
        normalized = str(name or "").strip().lower().replace("&", " and ")
        return re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")

    @staticmethod
    def _line(value) -> str | None:
        if value in (None, ""):
            return None
        try:
            number = float(str(value).strip())
        except (TypeError, ValueError):
            return None
        if number == 0:
            return "0"
        if number.is_integer():
            return str(int(number))
        return str(number).rstrip("0").rstrip(".")

    @staticmethod
    def _odds(value) -> str | None:
        return normalize_odds_value(value)

    @classmethod
    def _choices(
        cls,
        *,
        family: str,
        current: Mapping[str, Any],
        initial: Mapping[str, Any],
        initial_changed_at: Mapping[str, Any],
        source_collected_at: Mapping[str, Any],
        reference_time: datetime,
    ) -> tuple[CanonicalChoicePayload, ...]:
        expected = cls._EXPECTED_ROLES.get(family)
        if not expected:
            return ()

        choices = []
        for role in expected:
            current_odds = cls._odds(current.get(role))
            if current_odds is None:
                return ()
            choices.append(
                CanonicalChoicePayload(
                    name=role,
                    current_odds=current_odds,
                    initial_odds=cls._odds(initial.get(role)),
                    initial_changed_at=oddsportal_tooltip_time_to_iso(
                        initial_changed_at.get(role),
                        reference_time=reference_time,
                    ),
                    source_collected_at=oddsportal_tooltip_time_to_iso(
                        source_collected_at.get(role),
                        reference_time=reference_time,
                    ),
                )
            )
        return tuple(choices)

    @staticmethod
    def _extractions(odds_data) -> list:
        extractions = list(getattr(odds_data, "extractions", None) or [])
        if extractions:
            return extractions

        # Compatibility for old/synthetic MatchOddsData objects. The current
        # production scraper always emits explicit extractions.
        route = flatten_sport_scraping_route(getattr(odds_data, "sport", None))
        if not route:
            return []
        step = route[0]
        return [
            type(
                "LegacyOddsPortalExtraction",
                (),
                {
                    "source_group_key": step.get("group_key") or "",
                    "source_period_key": step.get("period_key") or "",
                    "market_group": step.get("db_market_group") or "",
                    "market_period": step.get("db_market_period") or "",
                    "market_name": step.get("db_market_name") or "",
                    "bookie_odds": getattr(odds_data, "bookie_odds", None) or [],
                    "betfair": getattr(odds_data, "betfair", None),
                },
            )()
        ]

    @classmethod
    def from_match_odds_data(
        cls,
        odds_data,
        *,
        canonical_types: Mapping[str, Any],
    ) -> OddsPortalOddsResponse:
        markets_by_bookie: dict[tuple[str, str], list[CanonicalMarketPayload]] = {}
        diagnostics: list[dict] = []
        reference_time = datetime.now()

        for extraction in cls._extractions(odds_data):
            canonical_key, reason = resolve_oddsportal_key(
                getattr(extraction, "source_group_key", None),
                getattr(extraction, "source_period_key", None),
            )
            canonical_type = canonical_types.get(canonical_key) if canonical_key else None
            if canonical_type is None:
                diagnostics.append(
                    {
                        "sourceGroupKey": getattr(extraction, "source_group_key", None),
                        "sourcePeriodKey": getattr(extraction, "source_period_key", None),
                        "reason": reason if canonical_key is None else "canonical_market_type_unavailable",
                    }
                )
                continue

            requires_choice_group = bool(canonical_type.requires_choice_group)
            for source_bookie in getattr(extraction, "bookie_odds", None) or []:
                source_name = str(getattr(source_bookie, "name", "") or "").strip()
                source_slug = cls._slugify(source_name)
                choice_group = cls._line(getattr(source_bookie, "handicap", None))
                if not source_name or not source_slug or (requires_choice_group and choice_group is None):
                    diagnostics.append(
                        {
                            "canonicalMarketKey": canonical_key,
                            "sourceBookie": source_name,
                            "reason": "missing_bookie_identity_or_required_choice_group",
                        }
                    )
                    continue

                role_1 = "over" if canonical_type.market_family in {"total", "team_total"} else "1"
                role_2 = "under" if canonical_type.market_family in {"total", "team_total"} else "2"
                choices = cls._choices(
                    family=canonical_type.market_family,
                    current={
                        role_1: getattr(source_bookie, "odds_1", None),
                        "x": getattr(source_bookie, "odds_x", None),
                        role_2: getattr(source_bookie, "odds_2", None),
                    },
                    initial={
                        role_1: getattr(source_bookie, "initial_odds_1", None),
                        "x": getattr(source_bookie, "initial_odds_x", None),
                        role_2: getattr(source_bookie, "initial_odds_2", None),
                    },
                    initial_changed_at={
                        role_1: getattr(source_bookie, "initial_odds_1_time", None),
                        "x": getattr(source_bookie, "initial_odds_x_time", None),
                        role_2: getattr(source_bookie, "initial_odds_2_time", None),
                    },
                    source_collected_at={
                        role_1: getattr(source_bookie, "odds_1_time", None),
                        "x": getattr(source_bookie, "odds_x_time", None),
                        role_2: getattr(source_bookie, "odds_2_time", None),
                    },
                    reference_time=reference_time,
                )
                if not choices:
                    diagnostics.append(
                        {
                            "canonicalMarketKey": canonical_key,
                            "sourceBookie": source_name,
                            "reason": "incomplete_canonical_choices",
                        }
                    )
                    continue
                markets_by_bookie.setdefault((source_name, source_slug), []).append(
                    CanonicalMarketPayload(
                        canonical_market_key=canonical_key,
                        market_name=canonical_type.canonical_market_name,
                        market_group=canonical_type.canonical_market_group,
                        market_period=canonical_type.canonical_market_period,
                        choice_group=choice_group,
                        choices=choices,
                    )
                )

            betfair = getattr(extraction, "betfair", None)
            if betfair is None:
                continue
            choice_group_line = cls._line(getattr(betfair, "handicap", None))
            if requires_choice_group and choice_group_line is None:
                diagnostics.append(
                    {
                        "canonicalMarketKey": canonical_key,
                        "sourceBookie": "Betfair Exchange",
                        "reason": "missing_required_choice_group",
                    }
                )
                continue

            for side in ("back", "lay"):
                role_1 = "over" if canonical_type.market_family in {"total", "team_total"} else "1"
                role_2 = "under" if canonical_type.market_family in {"total", "team_total"} else "2"
                choices = cls._choices(
                    family=canonical_type.market_family,
                    current={
                        role_1: getattr(betfair, f"{side}_1", None),
                        "x": getattr(betfair, f"{side}_x", None),
                        role_2: getattr(betfair, f"{side}_2", None),
                    },
                    initial={
                        role_1: getattr(betfair, f"initial_{side}_1", None),
                        "x": getattr(betfair, f"initial_{side}_x", None),
                        role_2: getattr(betfair, f"initial_{side}_2", None),
                    },
                    initial_changed_at={
                        role_1: getattr(betfair, f"initial_{side}_1_time", None),
                        "x": getattr(betfair, f"initial_{side}_x_time", None),
                        role_2: getattr(betfair, f"initial_{side}_2_time", None),
                    },
                    source_collected_at={
                        role_1: getattr(betfair, f"{side}_1_time", None),
                        "x": getattr(betfair, f"{side}_x_time", None),
                        role_2: getattr(betfair, f"{side}_2_time", None),
                    },
                    reference_time=reference_time,
                )
                if not choices:
                    continue
                side_group = side.title()
                if choice_group_line is not None:
                    side_group = f"{side_group} {choice_group_line}"
                markets_by_bookie.setdefault(
                    ("Betfair Exchange", "betfair-ex"), []
                ).append(
                    CanonicalMarketPayload(
                        canonical_market_key=canonical_key,
                        market_name=canonical_type.canonical_market_name,
                        market_group=canonical_type.canonical_market_group,
                        market_period=canonical_type.canonical_market_period,
                        choice_group=side_group,
                        choices=choices,
                    )
                )

        bookmakers = tuple(
            OddsPortalBookmakerPayload(
                source_name=source_name,
                source_slug=source_slug,
                markets=tuple(markets),
            )
            for (source_name, source_slug), markets in markets_by_bookie.items()
            if markets
        )
        return OddsPortalOddsResponse(
            bookmakers=bookmakers,
            diagnostics=tuple(diagnostics),
        )


__all__ = [
    "CanonicalChoicePayload",
    "CanonicalMarketPayload",
    "OddsPortalBookmakerPayload",
    "OddsPortalOddsResponse",
    "OddsPortalMarketAdapter",
]
