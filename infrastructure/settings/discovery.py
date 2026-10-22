"""Discovery policy and schedules. Edit this module and restart the application.

No environment overrides. Credentials/transport remain in Config; shared memory
limits remain in JobExecutionSettings. Evidence: docs/analysis/discovery-filter-plan.md.
Dataclass defaults preserve the original Config/provider defaults. Runtime
instances below pin discovery values from the supplied 2026-10-06 server snapshot.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DiscoveryFilters:
    future_only: bool = True
    minimum_lead_minutes: int = 10
    tracked_competitions_only: bool = False
    exclude_sports: bool = False
    excluded_sports: frozenset[str] = frozenset()  # Canonical IDs, e.g. "handball".
    exclude_categories: bool = True
    # Exact (canonical sport ID, SofaScore category name) pairs; never venue country.
    # Local DB audit 2026-10-22: every stored event in each pair had zero Market
    # rows. Counts are events / distinct SofaScore tournaments / competition IDs;
    # they describe that snapshot and are not a guarantee about future events.
    excluded_categories: frozenset[tuple[str, str]] = frozenset({
        ("basketball", "Croatia Amateur"),  # 146 events / 13 tournaments / 22 competitions; 0 with markets
        ("football", "Bosnia & Herzegovina Amateur"),  # 2,114 / 60 / 72; 0 with markets
        ("football", "Canada Amateur"),  # 66 / 4 / 6; 0 with markets
        ("football", "Colombia Amateur"),  # 260 / 8 / 16; 0 with markets
        ("football", "Cuba"),  # 444 / 19 / 32; 0 with markets
        ("football", "Ecuador Amateur"),  # 288 / 5 / 22; 0 with markets
        ("football", "Guadeloupe"),  # 169 / 5 / 8; 0 with markets
        ("football", "Guinea-Bissau"),  # 428 / 12 / 35; 0 with markets
        ("football", "Haiti"),  # 631 / 43 / 131; 0 with markets
        ("football", "Kenya Amateur"),  # 335 / 12 / 32; 0 with markets
        ("football", "Martinique"),  # 125 / 5 / 9; 0 with markets
        ("football", "Montenegro Amateur"),  # 108 / 4 / 6; 0 with markets
        ("football", "Morocco Amateur"),  # 188 / 10 / 18; 0 with markets
        ("football", "Norway Amateur"),  # 613 / 13 / 15; 0 with markets
        ("football", "Palestine"),  # 351 / 7 / 59; 0 with markets
        ("football", "Portugal Amateur"),  # 1,351 / 75 / 152; 0 with markets
        ("football", "Romania Amateur"),  # 4,669 / 138 / 219; 0 with markets
        ("football", "Russia Amateur"),  # 1,981 / 70 / 98; 0 with markets
        ("football", "Slovakia Amateur"),  # 459 / 16 / 17; 0 with markets
        ("football", "Slovenia Amateur"),  # 589 / 23 / 27; 0 with markets
        ("football", "South Sudan"),  # 102 / 5 / 5; 0 with markets
        ("football", "Sweden Amateur"),  # 1,275 / 28 / 38; 0 with markets
        ("football", "Ukraine Amateur"),  # 341 / 17 / 21; 0 with markets
        ("handball", "Romania Amateur"),  # 514 / 6 / 52; 0 with markets
    })
    exclude_competitions: bool = True
    # SofaScore uniqueTournament IDs, including when evaluating canonical events
    # for OddsPapi. These are NOT competitions.competition_id or OddsPapi IDs.
    excluded_sofascore_unique_tournament_ids: frozenset[int] = frozenset({
        18944,  # Football NCAA I women USA, regular season; 2,634/2,643 lack markets
        28543,  # Football NCAA II Women USA (1,952 events; all women)
        36033,  # Football NCAA III Women USA (2,976 events; all women) 
        15946,  # Football NCAA men; 1,473/1,476 lack markets (division unspecified, likely to be D1 (I))
        28541,  # Football NCAA II Men USA
        36031,  # Football NCAA III Men USA
        27118,  # Football Germany Friendly Games
        27113,  # Football Bulgaria Friendly Games
        28932,  # Football Türkiye Friendly Games
        35960,  # Football England Friendly Games
        27115,  # Football Netherlands Friendly Games
        # Additional SofaScore unique tournaments with >=100 stored events and
        # no market-bearing event in the 2026-10-22 local database audit.
        23109,  # Football Switzerland 4. Liga FVRZ (360; gender unknown in 26)
        33809,  # Football Türkiye U14 Gelişim Ligi (558 events; stored as Men)
        33808,  # Football Türkiye U15 Gelişim Ligi (552 events; stored as Men)
        37258,  # Football Coupe de France Féminine, regional phase (480 women)
        22139,  # Football Belarus Vtoraya Liga (438 men)
        24254,  # Football Romania Campionatul Național U19 (411 men)
        33800,  # Football Türkiye U16 Gelişim Ligi (358 men)
        33726,  # Football Türkiye U17 Gelişim Ligi (353 men)
        37028,  # Football Türkiye U15 Kızlar Gelişim Ligi (345 women)
        35865,  # Football Poland Klasa B, Zachodniopomorska (335; gender unknown in 66)
        20373,  # Football Poland Klasa Okręgowa, Zachodniopomorska (317 men)
        22694,  # Football Brazil U17 Paulista (313 men)
        22752,  # Football Brazil U15 Paulista (311 men)
        22800,  # Football England Northern Counties East League (309 men)
        35742,  # Football Germany Bezirksliga Hamburg (301 men)
        23095,  # Football Switzerland 3. Liga FVRZ (284; gender unknown in 69)
        30706,  # Football Italy U17 Allievi Nazionali (275 men)
        37514,  # Football Italy Campionato Regionale Italiana Serie 2
        22858,  # Football Nigeria NLO Division Two (270; gender unknown in 113)
        20426,  # Football Estonia II Liiga (270 men)
        36637,  # Football Romania Liga Elitelor U16 (269 men)
        36575,  # Football Romania U15 Liga Elitelor (268 men)

        19510,  # American football NCAA men, regular season USA (division not recorded)
        32199,  # American football division FBA Men USA
        32200,  # American football division FCS Men USA 
        32201,  # American football division interdivisional Men USA 

        19468,  # Baseball Dominican Summer League
        20468,  # Baseball Pioneer League
        19464,  # Baseball American Association
        19466,  # Baseball Atlantic League
        19465,  # Baseball Frontier League
        19467,  # Baseball Northwoods League (375 men)
        19478,  # Baseball Double-A Eastern League (338 men)
        19481,  # Baseball High-A South Atlantic League (303 men)
        19480,  # Baseball High-A Midwest League (303 men)
        19485,  # Baseball Single-A Carolina League (299 men)
        19477,  # Baseball Double-A Texas League (287 men)

        37825,  # Basketball NCAA Men Philippines
        23300,  # Basketball France Nationale 3 (274 men)

        9409,   # Handball Club Friendly Games
        1416,   # Handball 1.Division Denmark
        13506,  # Handball Division de honor plata Spain

        14317,  # Ice Hockey NCAA I Woman
        14285,  # Ice Hockey NCAA I Men

        23428,  # Volleyball NCAA women, regular season USA(division not recorded)
    })

    def __post_init__(self):
        if self.minimum_lead_minutes < 0:
            raise ValueError("minimum_lead_minutes must be non-negative")


@dataclass(frozen=True, slots=True)
class SofascoreDiscoverySettings:
    filters: DiscoveryFilters = field(default_factory=DiscoveryFilters)
    round_filter_enabled: bool = True
    allowed_rounds: frozenset[str] = frozenset({"regular-season"}) # default value regular-season
    tennis_ranking_filter_enabled: bool = False
    tennis_ranking_cutoff: int = 120  # Reject when any known participant rank >= cutoff.
    dropping_interval_hours: int = 6
    secondary_interval_hours: int = 6
    daily_check_interval_minutes: int = 30
    daily_advance_time: str = "17:07"
    daily_refresh_time: str = "08:07"
    daily_progress_retention_days: int = 1
    team_event_workers: int = 10
    odds_workers: int = 5
    team_streaks_require_odds: bool = True  # Existing feed policy: fetch odds before admitting events.

    def __post_init__(self):
        if any(not isinstance(value, str) or not value.strip() for value in self.allowed_rounds):
            raise ValueError("allowed_rounds must contain non-empty strings")
        object.__setattr__(self, "allowed_rounds", frozenset(
            value.strip().lower().replace("_", "-") for value in self.allowed_rounds
        ))
        for name in ("dropping_interval_hours", "secondary_interval_hours", "daily_check_interval_minutes",
                     "team_event_workers", "odds_workers", "daily_progress_retention_days",
                     "tennis_ranking_cutoff"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        _validate_times((self.daily_advance_time, self.daily_refresh_time))

    @property
    def dropping_times(self) -> tuple[str, ...]:
        return tuple(f"{hour:02d}:12" for hour in range(0, 24, self.dropping_interval_hours))

    @property
    def secondary_times(self) -> tuple[str, ...]:
        return tuple(f"{hour:02d}:02" for hour in range(0, 24, self.secondary_interval_hours))


@dataclass(frozen=True, slots=True)
class OddspapiDiscoverySettings:
    filters: DiscoveryFilters = field(default_factory=DiscoveryFilters)
    scheduled_times: tuple[str, ...] = ("17:47",)
    reconciliation_enabled: bool = False  # Replay success after newer Daily Discovery completion.
    catchup_lookback_hours: int = 36
    max_catchup_runs: int = 2
    status_id: int = 0
    request_has_odds: bool | None = False  # Provider request parameter, not admission policy.
    language: str = "en"
    lookahead_days: int = 1
    persist_queue: bool = False
    max_request_window_hours: int = 48
    persistence_chunk_size: int = 50

    def __post_init__(self):
        _validate_times(self.scheduled_times)
        if self.catchup_lookback_hours < 0 or self.max_catchup_runs < 0:
            raise ValueError("catchup limits must be non-negative")
        for name in ("lookahead_days", "max_request_window_hours", "persistence_chunk_size"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")


def _validate_times(times):
    from datetime import datetime

    for time in times:
        datetime.strptime(time, "%H:%M")


# Runtime configuration from copies/server/06-10-2026/.env.prod. Values absent
# from that snapshot use the original Config/provider job defaults above.
# Each provider owns its switches and can override the shared audited lists.
SOFASCORE = SofascoreDiscoverySettings(
    round_filter_enabled=True,
    allowed_rounds=frozenset({"regular-season", "knockouts/playoffs"}),
    # Optional tennis policy enabled explicitly; the constructor defaults to disabled.
    tennis_ranking_filter_enabled=True,
    tennis_ranking_cutoff=120,
    dropping_interval_hours=3,
    secondary_interval_hours=6,
    daily_check_interval_minutes=30,
    daily_advance_time="17:07",
    daily_refresh_time="08:07",
    daily_progress_retention_days=1,
    filters=DiscoveryFilters(
        future_only=True,
        minimum_lead_minutes=10,
        tracked_competitions_only=False,
        exclude_sports=False,
        exclude_categories=True,
        exclude_competitions=True,
    ),
)
ODDSPAPI = OddspapiDiscoverySettings(
    scheduled_times=("17:47",),
    reconciliation_enabled=False,
    catchup_lookback_hours=36,
    max_catchup_runs=2,
    filters=DiscoveryFilters(
        future_only=True,
        minimum_lead_minutes=10,
        tracked_competitions_only=False,
        exclude_sports=False,
        exclude_categories=True,
        exclude_competitions=True,
    ),
)
