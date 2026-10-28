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
    # Category selectors cover all divisions and genders within that category.
    # Existing policy exclusions can contain market-bearing events; maintenance
    # must use --markets absent. New scopes were audited across several months.
    # Evidence: docs/discovery-expansion-post-prune-2026-10-22.md.
    # Additional category evidence: docs/maintenance/discovery-exclusions-2026-10-23.md.
    excluded_categories: frozenset[tuple[str, str]] = frozenset({
        ("baseball", "Cuba"),  # Baseball; all category divisions; Cuba; all genders.
        ("basketball", "Croatia Amateur"),  # Basketball; amateur divisions; Croatia; all genders.
        ("basketball", "Bosnia & Herzegovina"),  # Basketball; amateur divisions; Bosnia & Herzegovina; all genders.
        ("basketball", "Italy Amateur"),  # Basketball; amateur divisions; Italy; all genders.
        ("basketball", "USA Amateur"),  # Basketball; amateur divisions; USA; all genders.
        ("basketball", "Brazil Amateur"),  # Basketball; amateur divisions; Brazil; all
        ("basketball", "Argentina Amateur"),  # Basketball; amateur divisions; Argentina; all genders.
        ("basketball", "Germany Amateur"),  # Basketball; amateur divisions; Germany; all
        ("basketball", "Spain Amateur"),  # Basketball; amateur divisions; Spain; all genders.
        ("basketball", "France Amateur"),  # Basketball; amateur divisions; France; all genders.
        ("basketball", "Poland Amateur"),  # Basketball; amateur divisions; Poland; all genders.
        ("basketball", "Greece Amateur"),  # Basketball; amateur divisions; Greece; all genders.
        ("football", "Argentina Amateur"),  # Football; amateur divisions; Argentina; all genders.
        ("football", "Austria Amateur"),  # Football; amateur divisions; Austria; all genders.
        ("football", "Belgium Amateur"),  # Football; amateur divisions; Belgium; all genders.
        ("football", "Bosnia & Herzegovina Amateur"),  # Football; amateur divisions; Bosnia & Herzegovina; all genders.
        ("football", "Brazil Amateur"),  # Football; amateur divisions; Brazil; all genders.
        ("football", "Bulgaria Amateur"),  # Football; amateur divisions; Bulgaria; all genders.
        ("football", "Canada Amateur"),  # Football; amateur divisions; Canada; all genders.
        ("football", "USA Amateur"),  # Football; amateur divisions; Chile; all genders.
        ("football", "Colombia Amateur"),  # Football; amateur divisions; Colombia; all genders.
        ("football", "Croatia Amateur"),  # Football; amateur divisions; Croatia; all genders.
        ("football", "Cuba"),  # Football; all category divisions; Cuba; all genders.
        # Keep the configured alternative; current database rows use Czechia Amateur.
        ("football", "Czech Republic Amateur"),  # Football; amateur divisions; Czech Republic; all genders.
        ("football", "Czechia Amateur"),  # Football; amateur divisions; Czech Republic; all genders.
        ("football", "Denmark Amateur"),  # Football; amateur divisions; Denmark; all genders.
        ("football", "Ecuador Amateur"),  # Football; amateur divisions; Ecuador; all genders.
        ("football", "England Amateur"),  # Football; amateur divisions; England; all genders.
        ("football", "Scotland Amateur"),  # Football; amateur divisions; Scotland; all genders.
        ("football", "Kuwait Amateur"),  # Football; amateur divisions; Kuwait; all genders.
        ("football", "Finland Amateur"),  # Football; amateur divisions; Finland; all genders.
        ("football", "France Amateur"),  # Football; amateur divisions; France; all genders.
        ("football", "Germany Amateur"),  # Football; amateur divisions; Germany; all genders.
        ("football", "Greece Amateur"),  # Football; amateur divisions; Greece; all genders.
        ("football", "Guadeloupe"),  # Football; all category divisions; Guadeloupe; all genders.
        ("football", "Guinea-Bissau"),  # Football; all category divisions; Guinea-Bissau; all genders.
        ("football", "Haiti"),  # Football; all category divisions; Haiti; all genders.
        ("football", "Hungary Amateur"),  # Football; amateur divisions; Hungary; all genders.
        ("football", "Italy Amateur"),  # Football; amateur divisions; Italy; all genders.
        ("football", "Kenya Amateur"),  # Football; amateur divisions; Kenya; all genders.
        ("football", "Martinique"),  # Football; all category divisions; Martinique; all genders.
        ("football", "Montenegro Amateur"),  # Football; amateur divisions; Montenegro; all genders.
        ("football", "Morocco Amateur"),  # Football; amateur divisions; Morocco; all genders.
        ("football", "Netherlands Amateur"),  # Football; amateur divisions; Netherlands; all genders.
        ("football", "Norway Amateur"),  # Football; amateur divisions; Norway; all genders.
        ("football", "North Macedonia"),  # Football; amateur divisions; North Macedonia; all genders.
        ("football", "Palestine"),  # Football; all category divisions; Palestine; all genders.
        ("football", "Peru Amateur"),  # Football; amateur divisions; Peru; all genders.
        ("football", "Chile Amateur"),  # Football; amateur divisions; Chile; all genders.
        ("football", "Uruguay Amateur"),  # Football; amateur divisions; Uruguay; all genders.
        ("football", "Poland Amateur"),  # Football; amateur divisions; Poland; all genders.
        ("football", "Portugal Amateur"),  # Football; amateur divisions; Portugal; all genders.
        ("football", "Romania Amateur"),  # Football; amateur divisions; Romania; all genders.
        ("football", "Armenia Amateur"), # Football; amateur divisions; Armenia; all genders.
        ("football", "Russia Amateur"),  # Football; amateur divisions; Russia; all genders.
        ("football", "Saint Lucia"),  # Football; all category divisions; Saint Lucia; all genders.
        ("football", "Sao Tome and Principe"),  # Football; all category divisions; Sao Tome and Principe; all genders.
        ("football", "Serbia Amateur"),  # Football; amateur divisions; Serbia; all genders.
        ("football", "Slovakia Amateur"),  # Football; amateur divisions; Slovakia; all genders.
        ("football", "Slovenia Amateur"),  # Football; amateur divisions; Slovenia; all genders.
        ("football", "Somalia"),  # Football; all category divisions; Somalia; all genders.
        ("football", "India Amateur"),  # Football; amateur divisions; India; all genders.
        ("football", "South Sudan"),  # Football; all category divisions; South Sudan; all genders.
        ("football", "Spain Amateur"),  # Football; amateur divisions; Spain; all genders.
        ("football", "Sweden Amateur"),  # Football; amateur divisions; Sweden; all genders.
        ("football", "Switzerland Amateur"),  # Football; amateur divisions; Switzerland; all genders.
        ("football", "Turkey Amateur"),  # Football; amateur divisions; Turkey; all genders.
        ("football", "Ukraine Amateur"),  # Football; amateur divisions; Ukraine; all genders.
        ("football", "Yemen"),  # Football; all category divisions; Yemen; all genders.
        ("handball", "Poland"),  # Handball; all category divisions and cups; Poland; all genders.
        ("handball", "Croatia"),  # Handball; all category divisions and cups; Croatia; all genders.
        ("handball", "Portugal"),  # Handball; all category divisions and cups; Portugal; all genders.
        ("handball", "Romania"),  # Handball; all category divisions and cups; Romania; all genders.
        ("handball", "Romania Amateur"),  # Handball; amateur divisions; Romania; all genders.
        ("handball", "Russia"),  # Handball; all category divisions and cups; Russia; all genders.
        ("handball", "Serbia"),  # Handball; all category divisions and cups; Serbia; all genders.
        ("handball", "Slovenia"),  # Handball; all category divisions and cups; Slovenia; all genders.
        ("handball", "Tunisia"),  # Handball; all category divisions, including youth; Tunisia; all genders.
        ("handball", "Turkey"),  # Handball; all category divisions; Turkey; all genders.
        ("volleyball", "Spain"),  # Volleyball; all category divisions; Spain; all genders.
    })
    exclude_competitions: bool = True
    # SofaScore uniqueTournament IDs, also used for OddsPapi admission.
    # These are not canonical competition IDs or OddsPapi tournament IDs.
    # Gender labels describe stored evidence; IDs are matched without a gender filter.
    # Countries follow SofaScore's competition category labels.
    # Additional scopes audited 2026-10-23: docs/maintenance/discovery-exclusions-2026-10-23.md.
    excluded_sofascore_unique_tournament_ids: frozenset[int] = frozenset({
        19510,  # American football; NCAA (division unspecified); USA; men.
        32199,  # American football; NCAA Division FBS; USA; men.
        32200,  # American football; NCAA Division FCS; USA; men.
        32201,  # American football; NCAA Interdivisional; USA; men.
        11394,  # Baseball; Serie Nacional; Cuba; men.
        19488,  # Baseball; Liga Mexicana del Pacifico; Mexico; men.
        11616,  # Baseball; Dominican Republic LIDOM; Dominican Republic; men.
        11395,  # Baseball; Venezuela LVBP; Venezuela; men.
        19446,  # Baseball; Appalachian League; USA; men.
        19464,  # Baseball; American Association; USA; men.
        19465,  # Baseball; Frontier League; USA; men.
        19466,  # Baseball; Atlantic League; USA; men.
        19467,  # Baseball; Northwoods League; USA; men.
        19468,  # Baseball; Dominican Summer League; Dominican Republic; men.
        19477,  # Baseball; Double-A Texas League; USA; men.
        19478,  # Baseball; Double-A Eastern League; USA; men.
        19479,  # Baseball; Double-A Southern League; USA; men.
        19480,  # Baseball; High-A Midwest League; USA; men.
        19481,  # Baseball; High-A South Atlantic League; USA; men.
        19482,  # Baseball; High-A Northwest League; USA; men.
        19483,  # Baseball; Single-A California League; USA; men.
        19484,  # Baseball; Single-A Florida State League; USA; men.
        19485,  # Baseball; Single-A Carolina League; USA; men.
        20468,  # Baseball; Pioneer League; USA; men.
        23300,  # Basketball; Nationale 3; France; men.
        27700,  # Basketball; Serie B Interregionale; Italy; men.
        28704,  # Basketball; CBI U15; Brazil; boys.
        30519,  # Basketball; U18 Paulista; Brazil; men.
        37825,  # Basketball; NCAA; Philippines; men.
        1095,   # Football; Second League, Division B, Group 3; Russia; men.
        2288,   # Football; 2. Frauen-Bundesliga; Germany; women.
        15946,  # Football; NCAA (division unspecified); USA; men.
        18944,  # Football; NCAA regular season (division unspecified); USA; women.
        20373,  # Football; Klasa Okregowa, Zachodniopomorska; Poland; men.
        20426,  # Football; II Liiga; Estonia; men.
        20706,  # Football; Second League, Division A; Russia; men.
        21261,  # Football; III Liiga; Estonia; men.
        21969,  # Football; Liga de Paysandú; Uruguay; men.
        22105,  # Football; Tercera División B; Chile; men.
        22139,  # Football; Vtoraya Liga; Belarus; men.
        22424,  # Football; San Francisco Soccer Football League; USA; men.
        22694,  # Football; U17 Paulista; Brazil; boys.
        22752,  # Football; U15 Paulista; Brazil; boys.
        22794,  # Football; U17 Campeonato Brasileiro; Brazil; men.
        22800,  # Football; Northern Counties East League; England; men.
        22858,  # Football; NLO Division Two; Nigeria; men / unclassified.
        23095,  # Football; 3. Liga FVRZ; Switzerland; men / unclassified.
        23109,  # Football; 4. Liga FVRZ; Switzerland; men / unclassified.
        23461,  # Football; 3. MFL; North Macedonia; men / unclassified.
        24172,  # Football; Ardal Leagues; Wales; men.
        24208,  # Football; Liga de Tineret; Romania; men.
        24209,  # Football; First League, Women; Russia; women.
        24254,  # Football; Campionatul National U19; Romania; boys.
        25272,  # Football; U20 M-League, Division B; Russia; men.
        25633,  # Football; Altero.lv LIIGA; Latvia; men.
        26023,  # Football; Dali Dali 3.Līga; Latvia; men.
        26682,  # Football; Primera A Cochabamba; Bolivia; men.
        26929,  # Football; Primera A Santa Cruz; Bolivia; men.
        26947,  # Football; Primera C; Colombia; men.
        27113,  # Football; club friendly games; Bulgaria; gender unspecified.
        27115,  # Football; club friendly games; Netherlands; gender unspecified.
        27118,  # Football; club friendly games; Germany; gender unspecified.
        27334,  # Football; Primera B Santa Cruz; Bolivia; men.
        27384,  # Football; Primera B Cochabamba; Bolivia; men.
        28530,  # Football; U-Sports; Canada; men.
        28541,  # Football; NCAA Division II; USA; men.
        28543,  # Football; NCAA Division II; USA; women.
        28932,  # Football; club friendly games; Turkey; gender unspecified.
        30424,  # Football; U17 League; Latvia; men.
        30706,  # Football; U17 Allievi Nazionali; Italy; boys.
        31773,  # Football; Ligue Inter Regions; Algeria; men.
        31970,  # Football; NSW U23 NPL, Women; Australia; women.
        32146,  # Football; NPL U20 Victoria, Women; Australia; women.
        32193,  # Football; 1. Division; Luxembourg; men.
        33726,  # Football; U17 Gelisim Ligi; Turkey; boys.
        33800,  # Football; U16 Gelisim Ligi; Turkey; boys.
        33808,  # Football; U15 Gelisim Ligi; Turkey; boys.
        33809,  # Football; U14 Gelisim Ligi; Turkey; boys.
        33895,  # Football; Kazakhstan 2nd Division; Kazakhstan; men.
        34185,  # Football; FaCup La Liga Malloco; Chile; men / unclassified.
        34268,  # Football; U15 League; Latvia; men.
        35333,  # Football; Campionatul Național U17; Romania; men.
        35742,  # Football; Bezirksliga Hamburg; Germany; men.
        35754,  # Football; CCAA Championship; Canada; men.
        35865,  # Football; Klasa B, Zachodniopomorska; Poland; men / unclassified.
        35960,  # Football; club friendly games; England; gender unspecified.
        36031,  # Football; NCAA Division III; USA; men.
        36033,  # Football; NCAA Division III; USA; women.
        36575,  # Football; U15 Liga Elitelor; Romania; boys.
        36587,  # Football; Liga Elitelor U17; Romania; men.
        36637,  # Football; Liga Elitelor U16; Romania; boys.
        37028,  # Football; U15 Kizlar Gelisim Ligi; Turkey; girls.
        37258,  # Football; Coupe de France regional phase; France; women.
        37514,  # Football; Campionato Regionale Italiana Serie 2; Italy; gender unconfirmed.
        921,    # Handball; 2. HBL; Germany; men.
        1416,   # Handball; 1. Division; Denmark; gender unconfirmed.
        1420,   # Handball; 1. Divisjon; Norway; men.
        9409,   # Handball; club friendly games; international; gender unspecified.
        13506,  # Handball; Division de Honor Plata; Spain; gender unconfirmed.
        34532,  # Handball; 1. HRL U11; Croatia; boys.
        34535,  # Handball; 1. HRL U13; Croatia; boys.
        34536,  # Handball; 1. HRL U14 Zene; Croatia; girls.
        34537,  # Handball; 1. HRL U15; Croatia; boys.
        35445,  # Handball; FeMeBal Liga de Honor Plata; Argentina; men.
        14285,  # Ice hockey; NCAA (division unspecified); USA; men.
        14317,  # Ice hockey; NCAA (division unspecified); USA; women.
        24549,  # Ice hockey; U20 Nationell; Sweden; men (U20).
        28753,  # Ice hockey; U21 Elit; Switzerland; men.
        28754,  # Ice hockey; U20 Extraliga Junioru; Czech Republic; men (U20).
        23428,  # Volleyball; NCAA regular season (division unspecified); USA; women.
        30109,  # Volleyball; Kadinlar 2. Ligi; Turkey; women.
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
