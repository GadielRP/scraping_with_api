"""Backfill SofaScore historical market choice names and choice groups."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import unicodedata
from difflib import SequenceMatcher
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import inspect, insert, text

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from infrastructure.persistence.database import db_manager  # noqa: E402
from infrastructure.persistence.models import Market  # noqa: E402
from app.logging_setup import setup_logging  # noqa: E402

logger = logging.getLogger(__name__)

CANONICAL_CHOICE_NAME_SEQUENCE = ("1", "2", "1x", "12", "x2", "x", "over", "under", "yes", "no", "no_goal")
CANONICAL_CHOICE_NAMES = frozenset(CANONICAL_CHOICE_NAME_SEQUENCE)
ALLOWED_TARGET_CHOICE_NAMES = CANONICAL_CHOICE_NAMES
DEFAULT_BOOKIE_ID = 1
DEFAULT_OUTPUT_ROOT = Path("debug") / "choice_renaming"
_CANONICAL_CHOICE_SQL_VALUES = ", ".join(f"'{choice}'" for choice in CANONICAL_CHOICE_NAME_SEQUENCE)


def _noncanonical_choice_sql(alias: str) -> str:
    return f"{alias}.choice_name IS NOT NULL AND {alias}.choice_name != '' AND {alias}.choice_name NOT IN ({_CANONICAL_CHOICE_SQL_VALUES})"


NONCANONICAL_CHOICE_SQL = _noncanonical_choice_sql("c2")

# Extend this map as new SofaScore team aliases are discovered.
TEAM_NAME_ALIASES: dict[str, tuple[str, ...]] = {
    "liverpool fc": ("liverpool", "liverpool fc"),
    "leed united": ("leeds", "leeds utd"),
    "athletic club": ("athletic bilbao", "athletic club"),
    "fc barcelona": ("barcelona", "fc barcelona", "barcelona fc", "barca"),
    "atletico madrileño" : ("atletico madrileño", "atletico madrid b", "atlético madrid b"),
    "leeds united": ("leeds", "leeds united", "leeds utd"),
    "manchester united": ("man utd", "man united", "manchester united"),
    "manchester city": ("man city", "manchester city"),
    "brighton & hove albion": ("brighton", "brighton & hove albion"),
    "west ham united": ("west ham", "west ham united", "west ham fc"),
    "real betis": ("real betis balompie", "real betis balompié", "real betis"),
    "deportivo alaves": ("cd alaves", "deportivo alaves", "deportivo alavés"),
    "barcelona sc guayaquil": ("barcelona sc", "barcelona sc guayaquil"),
    "club atletico platense": ("platense", "club atletico platense", "ca platense"),
    "club atletico belgrano": ("belgrano", "club atletico belgrano", "ca belgrano"),
    "scr altach": ("sc rheindorf altach", "scr altach", "rheindorf altach"),
    "mjondalen if": ("mjondalen", "mjondalen if"),
    "ca talleres": ("talleres", "ca talleres"),
    "ca lanus": ("lanus", "ca lanus"),
    "clube de regatas brasil": ("crb", "clube de regatas brasil"),
    "kuopion palloseura": ("kups", "kuopion palloseura"),
    "aguilas-umak fc": ("davao aguilas", "aguilas-umak fc", "aguilas umak fc"),
    "portimonense sporting clube": ("portimonense sc", "portimonense sporting clube", "portimonense"),
    "miami (oh) redhawks": ("miami ohio", "miami oh", "miami (oh) redhawks", "miami redhawks"),
    "ucf knights": ("central florida knights", "ucf knights", "central florida"),
    "volta redonda": ("volta redonda futebol clube", "volta redonda", "volta redonda fc"),
    "sao jose dos campos": ("sao jose basketball def sp", "sao jose dos campos", "sao jose"),
    "fc jurong": ("albirex niigata singapore", "fc albirex jurong", "fc jurong", "albirex jurong"),
    "kosner baskonia": ("saski baskonia", "kosner baskonia", "baskonia"),
    "elitzur netanya": ("barak netanya", "elitzur netanya", "netanya"),
    "galatasaray mct technic": ("galatasaray", "galatasaray mct technic", "galatasaray mct"),
    "george washington revolutionaries": (
        "george washington colonials",
        "george washington revolutionaries",
        "gw colonials",
        "gw revolutionaries",
    ),
    "club sport sebaco": ("club sport sebaco", "cs sebaco", "sebaco", "sébaco"),
    "istres fc": ("istres fc", "istres football club"),
    "asptt caen football": ("asptt caen", "asptt caen football"),
    "le puy foot 43 auvergne": ("le puy-en-velay", "le puy foot 43 auvergne"),
    "londrina esporte clube": ("londrina", "londrina esporte clube"),
    "azuriz futebol clube": ("azuriz", "azuriz futebol clube"),
    "mersin sports club": ("mersin sc", "mersin sports club"),
    "sao jose basketball def sp": ("sao jose basketball def sp", "sao jose dos campos"),
    "henan fc jiuzu dukang": ("henan fc", "henan fc jiuzu dukang", "henan jiuzu dukang"),
    "turun palloseura": ("tps", "turun palloseura"),
    "rytas vilnius": ("vilniaus rytas", "rytas vilnius", "vilnius rytas"),
    "unicaja": ("unicaja malaga", "unicaja málaga", "unicaja"),
    "dreamland gran canaria": ("gran canaria", "dreamland gran canaria"),
    "sport club corinthians paulista": ("corinthians", "corinthians paulista", "sport club corinthians paulista"),
    "bursaspor basketbol": ("bursaspor info yatirim", "bursaspor info yatırım", "bursaspor basketbol"),
    "mersin sc": ("mersin buyuksehir belediyesi", "mersin büyükşehir belediyesi", "mersin sc"),
    "mjøndalen if": ("mjondalen", "mjøndalen", "mjøndalen if"),
    "al ahly sc": ("al ahly", "al ahly sc", "al ahly cairo"),
    "al fateh sc": ("al fateh", "al fateh sc"),
    "al hilal saudi fc": ("al hilal", "al hilal saudi fc", "al hilal sfc"),
    "al ittihad club": ("al ittihad", "al ittihad club", "al ittihad jeddah"),
    "al shabab fc": ("al shabab", "al shabab fc", "al shabab riyadh"),
    "al nasr riyadh": ("al nasr", "al nasr riyadh", "al nasr rcd"),
    "al raed fc": ("al raed", "al raed fc"),
    "al taawoun fc": ("al taawoun", "al taawoun fc", "al taawoun fc buraidah"),
    "damac fc": ("damac", "damac fc"),
    "abHA": ("abHA", "abha fc", "abha club", "abха"),
    "al khaleej": ("al khaleej", "al khaleej club"),
    "al feyha": ("al feyha", "al feyha fc"),
    "ensh": ("ensh", "ensh fc", "ensh club", "ennaba"),
    "usb": ("usb", "usb setif", "union sportive", "union sportive setif"),
    "jwan": ("jwan", "jwan fc", "jwan club", "el jadida"),
    "wac": ("wac", "wac casablanca", "wydad", "wydad casablanca"),
    "raja club athletic": ("raja", "raja casablanca", "raja club athletic"),
    "husa": ("husa", "husa agadir", "hassania agadir", "husa agadir"),
    "rc de strasbourg alsace": ("strasbourg", "rc strasbourg", "rc de strasbourg alsace"),
    "racing strasbourg": ("strasbourg", "rc strasbourg", "racing strasbourg"),
    "strasbourg alsace": ("strasbourg", "rc strasbourg", "strasbourg alsace"),
    "olympic lyonnais": ("lyon", "olympic lyonnais", "ol lyon", "ol"),
    "olympique lyonnais": ("lyon", "olympique lyonnais", "ol", "ol lyon"),
    "tottenham hotspur": ("tottenham", "tottenham hotspur"),
    "FC Jurong": ("albirex niigata singapore", "fc albirex jurong", "fc jurong", "albirex jurong"),
    "CDT Real Oruro": ("CD Totora Real Oruro",),
    "leicester city": ("leicester",),
    "ac milan": ("milan", "ac milan"),
    "ssc napoli": ("napoli", "ssc napoli"),
    "as roma": ("roma", "as roma"),
    "fc viktoria plzen": ("viktoria plzen", "fc viktoria plzen"),
    "bsc young boys": ("young boys", "bsc young boys"),
    "fk austria wien": ("austria wien", "fk austria wien"),
    "malaga cf": ("malaga", "malaga cf"),
    "deportivo de la coruna": ("deportivo la coruna", "deportivo de la coruna"),
    "villarreal b u23": ("villarreal cf b u23", "villarreal b u23"),
    "cs maritimo": ("maritimo", "cs maritimo"),
    "sporting cp": ("sporting", "sporting cp"),
    "sl benfica b u21": ("benfica b u21", "sl benfica b u21"),
    "mallorca": ("rcd mallorca", "mallorca"),
    "cabo verde": ("cape verde", "cabo verde"),
    "gremio novorizontino": ("novorizontino", "gremio novorizontino", "grêmio novorizontino"),
    "jong psv eindhoven": ("jong psv", "jong psv eindhoven", "jong psv eindhoven u21"),
    "real sociedad b u21": ("real sociedad b", "real sociedad b u21"),
    "sd tarazona": ("sd tarazona", "sociedad deportivo tarazona"),
    "fortaleza fc": ("fortaleza", "fortaleza fc", "fortaleza ceif"),
    "ael novibet": ("ae larisa", "ael novibet", "ael"),
    "universitario de deportes": ("universitario", "universitario de deportes"),
    "denizli idman yurdu 1959 sk": ("denizli idman yurdu", "denizli idman yurdu 1959 sk"),
    "eskisehir anadolu spor faaliyetleri": ("anadolu universitesi", "eskisehir anadolu spor faaliyetleri"),
    "universitario sfxch": ("cd universitario sfxch", "universitario sfxch"),
    "fancesa": ("ad fancesa", "fancesa"),
    "mus spor kulubu": ("mus 1984 musspor", "mus spor kulubu"),
    "us torcy": ("torcy", "us torcy"),
    "vendee fontenay foot": ("fontenay", "vendee fontenay foot"),
    "il bjarg": ("bjarg", "il bjarg"),
    "kahta 02": ("kahta 02 sk", "kahta 02"),
    "young africans sc": ("young africans sport club", "young africans sc", "yanga"),
    "club sporting cristal": ("cs cristal", "club sporting cristal", "sporting cristal"),
    "esporte clube de patos": ("esporte de patos", "esporte clube de patos"),
    "valentine phoenix fc": ("valentine fc", "valentine phoenix fc"),
    "carmelita": ("ad carmelita", "carmelita"),
    "cd cobreloa": ("cobreloa", "cd cobreloa"),
    "tennessee tempo fc": ("beaman united fc", "tennessee tempo fc"),
    "ca independiente unificada": ("f.p. club independiente unificada", "f.p. club independente unificada", "ca independiente unificada", "ca independente unificada", "independiente unificada"),
    "panevėžio lietkabelis": ("panevėžio lietkabelis", "panevežio 7bet-lietkabelis", "lietkabelis", "7bet-lietkabelis"),
    "bnei penlink herzliya": ("bnei herzeliya", "bnei herzeliya", "bnei penlink herzliya"),
    "fitness first wurzburg baskets": ("fitone wurzburg baskets", "würzburg baskets", "fitness first würzburg baskets"),
    "promitheas patras bc vikos cola": ("promitheas patras", "promitheas patras bc vikos cola"),
    "igokea m:tel": ("igokea", "igokea m:tel", "kk igokea aleksandrovac mtl", "igokea aleksandrovac"),
    "kk spartak office shoes": ("kk spartak office shoes subotica", "kk spartak office shoes", "spartak office shoes"),
    "filou oostende": ("filou oostende", "filou bc oostende", "bc oostende"),
    "bc sabah": ("bc sabah", "sabah bc"),
    "era nymburk": ("era nymburk", "cez basketball nymburk", "basketball nymburk", "cez nymburk"),
    "toulouse bc": ("toulouse bc", "stade toulousain basketball", "stade toulousain"),
    "joventut badalona": ("joventut badalona", "club joventut de badalona", "joventut"),
    "c.d. femarguin spar gran canaria": ("spar gran canaria", "femarguin spar gran canaria"),
    "hapoel netanel holon": ("hapoel unet holon", "hapoel holon", "hapoel netanel holon"),
    "hapoel haemek": ("galil gilboa", "hapoel gilboa galil", "gilboa galil", "hapoel haemek"),
    "utsunomiya brex": ("utstunomiya brex", "utsunomiya brex"),
    "vasco da gama": ("r10 score vasco da gama", "vasco da gama", "cr vasco da gama"),
    "vikos falcons bc": ("vikos bc", "vikos falcons bc"),
    "karditsa iaponiki": ("as karditsas", "karditsa iaponiki"),
    "a.lobos bkb": ("bc chalchuapa united", "a.lobos bkb"),
    "age chalkida": ("gs ermis sximatariou", "age chalkida"),
    "norwich city": ("norwich", "norwich city"),
    "newcastle united": ("newcastle", "newcastle united"),
}


@contextmanager
def progress_step(label: str):
    """Print a readable step marker around indivisible work."""
    print(f"[START] {label}", flush=True)
    try:
        yield
    except BaseException:
        print(f"[FAIL ] {label}", flush=True)
        raise
    else:
        print(f"[ DONE] {label}", flush=True)


def _log_progress(label: str, current: int, total: int | None = None, every: int = 1000) -> None:
    every = max(1, every)
    if current <= 0:
        return
    should_log = current == 1 or current % every == 0
    if total is not None and current == total:
        should_log = True
    if not should_log:
        return
    if total is None:
        logger.info("%s: %d", label, current)
        return
    percent = (current / total) * 100 if total else 100.0
    logger.info("%s: %d/%d (%.1f%%)", label, current, total, percent)


@dataclass(frozen=True)
class TeamResolution:
    side: str | None
    strategy: str
    confidence: float
    matched_value: str | None
    reason: str


def _build_team_alias_lookup() -> dict[str, frozenset[str]]:
    lookup: dict[str, set[str]] = defaultdict(set)
    for canonical_name, aliases in TEAM_NAME_ALIASES.items():
        canonical_norm = normalize_team_text(canonical_name)
        if canonical_norm:
            lookup[canonical_norm].add(canonical_name)
            lookup[compact_team_text(canonical_norm)].add(canonical_name)
        for alias in aliases:
            alias_norm = normalize_team_text(alias)
            if not alias_norm:
                continue
            lookup[alias_norm].add(canonical_name)
            lookup[compact_team_text(alias_norm)].add(canonical_name)
    return {key: frozenset(values) for key, values in lookup.items() if key}



def _aliases_for_canonical_name(canonical_name: str) -> tuple[str, ...]:
    normalized = normalize_team_text(canonical_name)
    if not normalized:
        return ()
    return TEAM_NAME_ALIASES.get(normalized, TEAM_NAME_ALIASES.get(canonical_name, ()))


def _team_alias_candidates(team_name: Any) -> frozenset[str]:
    team_norm = normalize_team_text(team_name)
    if not team_norm:
        return frozenset()
    candidates: set[str] = {team_norm, compact_team_text(team_norm)}
    for key in (team_norm, compact_team_text(team_norm)):
        canonical_names = TEAM_ALIAS_LOOKUP.get(key, frozenset())
        for canonical_name in canonical_names:
            candidates.add(canonical_name)
            candidates.add(compact_team_text(canonical_name))
            for alias in _aliases_for_canonical_name(canonical_name):
                alias_norm = normalize_team_text(alias)
                if alias_norm:
                    candidates.add(alias_norm)
                    candidates.add(compact_team_text(alias_norm))
    return frozenset(candidate for candidate in candidates if candidate)


def _score_team_match(label: str, candidate: str) -> float:
    label_norm = normalize_team_text(label)
    candidate_norm = normalize_team_text(candidate)
    if not label_norm or not candidate_norm:
        return 0.0
    label_compact = compact_team_text(label_norm)
    candidate_compact = compact_team_text(candidate_norm)
    if not label_compact or not candidate_compact:
        return 0.0
    return max(
        SequenceMatcher(None, label_norm, candidate_norm).ratio(),
        SequenceMatcher(None, label_compact, candidate_compact).ratio(),
    )


def slug_to_title_case(slug: str | None) -> str:
    if not slug:
        return ""
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", slug)
    spaced = re.sub(r"[\-_]+", " ", spaced)
    spaced = re.sub(r"\s+", " ", spaced).strip()
    return spaced.title()


def _team_resolution_can_persist(resolution: TeamResolution) -> bool:
    return resolution.side is not None and resolution.strategy in {"exact", "alias", "slug"}


def resolve_team_side(
    label: Any,
    home_team: Any,
    away_team: Any,
    home_slug: str | None = None,
    away_slug: str | None = None,
) -> TeamResolution:
    raw_label = "" if label is None else str(label).strip()
    label_norm = normalize_team_text(raw_label)
    label_compact = compact_team_text(label_norm)
    home_norm = normalize_team_text(home_team)
    away_norm = normalize_team_text(away_team)
    if not raw_label:
        return TeamResolution(None, "blank", 0.0, None, "blank team label")
    if not home_norm or not away_norm:
        return TeamResolution(None, "missing_teams", 0.0, None, "missing home or away team")

    # Try matching against slug first
    home_slug_match = False
    away_slug_match = False
    home_slug_title = None
    away_slug_title = None

    if home_slug:
        home_slug_title = slug_to_title_case(home_slug)
        home_slug_norm = normalize_team_text(home_slug_title)
        home_slug_compact = compact_team_text(home_slug_norm)
        home_slug_match = label_norm == home_slug_norm or label_compact == home_slug_compact
    if away_slug:
        away_slug_title = slug_to_title_case(away_slug)
        away_slug_norm = normalize_team_text(away_slug_title)
        away_slug_compact = compact_team_text(away_slug_norm)
        away_slug_match = label_norm == away_slug_norm or label_compact == away_slug_compact

    if home_slug_match and away_slug_match and home_slug_norm != away_slug_norm:
        return TeamResolution(None, "ambiguous_slug", 0.95, raw_label, "label matches both home and away slugs")
    if home_slug_match:
        return TeamResolution("1", "slug", 0.99, home_slug_title, f"matched home team slug {home_slug!r}")
    if away_slug_match:
        return TeamResolution("2", "slug", 0.99, away_slug_title, f"matched away team slug {away_slug!r}")

    if label_norm == home_norm or label_compact == compact_team_text(home_norm):
        return TeamResolution("1", "exact", 1.0, str(home_team), "exact home team match")
    if label_norm == away_norm or label_compact == compact_team_text(away_norm):
        return TeamResolution("2", "exact", 1.0, str(away_team), "exact away team match")

    home_aliases = _team_alias_candidates(home_team)
    away_aliases = _team_alias_candidates(away_team)
    home_alias_match = label_norm in home_aliases or label_compact in home_aliases
    away_alias_match = label_norm in away_aliases or label_compact in away_aliases
    if home_alias_match and away_alias_match and home_norm != away_norm:
        return TeamResolution(None, "ambiguous_alias", 0.95, raw_label, "label matches both home and away aliases")
    if home_alias_match:
        matched = next((alias for alias in home_aliases if label_norm == alias or label_compact == alias), str(home_team))
        return TeamResolution("1", "alias", 0.99, matched, f"matched home team alias {matched!r}")
    if away_alias_match:
        matched = next((alias for alias in away_aliases if label_norm == alias or label_compact == alias), str(away_team))
        return TeamResolution("2", "alias", 0.99, matched, f"matched away team alias {matched!r}")

    def _best_candidate(candidates: frozenset[str]) -> tuple[float, str | None]:
        best_score = 0.0
        best_candidate: str | None = None
        for candidate in candidates:
            score = _score_team_match(raw_label, candidate)
            if score > best_score:
                best_score = score
                best_candidate = candidate
        return best_score, best_candidate

    home_score, home_candidate = _best_candidate(home_aliases)
    away_score, away_candidate = _best_candidate(away_aliases)
    if home_candidate is None and away_candidate is None:
        return TeamResolution(None, "unresolved", 0.0, None, "no candidate team names available")

    best_score = max(home_score, away_score)
    score_gap = abs(home_score - away_score)
    if best_score >= 0.88 and score_gap >= 0.08:
        if home_score > away_score:
            return TeamResolution("1", "fuzzy", round(home_score, 3), home_candidate, f"fuzzy match against {home_candidate!r}")
        if away_score > home_score:
            return TeamResolution("2", "fuzzy", round(away_score, 3), away_candidate, f"fuzzy match against {away_candidate!r}")
        return TeamResolution(None, "ambiguous_fuzzy", round(best_score, 3), home_candidate or away_candidate, "fuzzy scores tied")

    if best_score >= 0.72 and score_gap >= 0.08:
        if home_score > away_score:
            return TeamResolution(
                "1",
                "fuzzy",
                round(home_score, 3),
                home_candidate,
                f"fuzzy match against {home_candidate!r}",
            )
        if away_score > home_score:
            return TeamResolution(
                "2",
                "fuzzy",
                round(away_score, 3),
                away_candidate,
                f"fuzzy match against {away_candidate!r}",
            )
        return TeamResolution(
            None,
            "ambiguous_fuzzy",
            round(best_score, 3),
            home_candidate or away_candidate,
            "fuzzy scores tied",
        )

    if best_score >= 0.72:
        return TeamResolution(
            None,
            "ambiguous_fuzzy",
            round(best_score, 3),
            home_candidate or away_candidate,
            f"ambiguous fuzzy match against {home_candidate or away_candidate!r}",
        )
    return TeamResolution(
        None,
        "unresolved",
        round(best_score, 3),
        home_candidate or away_candidate,
        f"best match too weak against {home_candidate or away_candidate!r}",
    )


def _expand_in_clause(sql: str, param_name: str, values: Sequence[Any], prefix: str) -> tuple[str, dict[str, Any]]:
    items = list(values)
    if not items:
        return sql, {}
    placeholders = ", ".join(f":{prefix}_{idx}" for idx in range(len(items)))
    expanded_sql = sql.replace(f":{param_name}", f"({placeholders})")
    expanded_params = {f"{prefix}_{idx}": value for idx, value in enumerate(items)}
    return expanded_sql, expanded_params


def _chunked(values: Sequence[Any], chunk_size: int) -> Iterable[list[Any]]:
    chunk_size = max(1, chunk_size)
    items = list(values)
    for index in range(0, len(items), chunk_size):
        yield items[index : index + chunk_size]


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized.strip())
    return normalized.lower()


TEAM_NAME_MARKERS = {"fc", "cf", "ud", "ca", "if", "sc", "afc"}


def normalize_team_text(value: Any) -> str:
    normalized = normalize_text(value)
    if not normalized:
        return ""
    tokens = [token for token in normalized.split() if token not in TEAM_NAME_MARKERS]
    if not tokens:
        return normalized
    return " ".join(tokens)


def compact_team_text(value: Any) -> str:
    return re.sub(r"[\s\-\_.,()]", "", normalize_team_text(value))


def compact_text(value: Any) -> str:
    return re.sub(r"[\s\-\_.,()]", "", normalize_text(value))


def normalize_choice_name_to_canonical(value: Any) -> str | None:
    normalized = normalize_text(value)
    compact = compact_text(value)

    legacy_map = {
        "1": "1",
        "2": "2",
        "1x": "1x",
        "12": "12",
        "x": "x",
        "draw": "x",
        "tie": "x",
        "over": "over",
        "under": "under",
        "yes": "yes",
        "no": "no",
        "no_goal": "no_goal",
    }

    if normalized in legacy_map:
        return legacy_map[normalized]

    no_goal_normalized = {
        "no goal",
        "no goals",
        "no scorer",
        "no goalscorer",
        "no goal scorer",
        "no goalscorer",
        "none",
    }
    no_goal_compact = {
        "nogoal",
        "nogoals",
        "noscorer",
        "nogoalscorer",
        "nogoalscorer",
        "none",
    }

    if normalized in no_goal_normalized or compact in no_goal_compact:
        return "no_goal"

    return None


TEAM_ALIAS_LOOKUP = _build_team_alias_lookup()


def format_line(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        decimal = Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if decimal == 0:
        return "0"
    formatted = format(decimal.normalize(), "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    if formatted.startswith("+"):
        formatted = formatted[1:]
    if formatted in {"-0", "-0.0", "+0"}:
        return "0"
    return formatted


def parse_parenthesized_line_choice(choice_name: Any) -> dict[str, str] | None:
    if choice_name in (None, ""):
        return None
    match = re.match(r"^\(\s*([+-]?\d+(?:\.\d+)?)\s*\)\s*(.+?)\s*$", str(choice_name).strip())
    if not match:
        return None
    line = format_line(match.group(1))
    if line is None:
        return None
    return {"line": line, "label_after_line": match.group(2).strip()}


def resolve_side_from_label(label: Any, home_team: Any, away_team: Any) -> str | None:
    return resolve_team_side(label, home_team, away_team).side


@dataclass(frozen=True)
class MarketShapeDefinition:
    kind: str
    source_shapes: frozenset[tuple[str, str, str]]
    canonical_target_key: str
    canonical_target_name: str
    canonical_target_group: str
    canonical_target_period: str
    market_family: str
    line_based: bool = False
    allows_draw: bool = False


@dataclass(frozen=True)
class MarketContext:
    market_id: int
    event_id: int
    bookie_id: int
    market_name: str
    market_group: str | None
    market_period: str
    choice_group: str | None
    is_live: bool
    sport: str | None
    home_team: str | None
    away_team: str | None
    source_shape: tuple[str, str, str]
    market_kind: str
    canonical_target_key: str
    canonical_target_name: str
    canonical_target_group: str
    canonical_target_period: str
    line_based: bool
    allows_draw: bool
    source_collected_at: datetime | None = None
    home_slug: str | None = None
    away_slug: str | None = None
    start_time_utc: datetime | None = None


@dataclass(frozen=True)
class ChoiceContext:
    choice_id: int
    market_id: int
    choice_name: str
    initial_odds: Any
    current_odds: Any
    change: Any


@dataclass(frozen=True)
class ChoiceResolution:
    choice_id: int
    market_id: int
    source_choice_name: str
    home_team: str | None
    away_team: str | None
    team_resolution_strategy: str | None
    team_resolution_confidence: float | None
    team_resolution_match: str | None
    target_choice_name: str | None
    target_choice_group: str | None
    parsed_line: str | None
    action_type: str
    status: str
    reason: str
    target_market_key: str | None
    target_market_name: str | None
    target_market_group: str | None
    target_market_period: str | None
    target_market_choice_group: str | None
    target_market_is_exact_source: bool
    event_id: int | None = None
    start_time_utc: datetime | None = None


@dataclass(frozen=True)
class TargetMarketSpec:
    event_id: int
    bookie_id: int
    is_live: bool
    market_name: str
    market_group: str | None
    market_period: str
    choice_group: str | None
    target_market_key: str
    source_market_ids: tuple[int, ...] = ()
    source_choice_ids: tuple[int, ...] = ()
    existing_market_id: int | None = None


@dataclass(frozen=True)
class MergeAction:
    target_market_key: str
    target_choice_name: str
    survivor_choice_id: int
    duplicate_choice_id: int
    source_market_id: int
    source_choice_name: str
    target_market_id: int | None = None


MARKET_SHAPE_DEFINITIONS: tuple[MarketShapeDefinition, ...] = (
    MarketShapeDefinition(
        kind="moneyline_full_time_2way",
        source_shapes=frozenset(
            {
                ("full time", "home/away", "match"),
                ("full time", "home/away", "full-time"),
                ("full time", "home/away", "full time"),
                ("home/away full time", "home/away", "full time"),
            }
        ),
        canonical_target_key="home_away_full_time",
        canonical_target_name="Home/Away Full Time",
        canonical_target_group="Home/Away",
        canonical_target_period="Full Time",
        market_family="side",
    ),
    MarketShapeDefinition(
        kind="moneyline_1st_half_2way",
        source_shapes=frozenset(
            {
                ("1st half", "home/away", "1st half"),
                ("1st half", "home/away", "1st Half"),
                ("home/away 1st half", "home/away", "1st half"),
            }
        ),
        canonical_target_key="home_away_1st_half",
        canonical_target_name="Home/Away 1st Half",
        canonical_target_group="Home/Away",
        canonical_target_period="1st Half",
        market_family="side",
    ),
    MarketShapeDefinition(
        kind="1x2_full_time",
        source_shapes=frozenset(
            {
                ("full time", "1x2", "full-time"),
                ("full-time", "1x2", "full-time"),
                ("full-time", "1x2", "full time"),
                ("1x2 full time", "1x2", "full time"),
            }
        ),
        canonical_target_key="1x2_full_time",
        canonical_target_name="1X2 Full Time",
        canonical_target_group="1X2",
        canonical_target_period="Full Time",
        market_family="side",
        allows_draw=True,
    ),
    MarketShapeDefinition(
        kind="1x2_1st_half",
        source_shapes=frozenset(
            {
                ("1st half", "1x2", "1st half"),
                ("1st half", "1x2", "1st Half"),
                ("1x2 1st half", "1x2", "1st half"),
            }
        ),
        canonical_target_key="1x2_1st_half",
        canonical_target_name="1X2 1st Half",
        canonical_target_group="1X2",
        canonical_target_period="1st Half",
        market_family="side",
        allows_draw=True,
    ),
    MarketShapeDefinition(
        kind="draw_no_bet_full_time",
        source_shapes=frozenset(
            {
                ("draw no bet", "draw no bet", "full-time"),
                ("draw no bet full time", "draw no bet", "full time"),
            }
        ),
        canonical_target_key="draw_no_bet_full_time",
        canonical_target_name="Draw No Bet Full Time",
        canonical_target_group="Draw No Bet",
        canonical_target_period="Full Time",
        market_family="draw_no_bet",
    ),
    MarketShapeDefinition(
        kind="total_full_time",
        source_shapes=frozenset(
            {
                ("total", "over/under", "full-time"),
                ("game total", "over/under", "match"),
                ("total points", "over/under", "match"),
                ("match goals", "match goals", "match"),
                ("match goals", "match goals", "full-time"),
                ("over/under full time", "over/under", "full time"),
            }
        ),
        canonical_target_key="over_under_full_time",
        canonical_target_name="Over/Under Full Time",
        canonical_target_group="Over/Under",
        canonical_target_period="Full Time",
        market_family="total",
        line_based=True,
    ),
    MarketShapeDefinition(
        kind="asian_handicap_full_time",
        source_shapes=frozenset(
            {
                ("point spread", "point spread", "match"),
                ("asian handicap", "asian handicap", "full-time"),
                ("asian handicap", "asian handicap", "full time"),
                ("asian handicap full time", "asian handicap", "full time"),
            }
        ),
        canonical_target_key="asian_handicap_full_time",
        canonical_target_name="Asian Handicap Full Time",
        canonical_target_group="Asian Handicap",
        canonical_target_period="Full Time",
        market_family="handicap",
        line_based=True,
    ),
    MarketShapeDefinition(
        kind="both_teams_to_score_full_time",
        source_shapes=frozenset(
            {
                ("both teams to score", "both teams to score", "full-time"),
                ("both teams to score full time", "both teams to score", "full time"),
            }
        ),
        canonical_target_key="both_teams_to_score_full_time",
        canonical_target_name="Both Teams To Score Full Time",
        canonical_target_group="Both Teams To Score",
        canonical_target_period="Full Time",
        market_family="btts",
    ),
    MarketShapeDefinition(
        kind="first_team_to_score_full_time",
        source_shapes=frozenset(
            {
                ("first team to score", "first team to score", "full-time"),
                ("first team to score full time", "first team to score", "full time"),
            }
        ),
        canonical_target_key="first_team_to_score_full_time",
        canonical_target_name="First Team To Score Full Time",
        canonical_target_group="First Team To Score",
        canonical_target_period="Full Time",
        market_family="first_team_to_score",
    ),
    MarketShapeDefinition(
        kind="double_chance_full_time",
        source_shapes=frozenset(
            {
                ("double chance", "double chance", "full-time"),
                ("double chance full time", "double chance", "full time"),
            }
        ),
        canonical_target_key="double_chance_full_time",
        canonical_target_name="Double Chance Full Time",
        canonical_target_group="Double Chance",
        canonical_target_period="Full Time",
        market_family="side",
    ),
    MarketShapeDefinition(
        kind="home_away_full_time_including_overtime",
        source_shapes=frozenset(
            {
                ("full time (including overtime)", "full time (including overtime)", "full-time"),
                ("home/away full time including overtime", "home/away", "full time including overtime"),
            }
        ),
        canonical_target_key="home_away_full_time_including_overtime",
        canonical_target_name="Home/Away Full Time Including Overtime",
        canonical_target_group="Home/Away",
        canonical_target_period="Full Time Including Overtime",
        market_family="side",
    ),
    MarketShapeDefinition(
        kind="next_goal_full_time",
        source_shapes=frozenset(
            {
                ("next goal", "next goal", "full-time"),
                ("next goal full time", "next goal", "full time"),
            }
        ),
        canonical_target_key="next_goal_full_time",
        canonical_target_name="Next Goal Full Time",
        canonical_target_group="Next Goal",
        canonical_target_period="Full Time",
        market_family="first_team_to_score",
    ),
)

SHAPE_BY_KEY: dict[tuple[str, str, str], MarketShapeDefinition] = {}
for definition in MARKET_SHAPE_DEFINITIONS:
    for shape in definition.source_shapes:
        SHAPE_BY_KEY[shape] = definition

CANONICAL_TARGET_KEYS = {
    "home_away_full_time",
    "home_away_1st_half",
    "home_away_full_time_including_overtime",
    "1x2_full_time",
    "1x2_1st_half",
    "over_under_full_time",
    "asian_handicap_full_time",
    "draw_no_bet_full_time",
    "double_chance_full_time",
    "both_teams_to_score_full_time",
    "first_team_to_score_full_time",
    "next_goal_full_time",
}


def _shape_key(market_name: Any, market_group: Any, market_period: Any) -> tuple[str, str, str]:
    return (normalize_text(market_name), normalize_text(market_group), normalize_text(market_period))


def classify_market_shape(market_name: Any, market_group: Any, market_period: Any) -> MarketShapeDefinition | None:
    return SHAPE_BY_KEY.get(_shape_key(market_name, market_group, market_period))


def resolve_choice_name(
    choice_name: Any,
    market_context: Mapping[str, Any],
    home_team: Any,
    away_team: Any,
) -> dict[str, Any]:
    raw = "" if choice_name is None else str(choice_name).strip()
    normalized = normalize_text(raw)
    kind = str(market_context.get("market_kind") or "")
    target_group_from_context = format_line(market_context.get("choice_group"))
    parsed = parse_parenthesized_line_choice(raw)
    home_slug = market_context.get("home_slug")
    away_slug = market_context.get("away_slug")

    result = {
        "target_choice_name": None,
        "target_choice_group": None,
        "parsed_line": None,
        "team_resolution_strategy": None,
        "team_resolution_confidence": None,
        "team_resolution_match": None,
        "action_type": "unresolved",
        "status": "unresolved",
        "reason": "unresolved",
    }
    if not raw:
        result["reason"] = "blank choice name"
        return result

    btts_kinds = {"btts", "both_teams_to_score_full_time"}
    normalized_choice = normalize_choice_name_to_canonical(raw)
    if normalized_choice is not None:
        if normalized_choice == "x":
            if kind in {"1x2_full_time", "1x2_1st_half"}:
                result.update(
                    {
                        "target_choice_name": "x",
                        "target_choice_group": target_group_from_context,
                        "action_type": "noop" if raw == "x" else "rename",
                        "status": "resolved",
                        "reason": "canonical choice",
                    }
                )
                return result
            result.update(
                {
                    "action_type": "anomaly",
                    "status": "anomaly",
                    "reason": f"draw not allowed for {kind}",
                }
            )
            return result
        if normalized_choice == "no_goal":
            if kind in {"first_team_to_score_full_time", "next_goal_full_time"}:
                result.update(
                    {
                        "target_choice_name": "no_goal",
                        "target_choice_group": target_group_from_context,
                        "action_type": "noop" if raw == "no_goal" else "rename",
                        "status": "resolved",
                        "reason": "no goal canonicalized",
                    }
                )
                return result
            result.update(
                {
                    "action_type": "anomaly",
                    "status": "anomaly",
                    "reason": "no_goal only allowed for first_team_to_score_full_time or next_goal_full_time",
                }
            )
            return result
        if normalized_choice in {"over", "under"}:
            if kind == "total_full_time":
                result.update(
                    {
                        "target_choice_name": normalized_choice,
                        "target_choice_group": target_group_from_context,
                        "action_type": "noop" if raw == normalized_choice else "rename",
                        "status": "resolved",
                        "reason": "canonical choice",
                    }
                )
                return result
            result.update(
                {
                    "action_type": "anomaly",
                    "status": "anomaly",
                    "reason": f"canonical {normalized_choice} not allowed for {kind}",
                }
            )
            return result
        if normalized_choice in {"yes", "no"}:
            if kind in btts_kinds:
                result.update(
                    {
                        "target_choice_name": normalized_choice,
                        "target_choice_group": target_group_from_context,
                        "action_type": "noop" if raw == normalized_choice else "rename",
                        "status": "resolved",
                        "reason": "BTTS canonicalized",
                    }
                )
                return result
            result.update(
                {
                    "action_type": "anomaly",
                    "status": "anomaly",
                    "reason": f"canonical {normalized_choice} not allowed for {kind}",
                }
            )
            return result
        if normalized_choice in {"1", "2"}:
            if kind in {
                "moneyline_full_time_2way",
                "moneyline_1st_half_2way",
                "1x2_full_time",
                "1x2_1st_half",
                "draw_no_bet_full_time",
                "asian_handicap_full_time",
                "first_team_to_score_full_time",
            }:
                result.update(
                    {
                        "target_choice_name": normalized_choice,
                        "target_choice_group": target_group_from_context,
                        "action_type": "noop" if raw == normalized_choice else "rename",
                        "status": "resolved",
                        "reason": "canonical choice",
                    }
                )
                return result
            result.update(
                {
                    "action_type": "anomaly",
                    "status": "anomaly",
                    "reason": f"canonical {normalized_choice} not allowed for {kind}",
                }
            )
            return result

    if kind in {"moneyline_full_time_2way", "moneyline_1st_half_2way", "1x2_full_time", "1x2_1st_half", "draw_no_bet_full_time"}:
        team_resolution = resolve_team_side(raw, home_team, away_team, home_slug, away_slug)
        result["team_resolution_strategy"] = team_resolution.strategy
        result["team_resolution_confidence"] = team_resolution.confidence
        result["team_resolution_match"] = team_resolution.matched_value
        if _team_resolution_can_persist(team_resolution):
            side = team_resolution.side
            if kind == "draw_no_bet_full_time" and side == "x":
                result.update({"action_type": "anomaly", "status": "anomaly", "reason": "draw not allowed for draw no bet"})
                return result
            result.update(
                {
                    "target_choice_name": side,
                    "target_choice_group": target_group_from_context,
                    "action_type": "rename",
                    "status": "resolved",
                    "reason": "resolved from team label",
                }
            )
            return result
        if team_resolution.side is not None:
            result.update(
                {
                    "action_type": "unresolved",
                    "status": "unresolved",
                    "reason": (
                        "team match below persistence threshold "
                        f"(strategy={team_resolution.strategy}, confidence={team_resolution.confidence})"
                    ),
                }
            )
            return result

        result.update(
            {
                "action_type": "anomaly",
                "status": "anomaly",
                "reason": (
                    "unable to resolve side "
                    f"(label={raw!r}, home_team={home_team!r}, away_team={away_team!r}, "
                    f"strategy={team_resolution.strategy}, confidence={team_resolution.confidence})"
                ),
            }
        )
        return result

    if kind in btts_kinds:
        if normalized in {"yes", "no"}:
            result.update(
                {
                    "target_choice_name": "yes" if normalized == "yes" else "no",
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if normalized == raw else "rename",
                    "status": "resolved",
                    "reason": "BTTS canonicalized",
                }
            )
            return result

    if kind == "total_full_time":
        if parsed:
            label_norm = normalize_text(parsed["label_after_line"])
            if label_norm in {"over", "under"}:
                result.update(
                    {
                        "target_choice_name": "over" if label_norm == "over" else "under",
                        "target_choice_group": parsed["line"],
                        "parsed_line": parsed["line"],
                        "action_type": "move" if parsed["line"] != target_group_from_context else "rename",
                        "status": "resolved",
                        "reason": "parsed total line",
                    }
                )
                return result
        if normalized in {"over", "under"} and target_group_from_context is not None:
            result.update(
                {
                    "target_choice_name": "over" if normalized == "over" else "under",
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if normalized == raw else "rename",
                    "status": "resolved",
                    "reason": "total canonicalized",
                }
            )
            return result

    if kind == "asian_handicap_full_time":
        if parsed:
            team_resolution = resolve_team_side(parsed["label_after_line"], home_team, away_team, home_slug, away_slug)
            result["team_resolution_strategy"] = team_resolution.strategy
            result["team_resolution_confidence"] = team_resolution.confidence
            result["team_resolution_match"] = team_resolution.matched_value
            side = team_resolution.side
            if not _team_resolution_can_persist(team_resolution):
                result.update(
                    {
                        "action_type": "unresolved",
                        "status": "unresolved",
                        "reason": (
                            "unable to persist handicap side "
                            f"(label={parsed['label_after_line']!r}, home_team={home_team!r}, away_team={away_team!r}, "
                            f"strategy={team_resolution.strategy}, confidence={team_resolution.confidence})"
                        ),
                    }
                )
                return result
            home_side_line = parsed["line"] if side == "1" else format_line(Decimal(parsed["line"]) * Decimal("-1"))
            result.update(
                {
                    "target_choice_name": side,
                    "target_choice_group": home_side_line,
                    "parsed_line": parsed["line"],
                    "action_type": "move" if home_side_line != target_group_from_context else "rename",
                    "status": "resolved",
                    "reason": "parsed handicap line",
                }
            )
            return result
        team_resolution = resolve_team_side(raw, home_team, away_team, home_slug, away_slug)
        result["team_resolution_strategy"] = team_resolution.strategy
        result["team_resolution_confidence"] = team_resolution.confidence
        result["team_resolution_match"] = team_resolution.matched_value
        if _team_resolution_can_persist(team_resolution):
            result.update(
                {
                    "target_choice_name": team_resolution.side,
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if normalized == raw else "rename",
                    "status": "resolved",
                    "reason": "resolved from team label",
                }
            )
            return result
        if normalized in {"1", "2"} and target_group_from_context is not None:
            result.update(
                {
                    "target_choice_name": normalized,
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if normalized == raw else "rename",
                    "status": "resolved",
                    "reason": "handicap canonicalized",
                }
            )
            return result

    if kind == "double_chance_full_time":
        normalized = normalize_text(raw)
        if normalized in {"1x", "x2", "12"}:
            result.update(
                {
                    "target_choice_name": normalized,
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if raw == normalized else "rename",
                    "status": "resolved",
                    "reason": "canonical choice",
                }
            )
            return result
        
        if " or " in normalized:
            parts = [p.strip() for p in normalized.split(" or ")]
            if len(parts) == 2:
                p1, p2 = parts[0], parts[1]
                if p1 == "draw" or p2 == "draw":
                    team_label = p2 if p1 == "draw" else p1
                    team_resolution = resolve_team_side(team_label, home_team, away_team, home_slug, away_slug)
                    if _team_resolution_can_persist(team_resolution):
                        side = team_resolution.side
                        target_name = "1x" if side == "1" else "x2"
                        result.update(
                            {
                                "target_choice_name": target_name,
                                "target_choice_group": target_group_from_context,
                                "action_type": "rename",
                                "status": "resolved",
                                "reason": f"resolved double chance draw combination from {team_resolution.strategy} match",
                            }
                        )
                        return result
                else:
                    res1 = resolve_team_side(p1, home_team, away_team, home_slug, away_slug)
                    res2 = resolve_team_side(p2, home_team, away_team, home_slug, away_slug)
                    if _team_resolution_can_persist(res1) and _team_resolution_can_persist(res2):
                        sides = {res1.side, res2.side}
                        if sides == {"1", "2"}:
                            result.update(
                                {
                                    "target_choice_name": "12",
                                    "target_choice_group": target_group_from_context,
                                    "action_type": "rename",
                                    "status": "resolved",
                                    "reason": "resolved double chance home-away combination",
                                }
                            )
                            return result

        result.update(
            {
                "action_type": "unresolved",
                "status": "unresolved",
                "reason": f"unable to resolve double chance side from label={raw!r}",
            }
        )
        return result

    if kind in {"first_team_to_score_full_time", "next_goal_full_time"}:
        normalized_choice = normalize_choice_name_to_canonical(raw)

        if normalized_choice == "no_goal":
            result.update(
                {
                    "target_choice_name": "no_goal",
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if raw == "no_goal" else "rename",
                    "status": "resolved",
                    "reason": "no goal canonicalized",
                }
            )
            return result

        team_resolution = resolve_team_side(raw, home_team, away_team, home_slug, away_slug)
        result["team_resolution_strategy"] = team_resolution.strategy
        result["team_resolution_confidence"] = team_resolution.confidence
        result["team_resolution_match"] = team_resolution.matched_value

        if _team_resolution_can_persist(team_resolution):
            result.update(
                {
                    "target_choice_name": team_resolution.side,
                    "target_choice_group": target_group_from_context,
                    "action_type": "noop" if raw == team_resolution.side else "rename",
                    "status": "resolved",
                    "reason": "resolved team side from team label",
                }
            )
            return result

        result.update(
            {
                "action_type": "unresolved",
                "status": "unresolved",
                "reason": (
                    "unable to resolve team side "
                    f"(label={raw!r}, home_team={home_team!r}, away_team={away_team!r}, "
                    f"strategy={team_resolution.strategy}, confidence={team_resolution.confidence})"
                ),
            }
        )
        return result

    result["reason"] = (
        "unrecognized choice "
        f"(choice_name={raw!r}, home_team={home_team!r}, away_team={away_team!r}, kind={kind})"
    )
    return result


def _target_market_for_resolution(market: MarketContext, resolution: Mapping[str, Any]) -> TargetMarketSpec | None:
    if market.market_kind in {"total_full_time", "asian_handicap_full_time"}:
        target_choice_group = resolution.get("target_choice_group") or format_line(market.choice_group)
        if target_choice_group is None:
            return None
        return TargetMarketSpec(
            event_id=market.event_id,
            bookie_id=market.bookie_id,
            is_live=market.is_live,
            market_name=market.market_name,
            market_group=market.market_group,
            market_period=market.market_period,
            choice_group=target_choice_group,
            target_market_key=market.canonical_target_key,
            source_market_ids=(market.market_id,),
        )

    # Side, draw-no-bet and BTTS markets stay in their source market row.
    return TargetMarketSpec(
        event_id=market.event_id,
        bookie_id=market.bookie_id,
        is_live=market.is_live,
        market_name=market.market_name,
        market_group=market.market_group,
        market_period=market.market_period,
        choice_group=market.choice_group,
        target_market_key=market.canonical_target_key,
        source_market_ids=(market.market_id,),
    )


def _target_spec_key(spec: TargetMarketSpec) -> tuple[Any, ...]:
    return (
        tuple(sorted(spec.source_market_ids)),
        spec.bookie_id,
        bool(spec.is_live),
    )


def _read_context_csv(path: Path | None, label: str) -> dict[str, Any] | None:
    if path is None:
        return None
    resolved = path.expanduser().resolve()
    with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    return {
        "label": label,
        "path": str(resolved),
        "rows": len(rows),
        "columns": reader.fieldnames or [],
    }


def validate_required_schema(connection) -> dict[str, Any]:
    inspector = inspect(connection)
    required = {
        "events": {"id", "sport", "home_team", "away_team", "home_participant_id", "away_participant_id"},
        "participants": {"participant_id", "name"},
        "markets": {"market_id", "event_id", "bookie_id", "market_name", "market_group", "market_period", "choice_group", "is_live", "collected_at"},
        "market_choices": {"choice_id", "market_id", "choice_name", "initial_odds", "current_odds", "change"},
        "market_choice_snapshots": {"snapshot_id", "choice_id", "odds_value", "collected_at"},
        "canonical_market_types": {"canonical_market_key", "canonical_market_name", "canonical_market_group", "canonical_market_period", "requires_choice_group"},
    }
    errors: list[str] = []
    for table_name, column_names in required.items():
        if not inspector.has_table(table_name):
            errors.append(f"missing table {table_name}")
            continue
        present = {column["name"] for column in inspector.get_columns(table_name)}
        missing = sorted(column_names - present)
        if missing:
            errors.append(f"{table_name} missing columns: {', '.join(missing)}")
    if errors:
        raise RuntimeError("Required schema validation failed:\n- " + "\n- ".join(errors))
    return {"tables": sorted(required), "columns": {table: sorted(cols) for table, cols in required.items()}}


def validate_canonical_targets(connection) -> dict[str, dict[str, Any]]:
    target_keys = sorted(CANONICAL_TARGET_KEYS)
    target_placeholders = ", ".join(f":target_key_{idx}" for idx in range(len(target_keys)))
    target_params = {f"target_key_{idx}": key for idx, key in enumerate(target_keys)}
    rows = connection.execute(
        text(
            """
            SELECT canonical_market_key, canonical_market_name, canonical_market_group,
                   canonical_market_period, requires_choice_group
            FROM canonical_market_types
            WHERE canonical_market_key IN ({target_placeholders})
            """.format(target_placeholders=target_placeholders)
        ),
        target_params,
    ).mappings().all()
    by_key = {row["canonical_market_key"]: dict(row) for row in rows}
    expected = {
        "home_away_full_time": ("Home/Away Full Time", "Home/Away", "Full Time", False),
        "home_away_1st_half": ("Home/Away 1st Half", "Home/Away", "1st Half", False),
        "home_away_full_time_including_overtime": ("Home/Away Full Time Including Overtime", "Home/Away", "Full Time Including Overtime", False),
        "1x2_full_time": ("1X2 Full Time", "1X2", "Full Time", False),
        "1x2_1st_half": ("1X2 1st Half", "1X2", "1st Half", False),
        "over_under_full_time": ("Over/Under Full Time", "Over/Under", "Full Time", True),
        "asian_handicap_full_time": ("Asian Handicap Full Time", "Asian Handicap", "Full Time", True),
        "draw_no_bet_full_time": ("Draw No Bet Full Time", "Draw No Bet", "Full Time", False),
        "double_chance_full_time": ("Double Chance Full Time", "Double Chance", "Full Time", False),
        "both_teams_to_score_full_time": ("Both Teams To Score Full Time", "Both Teams To Score", "Full Time", False),
        "first_team_to_score_full_time": ("First Team To Score Full Time", "First Team To Score", "Full Time", False),
        "next_goal_full_time": ("Next Goal Full Time", "Next Goal", "Full Time", False),
    }
    errors = []
    for key, expected_shape in expected.items():
        row = by_key.get(key)
        if row is None:
            errors.append(f"missing canonical target: {key}")
            continue
        actual_shape = (
            row["canonical_market_name"],
            row["canonical_market_group"],
            row["canonical_market_period"],
            bool(row["requires_choice_group"]),
        )
        if actual_shape != expected_shape:
            errors.append(f"{key} shape mismatch: expected={expected_shape!r} actual={actual_shape!r}")
    if errors:
        raise RuntimeError("Canonical target preflight failed:\n- " + "\n- ".join(errors))
    return by_key


def _candidate_shape_sql(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    pieces = []
    params: dict[str, Any] = {}
    for idx, definition in enumerate(MARKET_SHAPE_DEFINITIONS):
        if definition.market_family == "draw_no_bet" and not args.include_draw_no_bet:
            continue
        if definition.market_family == "btts" and not args.include_btts:
            continue
        if definition.market_family == "handicap" and not args.include_handicap_split:
            continue
        shape_parts = []
        for sidx, (name, group, period) in enumerate(sorted(definition.source_shapes)):
            prefix = f"s{idx}_{sidx}"
            params[f"{prefix}_name"] = name
            params[f"{prefix}_group"] = group
            params[f"{prefix}_period"] = period
            shape_parts.append(
                f"(lower(m.market_name) = :{prefix}_name AND lower(m.market_group) = :{prefix}_group AND lower(m.market_period) = :{prefix}_period)"
            )
        pieces.append("(" + " OR ".join(shape_parts) + ")")
    return (" OR ".join(pieces) if pieces else "1 = 0"), params


def _load_candidate_rows(connection, args: argparse.Namespace) -> list[dict[str, Any]]:
    shape_sql, shape_params = _candidate_shape_sql(args)
    where_clauses = ["m.bookie_id = :bookie_id"]
    params: dict[str, Any] = {"bookie_id": args.bookie_id, **shape_params}
    if args.sport is not None:
        where_clauses.append("e.sport = :sport")
        params["sport"] = args.sport
    if args.event_id is not None:
        where_clauses.append("m.event_id = :event_id")
        params["event_id"] = args.event_id
    if getattr(args, "event_id_start", None) is not None:
        where_clauses.append("m.event_id >= :event_id_start")
        params["event_id_start"] = args.event_id_start
    if getattr(args, "event_id_end", None) is not None:
        where_clauses.append("m.event_id <= :event_id_end")
        params["event_id_end"] = args.event_id_end
    if args.market_id is not None:
        where_clauses.append("m.market_id = :market_id")
        params["market_id"] = args.market_id
    where_clauses.append(f"({shape_sql})")
    where_clauses.append(
        f"""
        EXISTS (
            SELECT 1
            FROM market_choices c2
            WHERE c2.market_id = m.market_id
              AND {NONCANONICAL_CHOICE_SQL}
        )
        """
    )
    rows = connection.execute(
        text(
            f"""
            SELECT
                m.market_id,
                m.event_id,
                m.bookie_id,
                m.market_name,
                m.market_group,
                m.market_period,
                m.choice_group,
                m.is_live,
                m.collected_at,
                e.sport,
                e.home_team,
                e.away_team,
                e.start_time_utc,
                hp.name AS home_participant_name,
                ap.name AS away_participant_name,
                hp.slug AS home_participant_slug,
                ap.slug AS away_participant_slug,
                c.choice_id,
                c.choice_name,
                c.initial_odds,
                c.current_odds,
                c.change
            FROM markets m
            JOIN events e ON e.id = m.event_id
            LEFT JOIN participants hp ON hp.participant_id = e.home_participant_id
            LEFT JOIN participants ap ON ap.participant_id = e.away_participant_id
            LEFT JOIN market_choices c ON c.market_id = m.market_id
            WHERE {" AND ".join(where_clauses)}
            ORDER BY m.event_id, m.market_id, c.choice_id
            """
        ),
        params,
    ).mappings().all()
    return [dict(row) for row in rows]


def _build_market_contexts(
    rows: list[dict[str, Any]],
    limit: int | None,
    progress_every: int = 1000,
) -> tuple[list[MarketContext], dict[int, list[ChoiceContext]], list[str]]:
    markets: dict[int, MarketContext] = {}
    choices_by_market: dict[int, list[ChoiceContext]] = defaultdict(list)
    validation_errors: list[str] = []
    total_rows = len(rows)
    for index, row in enumerate(rows, start=1):
        _log_progress("Scanning candidate rows", index, total_rows, progress_every)
        definition = classify_market_shape(row["market_name"], row["market_group"], row["market_period"])
        if definition is None:
            continue
        market_id = int(row["market_id"])
        if market_id not in markets:
            home_team = row.get("home_participant_name") or row.get("home_team")
            away_team = row.get("away_participant_name") or row.get("away_team")
            markets[market_id] = MarketContext(
                market_id=market_id,
                event_id=int(row["event_id"]),
                bookie_id=int(row["bookie_id"]),
                market_name=row["market_name"],
                market_group=row["market_group"],
                market_period=row["market_period"],
                choice_group=row["choice_group"],
                is_live=bool(row["is_live"]),
                sport=row.get("sport"),
                home_team=home_team,
                away_team=away_team,
                source_shape=_shape_key(row["market_name"], row["market_group"], row["market_period"]),
                market_kind=definition.kind,
                canonical_target_key=definition.canonical_target_key,
                canonical_target_name=definition.canonical_target_name,
                canonical_target_group=definition.canonical_target_group,
                canonical_target_period=definition.canonical_target_period,
                line_based=definition.line_based,
                allows_draw=definition.allows_draw,
                source_collected_at=row.get("collected_at"),
                home_slug=row.get("home_participant_slug"),
                away_slug=row.get("away_participant_slug"),
                start_time_utc=row.get("start_time_utc"),
            )
        if row["choice_id"] is None:
            continue
        choices_by_market[market_id].append(
            ChoiceContext(
                choice_id=int(row["choice_id"]),
                market_id=market_id,
                choice_name=row["choice_name"],
                initial_odds=row.get("initial_odds"),
                current_odds=row.get("current_odds"),
                change=row.get("change"),
            )
        )
    if limit is not None:
        market_ids = sorted(markets)[:limit]
        markets = {market_id: markets[market_id] for market_id in market_ids}
        choices_by_market = {market_id: choices_by_market.get(market_id, []) for market_id in market_ids}
    market_list = sorted(markets.values(), key=lambda item: (item.event_id, item.market_id))
    for market in market_list:
        if market.market_kind != "btts" and (not market.home_team or not market.away_team):
            validation_errors.append(f"market_id={market.market_id} missing home/away teams")
    logger.info(
        "Prepared %d markets and %d choices from %d candidate rows",
        len(market_list),
        sum(len(choices_by_market.get(market.market_id, [])) for market in market_list),
        total_rows,
    )
    return market_list, choices_by_market, validation_errors


def _plan_choice(
    market: MarketContext,
    choice: ChoiceContext,
) -> ChoiceResolution:
    base = resolve_choice_name(choice.choice_name, asdict(market), market.home_team, market.away_team)
    if base["status"] != "resolved":
        return ChoiceResolution(
            choice_id=choice.choice_id,
            market_id=market.market_id,
            source_choice_name=choice.choice_name,
            home_team=market.home_team,
            away_team=market.away_team,
            team_resolution_strategy=base.get("team_resolution_strategy"),
            team_resolution_confidence=base.get("team_resolution_confidence"),
            team_resolution_match=base.get("team_resolution_match"),
            target_choice_name=None,
            target_choice_group=None,
            parsed_line=base.get("parsed_line"),
            action_type=base.get("action_type", "unresolved"),
            status=base["status"],
            reason=base["reason"],
            target_market_key=None,
            target_market_name=None,
            target_market_group=None,
            target_market_period=None,
            target_market_choice_group=None,
            target_market_is_exact_source=False,
            event_id=market.event_id,
            start_time_utc=market.start_time_utc,
        )

    target_choice_name = base["target_choice_name"]
    target_choice_group = base["target_choice_group"]
    target_market_name = market.market_name
    target_market_group = market.market_group
    target_market_period = market.market_period
    target_market_choice_group = market.choice_group

    if market.line_based:
        if target_choice_group is None:
            return ChoiceResolution(
                choice_id=choice.choice_id,
                market_id=market.market_id,
                source_choice_name=choice.choice_name,
                home_team=market.home_team,
                away_team=market.away_team,
                team_resolution_strategy=base.get("team_resolution_strategy"),
                team_resolution_confidence=base.get("team_resolution_confidence"),
                team_resolution_match=base.get("team_resolution_match"),
                target_choice_name=None,
                target_choice_group=None,
                parsed_line=base.get("parsed_line"),
                action_type=base.get("action_type", "unresolved"),
                status="anomaly",
                reason="line-based choice without resolvable line",
                target_market_key=None,
                target_market_name=None,
                target_market_group=None,
                target_market_period=None,
                target_market_choice_group=None,
                target_market_is_exact_source=False,
                event_id=market.event_id,
                start_time_utc=market.start_time_utc,
            )
        target_market_choice_group = target_choice_group
    target_market_key = market.canonical_target_key
    if target_choice_name == choice.choice_name and format_line(target_market_choice_group) == format_line(market.choice_group):
        action_type = "noop"
    elif target_choice_name == choice.choice_name:
        action_type = "group_update"
    else:
        action_type = "rename"

    return ChoiceResolution(
        choice_id=choice.choice_id,
        market_id=market.market_id,
        source_choice_name=choice.choice_name,
        home_team=market.home_team,
        away_team=market.away_team,
        team_resolution_strategy=base.get("team_resolution_strategy"),
        team_resolution_confidence=base.get("team_resolution_confidence"),
        team_resolution_match=base.get("team_resolution_match"),
        target_choice_name=target_choice_name,
        target_choice_group=target_choice_group,
        parsed_line=base.get("parsed_line"),
        action_type=action_type,
        status="resolved",
        reason=base["reason"],
        target_market_key=target_market_key,
        target_market_name=target_market_name,
        target_market_group=target_market_group,
        target_market_period=target_market_period,
        target_market_choice_group=target_market_choice_group,
        target_market_is_exact_source=True,
        event_id=market.event_id,
        start_time_utc=market.start_time_utc,
    )


def _build_plan(
    markets: list[MarketContext],
    choices_by_market: dict[int, list[ChoiceContext]],
    progress_every: int = 1000,
) -> tuple[list[ChoiceResolution], dict[TargetMarketSpec, list[ChoiceResolution]], list[dict[str, Any]]]:
    resolutions: list[ChoiceResolution] = []
    bucket_specs: dict[tuple[Any, ...], TargetMarketSpec] = {}
    bucket_resolutions: dict[tuple[Any, ...], list[ChoiceResolution]] = defaultdict(list)
    summary_by_shape: dict[tuple[str, str, str, str, bool], dict[str, Any]] = defaultdict(
        lambda: {
            "market_kind": "",
            "market_name": "",
            "market_group": "",
            "market_period": "",
            "is_live": False,
            "candidate_markets": 0,
            "candidate_choices": 0,
            "resolved_choices": 0,
            "unresolved_choices": 0,
            "simple_renames": 0,
            "group_updates": 0,
            "move_actions": 0,
        }
    )

    total_choices = sum(len(choices_by_market.get(market.market_id, [])) for market in markets)
    processed_choices = 0
    for market in markets:
        key = (market.market_kind, market.market_name, market.market_group or "", market.market_period, market.is_live)
        summary = summary_by_shape[key]
        summary.update(
            {
                "market_kind": market.market_kind,
                "market_name": market.market_name,
                "market_group": market.market_group or "",
                "market_period": market.market_period,
                "is_live": market.is_live,
            }
        )
        summary["candidate_markets"] += 1
        choices = choices_by_market.get(market.market_id, [])
        summary["candidate_choices"] += len(choices)
        for choice in choices:
            processed_choices += 1
            _log_progress("Planning choice resolutions", processed_choices, total_choices, progress_every)
            resolution = _plan_choice(market, choice)
            resolutions.append(resolution)
            if resolution.status != "resolved":
                summary["unresolved_choices"] += 1
                continue
            summary["resolved_choices"] += 1
            if resolution.action_type == "rename":
                summary["simple_renames"] += 1
            elif resolution.action_type == "group_update":
                summary["group_updates"] += 1
            spec = TargetMarketSpec(
                event_id=market.event_id,
                bookie_id=market.bookie_id,
                is_live=market.is_live,
                market_name=resolution.target_market_name or market.market_name,
                market_group=resolution.target_market_group if resolution.target_market_group is not None else market.market_group,
                market_period=resolution.target_market_period or market.market_period,
                choice_group=resolution.target_market_choice_group if resolution.target_market_choice_group is not None else market.choice_group,
                target_market_key=resolution.target_market_key or market.canonical_target_key,
                source_market_ids=(market.market_id,),
                source_choice_ids=(choice.choice_id,),
            )
            bucket_key = _target_spec_key(spec)
            existing_spec = bucket_specs.get(bucket_key)
            if existing_spec is None:
                bucket_specs[bucket_key] = spec
            else:
                bucket_specs[bucket_key] = replace(
                    existing_spec,
                    source_market_ids=tuple(sorted(set(existing_spec.source_market_ids + spec.source_market_ids))),
                    source_choice_ids=tuple(sorted(set(existing_spec.source_choice_ids + spec.source_choice_ids))),
                )
            bucket_resolutions[bucket_key].append(resolution)

    summary_rows = sorted(summary_by_shape.values(), key=lambda row: (row["market_kind"], row["market_name"], row["market_group"], row["market_period"], row["is_live"]))
    merged_buckets = {bucket_specs[key]: bucket_resolutions[key] for key in bucket_specs}
    logger.info(
        "Planned %d choices across %d markets: %d resolved, %d unresolved",
        len(resolutions),
        len(markets),
        sum(1 for item in resolutions if item.status == "resolved"),
        sum(1 for item in resolutions if item.status != "resolved"),
    )
    return resolutions, merged_buckets, summary_rows


def _merge_specs(specs: Iterable[TargetMarketSpec], progress_every: int = 1000) -> list[TargetMarketSpec]:
    progress_every = max(1, progress_every)
    items = list(specs)
    merged: dict[tuple[Any, ...], TargetMarketSpec] = {}
    for index, spec in enumerate(items, start=1):
        _log_progress("Merging target specs", index, len(items), progress_every)
        key = _target_spec_key(spec)
        existing = merged.get(key)
        if existing is None:
            merged[key] = spec
            continue
        merged[key] = replace(
            existing,
            source_market_ids=tuple(sorted(set(existing.source_market_ids + spec.source_market_ids))),
            source_choice_ids=tuple(sorted(set(existing.source_choice_ids + spec.source_choice_ids))),
        )
    logger.info("Merged %d target specs into %d unique targets", len(items), len(merged))
    return sorted(merged.values(), key=lambda item: (_target_spec_key(item), item.target_market_key))


def _annotate_existing_target_markets(
    connection,
    specs: list[TargetMarketSpec],
    progress_every: int = 1000,
) -> list[TargetMarketSpec]:
    if not specs:
        return []

    progress_every = max(1, progress_every)
    annotated = []
    for index, spec in enumerate(specs, start=1):
        if index == 1 or index % progress_every == 0 or index == len(specs):
            _log_progress("Annotating existing target markets", index, len(specs), progress_every)
        try:
            with connection.begin_nested():
                existing_market_id = _lookup_existing_market_id(connection, spec)
        except Exception as exc:
            logger.warning(
                "Could not annotate existing target market for key=%s event_id=%s bookie_id=%s market=%s/%s/%s choice_group=%r: %s",
                spec.target_market_key,
                spec.event_id,
                spec.bookie_id,
                spec.market_name,
                spec.market_group,
                spec.market_period,
                spec.choice_group,
                exc,
            )
            existing_market_id = None
        annotated.append(replace(spec, existing_market_id=existing_market_id))
    return annotated


def _load_context_summary(args: argparse.Namespace) -> list[dict[str, Any]]:
    context_csvs = []
    for path, label in (
        (args.markets_detailed_csv, "markets_detailed"),
        (args.weird_choices_csv, "weird_choices"),
        (args.projection_summary_csv, "projection_summary"),
        (args.merge_examples_csv, "merge_examples"),
        (args.handicap_line_samples_csv, "handicap_line_samples"),
    ):
        try:
            summary = _read_context_csv(path, label)
            if summary is not None:
                context_csvs.append(summary)
        except FileNotFoundError:
            logger.warning("Context CSV not found: %s", path)
        except Exception as exc:
            logger.warning("Could not read context CSV %s: %s", path, exc)
    return context_csvs


def _move_choice_into_bucket(
    connection,
    resolution: ChoiceResolution,
    target_market_id: int,
    survivor_choice_id: int,
) -> None:
    if survivor_choice_id != resolution.choice_id:
        duplicate = connection.execute(
            text(
                """
                SELECT initial_odds, current_odds
                FROM market_choices
                WHERE choice_id = :choice_id
                """
            ),
            {"choice_id": resolution.choice_id},
        ).mappings().first()
        survivor = connection.execute(
            text(
                """
                SELECT initial_odds, current_odds
                FROM market_choices
                WHERE choice_id = :choice_id
                """
            ),
            {"choice_id": survivor_choice_id},
        ).mappings().first()
        if duplicate and survivor:
            if survivor["initial_odds"] is None and duplicate["initial_odds"] is not None:
                connection.execute(
                    text("UPDATE market_choices SET initial_odds = :v WHERE choice_id = :choice_id"),
                    {"v": duplicate["initial_odds"], "choice_id": survivor_choice_id},
                )
            if survivor["current_odds"] is None and duplicate["current_odds"] is not None:
                connection.execute(
                    text("UPDATE market_choices SET current_odds = :v WHERE choice_id = :choice_id"),
                    {"v": duplicate["current_odds"], "choice_id": survivor_choice_id},
                )
            connection.execute(
                text("UPDATE market_choice_snapshots SET choice_id = :survivor WHERE choice_id = :duplicate"),
                {"survivor": survivor_choice_id, "duplicate": resolution.choice_id},
            )
            connection.execute(text("DELETE FROM market_choices WHERE choice_id = :choice_id"), {"choice_id": resolution.choice_id})

    connection.execute(
        text(
            """
            UPDATE market_choices
            SET choice_name = :choice_name
            WHERE choice_id = :choice_id
            """
        ),
        {
            "choice_name": resolution.target_choice_name,
            "choice_id": survivor_choice_id,
        },
    )


def _count_rows(connection, sql: str, params: dict[str, Any] | None = None) -> int:
    result = connection.execute(text(sql), params or {}).mappings().one()
    return int(result["count"])


def _count_rows_in_list(
    connection,
    sql: str,
    param_name: str,
    values: Sequence[Any],
    prefix: str,
    chunk_size: int = 1000,
) -> int:
    if not values:
        return 0
    total = 0
    for chunk_index, chunk in enumerate(_chunked(values, chunk_size), start=1):
        expanded_sql, expanded_params = _expand_in_clause(sql, param_name, chunk, f"{prefix}_{chunk_index}")
        total += _count_rows(connection, expanded_sql, expanded_params)
    return total


def _validate_post_commit(
    connection,
    market_ids: Sequence[int],
    event_ids: Sequence[int],
    before_snapshot_count: int,
    before_trajectory_count: int | None,
    changed_choice_count: int,
) -> dict[str, Any]:
    if not market_ids:
        return {
            "remaining_noncanonical": 0,
            "line_markets_without_group": 0,
            "duplicate_choices": 0,
            "snapshot_count_before": before_snapshot_count,
            "snapshot_count_after": before_snapshot_count,
            "trajectory_count_before": before_trajectory_count,
            "trajectory_count_after": before_trajectory_count,
        }
    remaining_noncanonical = _count_rows_in_list(
        connection,
        """
        SELECT COUNT(*) AS count
        FROM market_choices mc
        WHERE {noncanonical_sql}
          AND mc.market_id IN :market_ids
        """.format(noncanonical_sql=_noncanonical_choice_sql("mc")),
        "market_ids",
        market_ids,
        "market_id",
    )
    line_markets_without_group = _count_rows_in_list(
        connection,
        """
        SELECT COUNT(*) AS count
        FROM markets
        WHERE market_id IN :market_ids
          AND market_name IN ('Asian handicap', 'Point spread', 'Total', 'Game total', 'Match goals')
          AND choice_group IS NULL
        """,
        "market_ids",
        market_ids,
        "market_id",
    )
    duplicate_choices = _count_rows_in_list(
        connection,
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT market_id, choice_name
            FROM market_choices
            WHERE market_id IN :market_ids
            GROUP BY market_id, choice_name
            HAVING COUNT(*) > 1
        ) duplicates
        """,
        "market_ids",
        market_ids,
        "market_id",
    )
    after_snapshot_count = _count_rows(connection, "SELECT COUNT(*) AS count FROM market_choice_snapshots")
    if after_snapshot_count != before_snapshot_count:
        raise RuntimeError(
            f"Snapshot count changed unexpectedly: before={before_snapshot_count} after={after_snapshot_count}"
        )
    after_trajectory_count = None
    try:
        after_trajectory_count = _count_rows_in_list(
            connection,
            """
            SELECT COUNT(*) AS count
            FROM public.v_pre_start_odds_trajectory
            WHERE event_id IN :event_ids
            """,
            "event_ids",
            list(event_ids),
            "event_id",
            1000,
        )
    except Exception as exc:  # pragma: no cover - runtime guard
        raise RuntimeError(f"Could not validate v_pre_start_odds_trajectory: {exc}") from exc
    if changed_choice_count > 0 and before_trajectory_count is not None and after_trajectory_count < before_trajectory_count:
        raise RuntimeError(
            f"Trajectory rows decreased unexpectedly: before={before_trajectory_count} after={after_trajectory_count}"
        )
    return {
        "remaining_noncanonical": remaining_noncanonical,
        "line_markets_without_group": line_markets_without_group,
        "duplicate_choices": duplicate_choices,
        "snapshot_count_before": before_snapshot_count,
        "snapshot_count_after": after_snapshot_count,
        "trajectory_count_before": before_trajectory_count,
        "trajectory_count_after": after_trajectory_count,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill SofaScore choice names and choice groups.")
    parser.add_argument("--bookie-id", type=int, default=DEFAULT_BOOKIE_ID)
    parser.add_argument("--sport")
    parser.add_argument("--event-id", type=int)
    parser.add_argument("--event-id-start", type=int)
    parser.add_argument("--event-id-end", type=int)
    parser.add_argument("--market-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--include-draw-no-bet", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--include-btts", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--include-handicap-split", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Emit progress logs every N processed rows or updates.",
    )
    parser.add_argument("--output-dir", type=Path, help="Override output directory (defaults to ./debug/choice_renaming/<run>).")
    parser.add_argument("--output-json", action="store_true")
    parser.add_argument("--markets-detailed-csv", type=Path)
    parser.add_argument("--weird-choices-csv", type=Path)
    parser.add_argument("--projection-summary-csv", type=Path)
    parser.add_argument("--merge-examples-csv", type=Path)
    parser.add_argument("--handicap-line-samples-csv", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--commit", action="store_true")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)
    args.dry_run = not args.commit
    return args


def _load_context_csvs(args: argparse.Namespace) -> list[dict[str, Any]]:
    context_csvs = []
    for path, label in (
        (args.markets_detailed_csv, "markets_detailed"),
        (args.weird_choices_csv, "weird_choices"),
        (args.projection_summary_csv, "projection_summary"),
        (args.merge_examples_csv, "merge_examples"),
        (args.handicap_line_samples_csv, "handicap_line_samples"),
    ):
        try:
            summary = _read_context_csv(path, label)
            if summary is not None:
                context_csvs.append(summary)
        except FileNotFoundError:
            logger.warning("Context CSV not found: %s", path)
        except Exception as exc:
            logger.warning("Could not read context CSV %s: %s", path, exc)
    return context_csvs


def _read_context_csv(path: Path | None, label: str) -> dict[str, Any] | None:
    if path is None:
        return None
    resolved = path.expanduser().resolve()
    with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    return {"label": label, "path": str(resolved), "rows": len(rows), "columns": reader.fieldnames or []}


def _default_output_dir(args: argparse.Namespace, mode: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix_parts = [mode, f"bookie_{args.bookie_id}"]
    if args.limit is not None:
        suffix_parts.append(f"limit_{args.limit}")
    if args.event_id is not None:
        suffix_parts.append(f"event_{args.event_id}")
    if args.market_id is not None:
        suffix_parts.append(f"market_{args.market_id}")
    if args.sport is not None:
        suffix_parts.append(f"sport_{re.sub(r'[^a-zA-Z0-9]+', '_', str(args.sport)).strip('_')}")
    return (DEFAULT_OUTPUT_ROOT / f"{timestamp}_{'_'.join(suffix_parts)}").resolve()


def _print_summary(summary: dict[str, Any]) -> None:
    print("\nSofaScore choice backfill")
    print(f"  mode: {summary['mode']}")
    if summary.get("output_dir"):
        print(f"  output_dir: {summary['output_dir']}")
    print(f"  bookie_id: {summary['bookie_id']}")
    print(f"  sport: {summary['sport'] or '(all)'}")
    print(f"  event_id: {summary['event_id'] or '(all)'}")
    print(f"  market_id: {summary['market_id'] or '(all)'}")
    print(f"  include_draw_no_bet: {summary['include_draw_no_bet']}")
    print(f"  include_btts: {summary['include_btts']}")
    print(f"  include_handicap_split: {summary['include_handicap_split']}")
    print(f"  total candidate markets: {summary['total_candidate_markets']}")
    print(f"  total candidate choices: {summary['total_candidate_choices']}")
    print(f"  resolvable choices: {summary['resolvable_choices']}")
    print(f"  unresolved choices: {summary['unresolved_choices']}")
    if "choice_name_changes" in summary:
        print(f"  choice name changes: {summary['choice_name_changes']}")
    if "choice_group_changes" in summary:
        print(f"  choice group changes: {summary['choice_group_changes']}")
    if "market_moves" in summary:
        print(f"  market moves: {summary['market_moves']}")
    if "changed_choice_count" in summary:
        print(f"  changed choices: {summary['changed_choice_count']}")
    print(f"  simple renames: {summary['simple_renames']}")
    if "group_updates" in summary:
        print(f"  group updates: {summary['group_updates']}")
    print(f"  move actions: {summary['move_actions']}")
    print(f"  merges needed: {summary['merges_needed']}")
    print(f"  target markets missing (would need creation): {summary['target_markets_to_create']}")
    print(f"  markets that would become empty if choices were removed: {summary['old_markets_that_would_become_empty']}")
    if summary.get("resume"):
        resume = summary["resume"]
        print("  resume:")
        print(f"    markets: {resume['source_markets']} source -> {resume['target_specs']} specs")
        print(f"    specs: {resume['existing_target_markets']} existing, {resume['new_target_markets']} new")
        print(
            f"    choice changes: {resume['choice_action_counts']} "
            f"choice_name_changes={resume['choice_name_changes']} "
            f"choice_group_changes={resume['choice_group_changes']} "
            f"market_moves={resume['market_moves']}"
        )
        if resume.get("examples"):
            print("    choice examples:")
            for item in resume["examples"]:
                print(f"      - market_id={item['market_id']} choice_id={item['choice_id']} market: {item['market']}")
                print(f"        choice: {item['source_choice_name']!r} -> {item['target_choice_name']!r}")
                print(f"        group: {item['source_choice_group']!r} -> {item['target_choice_group']!r}")
                print(
                    f"        team_resolution: strategy={item.get('team_resolution_strategy')!r} "
                    f"confidence={item.get('team_resolution_confidence')!r} "
                    f"match={item.get('team_resolution_match')!r}"
                )
                print(f"        action: {item['action_type']} ({item['reason']})")
            if resume.get("examples_truncated"):
                print("      ... examples truncated")
    if summary.get("validation_errors"):
        print("  validation errors:")
        for item in summary["validation_errors"]:
            print(f"    - {item}")
    if summary.get("top_unresolved_examples"):
        print("  top unresolved examples:")
        for item in summary["top_unresolved_examples"]:
            print(
                f"    - market_id={item['market_id']} choice_id={item['choice_id']} "
                f"choice_name={item['source_choice_name']!r} "
                f"home_team={item.get('home_team')!r} away_team={item.get('away_team')!r} "
                f"strategy={item.get('team_resolution_strategy')!r} "
                f"confidence={item.get('team_resolution_confidence')!r} "
                f"match={item.get('team_resolution_match')!r} "
                f"reason={item['reason']}"
            )
    if summary.get("context_csvs"):
        print("  context csvs:")
        for item in summary["context_csvs"]:
            print(f"    - {item['label']}: rows={item['rows']} path={item['path']}")
    if summary.get("csv_outputs"):
        print("  csv outputs:")
        for item in summary["csv_outputs"]:
            print(f"    - {item}")
    if summary.get("post_validation"):
        print("  post validation:")
        for key, value in summary["post_validation"].items():
            print(f"    - {key}: {value}")


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def _choice_resolution_row(resolution: ChoiceResolution) -> dict[str, Any]:
    return asdict(resolution)


def _bucket_rows(resolutions: list[ChoiceResolution]) -> list[dict[str, Any]]:
    rows = []
    for resolution in resolutions:
        rows.append(
            {
                "choice_id": resolution.choice_id,
                "market_id": resolution.market_id,
                "source_choice_name": resolution.source_choice_name,
                "target_choice_name": resolution.target_choice_name,
                "target_choice_group": resolution.target_choice_group,
                "parsed_line": resolution.parsed_line,
                "action_type": resolution.action_type,
                "status": resolution.status,
                "reason": resolution.reason,
                "target_market_key": resolution.target_market_key,
                "target_market_name": resolution.target_market_name,
                "target_market_group": resolution.target_market_group,
                "target_market_period": resolution.target_market_period,
                "target_market_choice_group": resolution.target_market_choice_group,
                "target_market_is_exact_source": resolution.target_market_is_exact_source,
                "team_resolution_strategy": resolution.team_resolution_strategy,
                "team_resolution_confidence": resolution.team_resolution_confidence,
                "team_resolution_match": resolution.team_resolution_match,
            }
        )
    return rows


def _unresolved_rows(resolutions: list[ChoiceResolution]) -> list[dict[str, Any]]:
    return [
        {
            "choice_id": item.choice_id,
            "market_id": item.market_id,
            "event_id": item.event_id,
            "start_time": item.start_time_utc.isoformat() if item.start_time_utc else None,
            "choice_name": item.source_choice_name,
            "source_choice_name": item.source_choice_name,
            "home_team": item.home_team,
            "away_team": item.away_team,
            "team_resolution_strategy": item.team_resolution_strategy,
            "team_resolution_confidence": item.team_resolution_confidence,
            "team_resolution_match": item.team_resolution_match,
            "reason": item.reason,
            "status": item.status,
            "action_type": item.action_type,
        }
        for item in resolutions
        if item.status != "resolved"
    ]


def _changed_choice_rows(resolutions: list[ChoiceResolution]) -> list[dict[str, Any]]:
    return [
        {
            "choice_id": item.choice_id,
            "market_id": item.market_id,
            "source_choice_name": item.source_choice_name,
            "target_choice_name": item.target_choice_name,
            "target_choice_group": item.target_choice_group,
            "parsed_line": item.parsed_line,
            "team_resolution_strategy": item.team_resolution_strategy,
            "team_resolution_confidence": item.team_resolution_confidence,
            "team_resolution_match": item.team_resolution_match,
            "action_type": item.action_type,
            "status": item.status,
            "reason": item.reason,
            "target_market_key": item.target_market_key,
            "target_market_name": item.target_market_name,
            "target_market_group": item.target_market_group,
            "target_market_period": item.target_market_period,
            "target_market_choice_group": item.target_market_choice_group,
        }
        for item in resolutions
        if item.status == "resolved" and item.action_type != "noop"
    ]


def _resolution_stats(resolutions: list[ChoiceResolution], sample_limit: int = 100) -> dict[str, Any]:
    resolved = 0
    unresolved = 0
    simple_renames = 0
    move_actions = 0
    for item in resolutions:
        if item.status == "resolved":
            resolved += 1
            if item.target_market_is_exact_source and item.target_choice_name == item.source_choice_name:
                simple_renames += 1
            if not item.target_market_is_exact_source:
                move_actions += 1
        else:
            unresolved += 1
    sample_limit = max(0, sample_limit)
    return {
        "resolvable_choices": resolved,
        "unresolved_choices": unresolved,
        "simple_renames": simple_renames,
        "move_actions": move_actions,
        "choice_resolutions_count": len(resolutions),
        "choice_resolutions_sample_limit": sample_limit,
        "choice_resolutions_sample": [asdict(item) for item in resolutions[:sample_limit]],
        "choice_resolutions_truncated": len(resolutions) > sample_limit,
    }


def _format_market_shape_text(market_name: Any, market_group: Any, market_period: Any) -> str:
    return f"{market_name} / {market_group or ''} / {market_period}"


def _build_resume(
    markets: list[MarketContext],
    specs: list[TargetMarketSpec],
    resolutions: list[ChoiceResolution],
    market_map: dict[int, MarketContext],
    sample_limit: int = 10,
) -> dict[str, Any]:
    sample_limit = max(0, sample_limit)
    action_counts: dict[str, int] = defaultdict(int)
    choice_name_changes = 0
    choice_group_changes = 0
    market_moves = 0
    changed_total = 0
    examples: list[dict[str, Any]] = []
    for resolution in resolutions:
        action_counts[resolution.action_type] += 1
        market = market_map.get(resolution.market_id)
        if market is None or resolution.status != "resolved":
            continue
        source_choice_group = format_line(market.choice_group)
        target_choice_group = format_line(resolution.target_market_choice_group)
        if resolution.source_choice_name != resolution.target_choice_name:
            choice_name_changes += 1
        if source_choice_group != target_choice_group:
            choice_group_changes += 1
        if resolution.source_choice_name != resolution.target_choice_name or source_choice_group != target_choice_group:
            changed_total += 1
            if len(examples) >= sample_limit:
                continue
            examples.append(
                {
                    "market_id": resolution.market_id,
                    "choice_id": resolution.choice_id,
                    "market": _format_market_shape_text(market.market_name, market.market_group, market.market_period),
                    "source_choice_name": resolution.source_choice_name,
                    "target_choice_name": resolution.target_choice_name,
                    "source_choice_group": source_choice_group,
                    "target_choice_group": target_choice_group,
                    "team_resolution_strategy": resolution.team_resolution_strategy,
                    "team_resolution_confidence": resolution.team_resolution_confidence,
                    "team_resolution_match": resolution.team_resolution_match,
                    "action_type": resolution.action_type,
                    "reason": resolution.reason,
                }
            )
    return {
        "source_markets": len(markets),
        "target_specs": len(specs),
        "existing_target_markets": sum(1 for spec in specs if spec.existing_market_id is not None),
        "new_target_markets": sum(1 for spec in specs if spec.existing_market_id is None),
        "choice_action_counts": dict(sorted(action_counts.items())),
        "choice_name_changes": choice_name_changes,
        "choice_group_changes": choice_group_changes,
        "market_moves": market_moves,
        "changed_choice_count": changed_total,
        "examples": examples,
        "examples_truncated": changed_total > len(examples),
    }


def _merge_actions_from_buckets(
    buckets: dict[TargetMarketSpec, list[ChoiceResolution]],
    target_market_ids: dict[tuple[Any, ...], int],
) -> list[MergeAction]:
    actions: list[MergeAction] = []
    for spec, resolutions in buckets.items():
        target_market_id = target_market_ids.get(_target_spec_key(spec))
        target_choice_names = defaultdict(list)
        for resolution in resolutions:
            if resolution.target_choice_name is not None:
                target_choice_names[resolution.target_choice_name].append(resolution)
        for target_choice_name, same_name_resolutions in target_choice_names.items():
            if len(same_name_resolutions) < 2:
                continue
            survivor = min(same_name_resolutions, key=lambda item: item.choice_id)
            for duplicate in same_name_resolutions:
                if duplicate.choice_id == survivor.choice_id:
                    continue
                actions.append(
                    MergeAction(
                        target_market_key=spec.target_market_key,
                        target_choice_name=target_choice_name,
                        survivor_choice_id=survivor.choice_id,
                        duplicate_choice_id=duplicate.choice_id,
                        source_market_id=duplicate.market_id,
                        source_choice_name=duplicate.source_choice_name,
                        target_market_id=target_market_id,
                    )
                )
    return actions


def _count_source_markets_that_would_become_empty(
    markets: list[MarketContext],
    resolutions: list[ChoiceResolution],
) -> int:
    by_market: dict[int, list[ChoiceResolution]] = defaultdict(list)
    for resolution in resolutions:
        by_market[resolution.market_id].append(resolution)
    empty = 0
    for market in markets:
        relevant = by_market.get(market.market_id, [])
        if not relevant:
            continue
        if any(item.status != "resolved" for item in relevant):
            continue
        if any(item.target_market_is_exact_source for item in relevant):
            continue
        empty += 1
    return empty


def _lookup_existing_market_id(connection, spec: TargetMarketSpec) -> int | None:
    row = connection.execute(
        text(
            """
            SELECT market_id
            FROM markets
            WHERE event_id = :event_id
              AND bookie_id = :bookie_id
              AND market_name = :market_name
              AND COALESCE(market_group, '') = COALESCE(:market_group, '')
              AND market_period = :market_period
              AND COALESCE(choice_group, '') = COALESCE(:choice_group, '')
              AND is_live = :is_live
            """
        ),
        {
            "event_id": spec.event_id,
            "bookie_id": spec.bookie_id,
            "market_name": spec.market_name,
            "market_group": spec.market_group,
            "market_period": spec.market_period,
            "choice_group": spec.choice_group,
            "is_live": spec.is_live,
        },
    ).mappings().first()
    if row is None:
        return None
    return int(row["market_id"])


def _find_or_create_market(connection, spec: TargetMarketSpec, source_collected_at: datetime | None = None) -> int:
    if spec.existing_market_id is not None:
        return spec.existing_market_id
    existing_market_id = _lookup_existing_market_id(connection, spec)
    if existing_market_id is not None:
        return existing_market_id
    result = connection.execute(
        insert(Market).values(
            event_id=spec.event_id,
            bookie_id=spec.bookie_id,
            market_name=spec.market_name,
            market_group=spec.market_group,
            market_period=spec.market_period,
            choice_group=spec.choice_group,
            is_live=spec.is_live,
            collected_at=source_collected_at or datetime.utcnow(),
        )
    )
    return int(result.inserted_primary_key[0])


def _update_survivor_odds(connection, survivor_choice_id: int, duplicate_choice_id: int) -> None:
    duplicate = connection.execute(
        text("SELECT initial_odds, current_odds FROM market_choices WHERE choice_id = :choice_id"),
        {"choice_id": duplicate_choice_id},
    ).mappings().first()
    survivor = connection.execute(
        text("SELECT initial_odds, current_odds FROM market_choices WHERE choice_id = :choice_id"),
        {"choice_id": survivor_choice_id},
    ).mappings().first()
    if not duplicate or not survivor:
        return
    if survivor["initial_odds"] is None and duplicate["initial_odds"] is not None:
        connection.execute(
            text("UPDATE market_choices SET initial_odds = :value WHERE choice_id = :choice_id"),
            {"value": duplicate["initial_odds"], "choice_id": survivor_choice_id},
        )
    if survivor["current_odds"] is None and duplicate["current_odds"] is not None:
        connection.execute(
            text("UPDATE market_choices SET current_odds = :value WHERE choice_id = :choice_id"),
            {"value": duplicate["current_odds"], "choice_id": survivor_choice_id},
        )


def _merge_choice_into_survivor(connection, target_market_id: int, resolution: ChoiceResolution, survivor_choice_id: int) -> None:
    if survivor_choice_id != resolution.choice_id:
        _update_survivor_odds(connection, survivor_choice_id, resolution.choice_id)
        connection.execute(
            text("UPDATE market_choice_snapshots SET choice_id = :survivor WHERE choice_id = :duplicate"),
            {"survivor": survivor_choice_id, "duplicate": resolution.choice_id},
        )
        connection.execute(text("DELETE FROM market_choices WHERE choice_id = :choice_id"), {"choice_id": resolution.choice_id})
    connection.execute(
        text(
            """
            UPDATE market_choices
            SET choice_name = :choice_name
            WHERE choice_id = :choice_id
            """
        ),
        {
            "choice_name": resolution.target_choice_name,
            "choice_id": survivor_choice_id,
        },
    )


def _read_snapshot_count(connection) -> int:
    return _count_rows(connection, "SELECT COUNT(*) AS count FROM market_choice_snapshots")


def _trajectory_count(connection, event_ids: Sequence[int]) -> int | None:
    if not event_ids:
        return 0
    try:
        with connection.begin_nested():
            return _count_rows_in_list(
                connection,
                """
                SELECT COUNT(*) AS count
                FROM public.v_pre_start_odds_trajectory
                WHERE event_id IN :event_ids
                """,
                "event_ids",
                list(event_ids),
                "event_id",
                1000,
            )
    except Exception:
        return None


def _count_rows(connection, sql: str, params: dict[str, Any] | None = None) -> int:
    stmt_sql = sql
    stmt_params = dict(params or {})
    if "event_ids" in stmt_params:
        event_ids = list(stmt_params.pop("event_ids"))
        if not event_ids:
            return 0
        placeholders = ", ".join(f":event_id_{idx}" for idx in range(len(event_ids)))
        stmt_sql = stmt_sql.replace(":event_ids", f"({placeholders})")
        stmt_params.update({f"event_id_{idx}": event_id for idx, event_id in enumerate(event_ids)})
    row = connection.execute(text(stmt_sql), stmt_params).mappings().one()
    return int(row["count"])


def run(args: argparse.Namespace) -> tuple[int, dict[str, Any] | None]:
    context_csvs = _load_context_csvs(args)
    mode = "commit" if args.commit else "dry-run"
    output_dir = args.output_dir.expanduser().resolve() if args.output_dir is not None else _default_output_dir(args, mode)
    output_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, Any] | None = None
    logger.info(
        "Starting %s backfill: bookie_id=%s sport=%s event_id=%s market_id=%s progress_every=%s",
        mode,
        args.bookie_id,
        args.sport,
        args.event_id,
        args.market_id,
        args.progress_every,
    )

    try:
        if args.commit:
            with progress_step("Opening commit transaction"):
                with db_manager.engine.begin() as connection:
                    with progress_step("Validating schema"):
                        validate_required_schema(connection)
                    with progress_step("Validating canonical targets"):
                        validate_canonical_targets(connection)
                    with progress_step("Loading candidate rows"):
                        rows = _load_candidate_rows(connection, args)
                        markets, choices_by_market, validation_errors = _build_market_contexts(
                            rows,
                            args.limit,
                            args.progress_every,
                        )
                    if validation_errors:
                        for item in validation_errors:
                            logger.warning("Commit preflight warning: %s", item)
                    resolutions, buckets, summary_rows = _build_plan(
                        markets,
                        choices_by_market,
                        args.progress_every,
                    )
                    market_map = {market.market_id: market for market in markets}
                    before_snapshot_count = _read_snapshot_count(connection)
                    source_market_ids = sorted({market.market_id for market in markets})
                    event_ids = sorted({market.event_id for market in markets})
                    before_trajectory_count = _trajectory_count(connection, event_ids)
                    with progress_step("Merging target specs"):
                        specs = _merge_specs(buckets.keys(), args.progress_every)
                    with progress_step("Annotating existing target markets"):
                        specs = _annotate_existing_target_markets(connection, specs, args.progress_every)
                    target_market_ids: dict[tuple[Any, ...], int] = {}
                    for spec in specs:
                        target_market_ids[_target_spec_key(spec)] = spec.source_market_ids[0]
                    touched_market_ids = sorted(set(source_market_ids).union(target_market_ids.values()))
                    merge_actions = _merge_actions_from_buckets(buckets, target_market_ids)
                    total_updates = sum(1 for item in resolutions if item.status == "resolved")
                    applied_updates = 0
                    logger.info(
                        "Applying backfill updates: %d resolved choices across %d target markets",
                        total_updates,
                        len(specs),
                    )
                    for spec, resolutions_for_spec in buckets.items():
                        target_market_id = target_market_ids[_target_spec_key(spec)]
                        source_market = market_map.get(target_market_id)
                        source_choice_group = format_line(source_market.choice_group) if source_market is not None else None
                        target_choice_group = format_line(spec.choice_group)
                        if (
                            source_market is not None
                            and not source_choice_group
                            and target_choice_group is not None
                        ):
                            try:
                                with connection.begin_nested():
                                    conflict = connection.execute(
                                        text(
                                            """
                                            SELECT market_id
                                            FROM markets
                                            WHERE event_id = :event_id
                                              AND bookie_id = :bookie_id
                                              AND market_name = :market_name
                                              AND COALESCE(market_group, '') = COALESCE(:market_group, '')
                                              AND market_period = :market_period
                                              AND COALESCE(choice_group, '') = COALESCE(:choice_group, '')
                                              AND is_live = :is_live
                                              AND market_id != :market_id
                                            """
                                        ),
                                        {
                                            "event_id": spec.event_id,
                                            "bookie_id": spec.bookie_id,
                                            "market_name": spec.market_name,
                                            "market_group": spec.market_group,
                                            "market_period": spec.market_period,
                                            "choice_group": spec.choice_group,
                                            "is_live": spec.is_live,
                                            "market_id": target_market_id,
                                        },
                                    ).mappings().first()
                                    if conflict is None:
                                        connection.execute(
                                            text(
                                                """
                                                UPDATE markets
                                                SET choice_group = :choice_group
                                                WHERE market_id = :market_id
                                                """
                                            ),
                                            {"choice_group": spec.choice_group, "market_id": target_market_id},
                                        )
                                    else:
                                        logger.warning(
                                            "Skipping choice_group update for market_id=%s because market_id=%s already has the canonical key",
                                            target_market_id,
                                            conflict["market_id"],
                                        )
                            except Exception as exc:
                                logger.warning(
                                    "Skipping choice_group backfill for market_id=%s because the guarded update failed: %s",
                                    target_market_id,
                                    exc,
                                )
                        elif source_market is not None and source_choice_group != target_choice_group:
                            logger.debug(
                                "Leaving existing choice_group unchanged for market_id=%s (current=%r, desired=%r)",
                                target_market_id,
                                source_market.choice_group,
                                spec.choice_group,
                            )
                        existing_target_choice_ids = {
                            row["choice_name"]: int(row["choice_id"])
                            for row in connection.execute(
                                text(
                                    """
                                    SELECT choice_id, choice_name
                                    FROM market_choices
                                    WHERE market_id = :market_id
                                    """
                                ),
                                {"market_id": target_market_id},
                            ).mappings().all()
                        }
                        by_choice_name: dict[str, list[ChoiceResolution]] = defaultdict(list)
                        for resolution in resolutions_for_spec:
                            if resolution.target_choice_name is not None:
                                by_choice_name[resolution.target_choice_name].append(resolution)
                        for target_choice_name, group in by_choice_name.items():
                            survivor_choice_id = existing_target_choice_ids.get(target_choice_name)
                            if survivor_choice_id is None:
                                survivor_choice_id = min(group, key=lambda item: item.choice_id).choice_id
                            for resolution in group:
                                applied_updates += 1
                                _log_progress(
                                    "Applying choice updates",
                                    applied_updates,
                                    total_updates,
                                    args.progress_every,
                                )
                                _merge_choice_into_survivor(connection, target_market_id, resolution, survivor_choice_id)
                    logger.info("Checking %d source markets after backfill", len(source_market_ids))
                    for index, market_id in enumerate(source_market_ids, start=1):
                        _log_progress(
                            "Checking source markets after backfill",
                            index,
                            len(source_market_ids),
                            args.progress_every,
                        )
                        _count_rows(
                            connection,
                            "SELECT COUNT(*) AS count FROM market_choices WHERE market_id = :market_id",
                            {"market_id": market_id},
                        )
                    empty_markets = _count_source_markets_that_would_become_empty(markets, resolutions)
                    csv_outputs: list[str] = []
                    resume = _build_resume(markets, specs, resolutions, market_map, 10)
                    summary = {
                        "mode": mode,
                        "output_dir": str(output_dir),
                        "bookie_id": args.bookie_id,
                        "sport": args.sport,
                        "event_id": args.event_id,
                        "market_id": args.market_id,
                        "include_draw_no_bet": args.include_draw_no_bet,
                        "include_btts": args.include_btts,
                        "include_handicap_split": args.include_handicap_split,
                        "total_candidate_markets": len(markets),
                        "total_candidate_choices": sum(len(choices_by_market.get(m.market_id, [])) for m in markets),
                        "resolvable_choices": sum(1 for item in resolutions if item.status == "resolved"),
                        "unresolved_choices": sum(1 for item in resolutions if item.status != "resolved"),
                        "choice_name_changes": resume["choice_name_changes"],
                        "choice_group_changes": resume["choice_group_changes"],
                        "market_moves": resume["market_moves"],
                        "changed_choice_count": resume["changed_choice_count"],
                        "simple_renames": resume["choice_name_changes"],
                        "group_updates": sum(1 for item in resolutions if item.status == "resolved" and item.action_type == "group_update"),
                        "move_actions": resume["market_moves"],
                        "merges_needed": len(merge_actions),
                        "target_markets_to_create": sum(1 for item in specs if item.existing_market_id is None),
                        "old_markets_that_would_become_empty": empty_markets,
                        "validation_errors": validation_errors,
                        "top_unresolved_examples": _unresolved_rows(resolutions)[:10],
                        "context_csvs": context_csvs,
                        "csv_outputs": csv_outputs,
                        "post_validation": None,
                        "choice_resolutions_count": len(resolutions),
                        "choice_resolutions_sample_limit": 100,
                        "choice_resolutions_sample": [asdict(item) for item in resolutions[:100]],
                        "choice_resolutions_truncated": len(resolutions) > 100,
                        "resume": resume,
                    }
                    post_validation = _validate_post_commit(
                        connection,
                        touched_market_ids,
                        event_ids,
                        before_snapshot_count,
                        before_trajectory_count,
                        sum(
                            1
                            for item in resolutions
                            if item.status == "resolved" and item.action_type != "noop"
                        ),
                    )
                    outputs = {
                        "choice_backfill_plan.csv": _bucket_rows(resolutions),
                        "choice_backfill_changed_choices.csv": _changed_choice_rows(resolutions),
                        "choice_backfill_unresolved.csv": _unresolved_rows(resolutions),
                        "choice_backfill_merge_plan.csv": [asdict(item) for item in merge_actions],
                        "choice_backfill_target_markets.csv": [asdict(item) for item in specs],
                        "choice_backfill_summary_by_market_shape.csv": summary_rows,
                    }
                    for filename, rows_out in outputs.items():
                        path = output_dir / filename
                        _write_csv(path, rows_out)
                        csv_outputs.append(str(path))
                    if summary is not None:
                        summary["post_validation"] = post_validation
                    _print_summary(summary)
                    if args.output_json:
                        print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True, default=str))
                    return 0, summary

        with progress_step("Opening read-only connection"):
            with db_manager.engine.connect() as connection:
                with progress_step("Validating schema"):
                    validate_required_schema(connection)
                with progress_step("Validating canonical targets"):
                    validate_canonical_targets(connection)
                with progress_step("Loading candidate rows"):
                    rows = _load_candidate_rows(connection, args)
                    markets, choices_by_market, validation_errors = _build_market_contexts(
                        rows,
                        args.limit,
                        args.progress_every,
                    )
                resolutions, buckets, summary_rows = _build_plan(
                    markets,
                    choices_by_market,
                    args.progress_every,
                )
                with progress_step("Merging target specs"):
                    specs = _merge_specs(buckets.keys(), args.progress_every)
                with progress_step("Annotating existing target markets"):
                    specs = _annotate_existing_target_markets(connection, specs, args.progress_every)
                merge_actions = _merge_actions_from_buckets(buckets, {})
                empty_markets = _count_source_markets_that_would_become_empty(markets, resolutions)
                csv_outputs = []
                market_map = {market.market_id: market for market in markets}
                outputs = {
                    "choice_backfill_plan.csv": _bucket_rows(resolutions),
                    "choice_backfill_changed_choices.csv": _changed_choice_rows(resolutions),
                    "choice_backfill_unresolved.csv": _unresolved_rows(resolutions),
                    "choice_backfill_merge_plan.csv": [asdict(item) for item in merge_actions],
                    "choice_backfill_target_markets.csv": [asdict(item) for item in specs],
                    "choice_backfill_summary_by_market_shape.csv": summary_rows,
                }
                for filename, rows_out in outputs.items():
                    path = output_dir / filename
                    _write_csv(path, rows_out)
                    csv_outputs.append(str(path))
                resume = _build_resume(markets, specs, resolutions, market_map, 10)
                summary = {
                    "mode": mode,
                    "output_dir": str(output_dir),
                    "bookie_id": args.bookie_id,
                    "sport": args.sport,
                    "event_id": args.event_id,
                    "market_id": args.market_id,
                    "include_draw_no_bet": args.include_draw_no_bet,
                    "include_btts": args.include_btts,
                    "include_handicap_split": args.include_handicap_split,
                    "total_candidate_markets": len(markets),
                    "total_candidate_choices": sum(len(choices_by_market.get(m.market_id, [])) for m in markets),
                    "resolvable_choices": sum(1 for item in resolutions if item.status == "resolved"),
                    "unresolved_choices": sum(1 for item in resolutions if item.status != "resolved"),
                    "choice_name_changes": resume["choice_name_changes"],
                    "choice_group_changes": resume["choice_group_changes"],
                    "market_moves": resume["market_moves"],
                    "changed_choice_count": resume["changed_choice_count"],
                    "simple_renames": resume["choice_name_changes"],
                    "group_updates": sum(1 for item in resolutions if item.status == "resolved" and item.action_type == "group_update"),
                    "move_actions": resume["market_moves"],
                    "merges_needed": len(merge_actions),
                    "target_markets_to_create": sum(1 for item in specs if item.existing_market_id is None),
                    "old_markets_that_would_become_empty": empty_markets,
                    "validation_errors": validation_errors,
                    "top_unresolved_examples": _unresolved_rows(resolutions)[:10],
                    "context_csvs": context_csvs,
                    "csv_outputs": csv_outputs,
                    "choice_resolutions_count": len(resolutions),
                    "choice_resolutions_sample_limit": 100,
                    "choice_resolutions_sample": [asdict(item) for item in resolutions[:100]],
                    "choice_resolutions_truncated": len(resolutions) > 100,
                    "resume": resume,
                }
                _print_summary(summary)
                if args.output_json:
                    print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True, default=str))
                return 0, summary
    except Exception as exc:
        if summary is not None:
            _print_summary(summary)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1, None


def main(argv: Sequence[str] | None = None) -> int:
    setup_logging()
    args = parse_args(argv)
    return run(args)[0]


if __name__ == "__main__":
    raise SystemExit(main())
