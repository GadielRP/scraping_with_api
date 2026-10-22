"""Audit line ambiguity, export offline corrections, and apply without debug files.

See docs/operations/oddspapi_line_backfill.md for the local/server workflow.
Only this maintenance script owns repair writes; production selection is reused.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
import csv
from datetime import datetime
from decimal import Decimal
import hashlib
import json
import logging
from pathlib import Path
import re

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from infrastructure.persistence.backfill.checkpoint import (
    build_manifest, read_json, validate_manifest, write_json_atomic,
)
from infrastructure.persistence.backfill.runner import BackfillRunner
from infrastructure.persistence.backfill.strategy import BackfillConflict
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    BookieSourceMapping, CanonicalMarketType, Event, EventSourceMapping, Market,
    MarketChoice, MarketChoiceQuote, MarketChoiceSnapshot, OddspapiMainlineOutcomeCache,
)
from infrastructure.persistence.repositories.market_mapping_repository import MarketMappingRepository
from infrastructure.persistence.repositories.oddspapi_mainline_cache_repository import OddspapiMainlineCacheRepository
from infrastructure.persistence.repositories.reporting_refresh_repository import invalidate_reporting
from infrastructure.settings import Config
from modules.jobs.pre_start_check_job.providers.oddspapi.mainline_outcome_extractor import OddspapiMainlineOutcomeExtractor
from modules.jobs.pre_start_check_job.providers.oddspapi.settings import ODDSPAPI_PRE_START_SETTINGS
from modules.oddspapi.mainline_cache_ids import resolve_mainline_outcome_ids
from modules.odds_ingestion.oddspapi_line_selection import select_current_lines

STRATEGY = "oddspapi_cached_line_repair_v1"


def _json(value):
    return json.loads(json.dumps(value, default=str))


def _hash(value):
    return hashlib.sha256(json.dumps(_json(value), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _columns(row, *, exclude=()):
    return _json({c.name: getattr(row, c.name) for c in row.__table__.columns if c.name not in exclude})


def _files(root):
    result = defaultdict(list)
    for path in sorted(Path(root).rglob("*.json")):
        match = re.match(r"^(\d+)_", path.name)
        if match and "_historical" not in path.name:
            result[int(match[1])].append(path)
    return result


def _family_key(bookie_id, market_type_id, is_live):
    return f"{bookie_id}:{market_type_id}:{int(is_live)}"


def _load_event(session, event_id, *, lock=False):
    """Load one bounded event, including material history for conflict checks."""
    if lock:
        session.query(Event).filter_by(id=event_id).with_for_update().all()
        session.query(Market).filter_by(event_id=event_id).order_by(Market.market_id).with_for_update().all()
        session.query(MarketChoice).join(Market).filter(Market.event_id == event_id).order_by(MarketChoice.choice_id).with_for_update(of=MarketChoice).all()
        session.query(MarketChoiceQuote).join(MarketChoice).join(Market).filter(Market.event_id == event_id).order_by(MarketChoiceQuote.quote_id).with_for_update(of=MarketChoiceQuote).all()
        session.query(OddspapiMainlineOutcomeCache).filter_by(event_id=event_id).with_for_update().all()
    event = session.get(Event, event_id)
    if event is None:
        raise BackfillConflict(f"event {event_id} is missing")
    fixtures = sorted(session.scalars(select(EventSourceMapping.source_event_id).where(
        EventSourceMapping.event_id == event_id, EventSourceMapping.source == "oddspapi",
    )).all())
    slugs = dict(session.execute(select(BookieSourceMapping.bookie_id, BookieSourceMapping.source_bookie_slug).where(
        BookieSourceMapping.source == "oddspapi",
    )).all())
    cache = [_columns(c, exclude=("cache_id",)) for c in session.query(OddspapiMainlineOutcomeCache).filter_by(event_id=event_id)
             .order_by(OddspapiMainlineOutcomeCache.bookmaker_slug, OddspapiMainlineOutcomeCache.source_market_id,
                       OddspapiMainlineOutcomeCache.source_outcome_id).all()]
    families = {}
    markets = session.query(Market).join(CanonicalMarketType).filter(
        Market.event_id == event_id, CanonicalMarketType.requires_line_value.is_(True),
    ).options(
        selectinload(Market.canonical_market_type), selectinload(Market.bookie),
        selectinload(Market.choices).selectinload(MarketChoice.quotes).selectinload(MarketChoiceQuote.snapshots),
    ).order_by(Market.market_id).all()
    for market in markets:
        if not any(q.source == "oddspapi" for c in market.choices for q in c.quotes):
            continue
        key = _family_key(market.bookie_id, market.market_type_id, market.is_live)
        slug = str(slugs.get(market.bookie_id) or market.bookie.slug).lower()
        family = families.setdefault(key, {
            "event_id": event_id, "bookie_id": market.bookie_id, "bookmaker_slug": slug,
            "market_type_id": market.market_type_id, "is_live": market.is_live,
            "canonical_market_key": market.canonical_market_type.canonical_market_key,
            "markets": [], "cache": sorted(
                [c for c in cache if c["bookmaker_slug"] == slug and c["canonical_market_key"] == market.canonical_market_type.canonical_market_key],
                key=lambda c: (c["source_market_id"], c["source_outcome_id"]),
            ),
        })
        item = _columns(market)
        item["choices"] = []
        for choice in sorted(market.choices, key=lambda c: c.choice_id):
            entry = _columns(choice)
            entry["quotes"] = []
            for quote in sorted(choice.quotes, key=lambda q: q.quote_id):
                qr = _columns(quote)
                qr["snapshots"] = [_columns(s) for s in sorted(quote.snapshots, key=lambda s: s.snapshot_id)]
                entry["quotes"].append(qr)
            item["choices"].append(entry)
        family["markets"].append(item)
    return {"event_id": event_id, "starts_at": str(event.starts_at), "fixtures": fixtures,
            "cache": cache, "families": families}


def _quotes(market):
    return [q for c in market["choices"] for q in c["quotes"] if q["source"] == "oddspapi"]


def _project(family, action):
    """Exact expected post-repair state, also used to recognize a repeat apply."""
    result = deepcopy(family)
    removed = set(action["delete_quote_ids"])
    flags = {int(k): v for k, v in action["main_line"].items()}
    for market in result["markets"]:
        for choice in market["choices"]:
            choice["quotes"] = [q for q in choice["quotes"] if q["quote_id"] not in removed]
            for q in choice["quotes"]:
                if q["quote_id"] in flags:
                    q["main_line"] = flags[q["quote_id"]]
        market["choices"] = [c for c in market["choices"] if c["quotes"]]
    # The state reader scopes markets to those still carrying OddspAPI quotes;
    # shells retained solely for another provider remain outside that scope.
    result["markets"] = [m for m in result["markets"] if _quotes(m)]
    if action["cache_after"] is not None:
        result["cache"] = action["cache_after"]
    return result


def audit(output, debug_root):
    """Report any true quote on multiple distinct lines, independently of depth."""
    files = _files(debug_root)
    q, mc, m, ct = MarketChoiceQuote, MarketChoice, Market, CanonicalMarketType
    with db_manager.get_session() as session:
        population = session.execute(select(
            func.count(func.distinct(m.event_id)), func.count(func.distinct(m.market_id)), func.count(q.quote_id),
        ).select_from(q).join(mc).join(m).where(q.source == "oddspapi")).one()
        rows = session.execute(select(
            m.event_id, m.bookie_id, m.market_type_id, m.is_live, m.market_id, m.line_value,
        ).join(ct).join(mc).join(q).where(
            ct.requires_line_value.is_(True), m.line_value.is_not(None),
            q.source == "oddspapi", q.main_line.is_(True),
        ).distinct()).all()
        groups = defaultdict(list)
        for row in rows:
            groups[(row.event_id, row.bookie_id, row.market_type_id, row.is_live)].append(row)
        bad = {k: v for k, v in groups.items() if len({r.line_value for r in v}) > 1}
        detail = []
        for event_id in sorted({k[0] for k in bad}):
            state = _load_event(session, event_id)
            event = session.get(Event, event_id)
            for key, family in state["families"].items():
                group = (event_id, family["bookie_id"], family["market_type_id"], family["is_live"])
                if group not in bad:
                    continue
                quotes = [q for market in family["markets"] for q in _quotes(market)]
                detail.append({
                    "event_id": event_id, "starts_at": state["starts_at"], "sport": event.sport,
                    "competition": event.competition, "matchup": f"{event.home_team} vs {event.away_team}",
                    "family": key, "bookmaker": family["bookmaker_slug"],
                    "canonical_market_key": family["canonical_market_key"], "is_live": family["is_live"],
                    "main_lines": [str(r.line_value) for r in sorted(bad[group], key=lambda r: r.line_value)],
                    "market_ids": [r.market_id for r in bad[group]],
                    "quotes": len(quotes), "snapshots": sum(len(q["snapshots"]) for q in quotes),
                    "quote_flags": [{k: q[k] for k in ("quote_id", "main_line", "source_market_id", "source_outcome_id", "exchange_side", "exchange_level")} for q in quotes],
                    "debug_files": [str(p) for p in files.get(event_id, [])], "cache": family["cache"],
                })
    summary = {
        "population_events": population[0], "population_markets": population[1], "population_quotes": population[2],
        "affected_events": len({r["event_id"] for r in detail}), "affected_families": len(detail),
        "mainline_markets": sum(len(r["main_lines"]) for r in detail),
        "excess_mainline_markets": sum(len(r["main_lines"]) - 1 for r in detail),
        "quotes_in_affected_families": sum(r["quotes"] for r in detail),
        "snapshots_in_affected_families": sum(r["snapshots"] for r in detail),
        "affected_events_with_debug": len({r["event_id"] for r in detail if r["debug_files"]}),
        "events_by_sport": dict(Counter(r["sport"] for r in {x["event_id"]: x for x in detail}.values())),
        "families_by_bookmaker": dict(Counter(r["bookmaker"] for r in detail)),
        "families_by_market": dict(Counter(r["canonical_market_key"] for r in detail)),
    }
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    write_json_atomic(output / "audit.json", {"summary": summary, "families": detail})
    with (output / "audit.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(detail[0]) if detail else ["event_id"])
        writer.writeheader()
        writer.writerows({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in r.items()} for r in detail)
    lines = ["# Auditoría de líneas OddspAPI", "", "Grupos: evento, casa, tipo canónico (incluye período), live.", "",
             "| Indicador | Valor |", "|---|---:|", *[f"| {k} | {v} |" for k, v in summary.items() if not isinstance(v, dict)], "",
             "Varias líneas principales indican ambigüedad; no demuestran por sí solas qué historial debe eliminarse.", "",
             "El detalle completo está en audit.csv y audit.json.", ""]
    for title in ("events_by_sport", "families_by_bookmaker", "families_by_market"):
        lines.extend([f"## {title}", "", *[f"- {k}: {v}" for k, v in summary[title].items()], ""])
    (output / "audit.md").write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return summary


def _captures(paths, state, index, require_active):
    captures, issues = [], []
    for path in paths:
        try:
            raw = path.read_bytes()
            payload = json.loads(raw)
            if "bookmakerOdds" not in payload:
                raise ValueError("not a current /odds payload")
            if str(payload.get("fixtureId")) not in state["fixtures"]:
                raise ValueError("fixture does not match canonical event mapping")
            moment = re.search(r"_t_(-?\d+)_", path.name)
            decisions, observed = {}, {}
            for slug, book in OddspapiMainlineOutcomeExtractor._entries(payload["bookmakerOdds"]):
                slug = str(book.get("slug") or slug).lower()
                selection = select_current_lines(book.get("markets", {}), market_mapping_index=index,
                    source_sport_id=payload.get("sportId"), require_active_quotes=require_active)
                for mid, market in OddspapiMainlineOutcomeExtractor._entries(book.get("markets", {})):
                    resolution = MarketMappingRepository.resolve_market(index, source="oddspapi",
                        source_sport_id=payload.get("sportId"), source_market_id=mid)
                    if not resolution.resolved or not resolution.requires_line_value or market.get("isLive", False):
                        continue
                    fk = (slug, resolution.market_type_id, False)
                    observed.setdefault(fk, {})[Decimal(resolution.source_handicap)] = (mid, market)
                    if mid in selection.selected_market_ids:
                        decisions[fk] = str(Decimal(resolution.source_handicap))
            outcomes = OddspapiMainlineOutcomeExtractor.extract(payload, market_mapping_index=index,
                require_active_quotes=require_active, exchange_bookmakers=["betfair-ex"])
            captures.append({"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                "moment": int(moment[1]) if moment else None, "payload": payload,
                "selected": decisions, "observed": observed, "outcomes": outcomes})
        except (ValueError, TypeError, KeyError) as exc:
            issues.append({"path": str(path), "reason": str(exc)})
    return captures, issues


def _action(family, state, captures, index, require_active):
    """Only delete rejected instruments tied to the same single observed batch."""
    fk = (family["bookmaker_slug"], family["market_type_id"], family["is_live"])
    relevant = [c for c in captures if fk in c["observed"]]
    good = [c for c in relevant if fk in c["selected"]]
    selected_before = {Decimal(c["selected"][fk]) for c in good}
    selected = None
    evidence = []
    latest = None
    if good:
        if len(relevant) > 1 and any(c["moment"] is None for c in relevant):
            return None, "ambiguous_capture_order"
        if len({c["moment"] for c in relevant}) != len(relevant):
            return None, "duplicate_capture_moment"
        latest = min(relevant, key=lambda c: c["moment"] if c["moment"] is not None else 0)
        if fk in latest["selected"]:
            selected = Decimal(latest["selected"][fk])
        evidence = [{k: c[k] for k in ("path", "sha256", "moment")} for c in relevant]
    mode = "debug"
    cached_ids, donor = resolve_mainline_outcome_ids(family["bookmaker_slug"],
        {slug: {c["source_outcome_id"] for c in state["cache"] if c["bookmaker_slug"] == slug} for slug in {c["bookmaker_slug"] for c in state["cache"]}},
        ODDSPAPI_PRE_START_SETTINGS.mainline_cache_fallback_bookmakers)
    if selected is None:
        lines = set()
        for row in state["cache"]:
            if row["bookmaker_slug"] != donor or row["source_outcome_id"] not in cached_ids:
                continue
            resolution = MarketMappingRepository.resolve_market(index, source="oddspapi",
                source_sport_id=row["source_sport_id"], source_market_id=row["source_market_id"])
            if resolution.resolved and resolution.market_type_id == family["market_type_id"] and resolution.source_handicap is not None:
                lines.add(Decimal(resolution.source_handicap))
        if len(lines) != 1:
            return None, "missing_or_ambiguous_selection"
        selected = next(iter(lines))
        mode = "cache_flags_only"
    keep = [m for m in family["markets"] if Decimal(m["line_value"]) == selected]
    if len(keep) != 1:
        return None, "selected_market_missing"
    keep = keep[0]
    keep_quotes = _quotes(keep)
    if mode == "debug":
        expected_ids = {r["source_outcome_id"] for r in latest["outcomes"] if r["bookmaker_slug"] == fk[0] and r["canonical_market_key"] == family["canonical_market_key"]}
    else:
        expected_ids = {c["source_outcome_id"] for c in state["cache"] if c["bookmaker_slug"] == donor and c["source_outcome_id"] in cached_ids and c["canonical_market_key"] == family["canonical_market_key"]}
    if not expected_ids or {q["source_outcome_id"] for q in keep_quotes} != expected_ids:
        return None, "selected_quotes_incomplete_or_different"
    keep_batches = {s["collected_at"] for q in keep_quotes for s in q["snapshots"]}
    if mode == "debug":
        # A later local/server acquisition cannot be classified from an older file.
        observed_current = latest["observed"][fk][selected][1]
        latest_ticks = {(str(oid), str(p.get("changedAt")), Decimal(str(p["price"])).quantize(Decimal(".001")))
            for oid, outcome in OddspapiMainlineOutcomeExtractor._entries(observed_current.get("outcomes", {}))
            for p in OddspapiMainlineOutcomeExtractor._players(outcome) if p.get("price") is not None}
        matching_batches = set()
        for q in keep_quotes:
            for s in q["snapshots"]:
                if any(oid == q["source_outcome_id"] and _same_time(t, s["source_collected_at"]) and price == Decimal(s["odds_value"])
                       for oid, t, price in latest_ticks):
                    matching_batches.add(s["collected_at"])
        family_batches = {s["collected_at"] for m in family["markets"] for q in _quotes(m) for s in q["snapshots"]}
        if (not matching_batches or not keep_batches or max(keep_batches) not in matching_batches
                or (family_batches and max(family_batches) > max(matching_batches))):
            return None, "capture_not_matched_to_latest_persisted_batch"
    action = {"family": _family_key(family["bookie_id"], family["market_type_id"], family["is_live"]),
        "mode": mode, "selected_line": str(selected), "keep_market_id": keep["market_id"],
        "historical_selected_lines": sorted(map(str, selected_before)), "evidence": evidence,
        "cache_donor": donor if mode == "cache_flags_only" else None,
        "cache_dependency": [c for c in state["cache"] if c["bookmaker_slug"] == donor]
            if mode == "cache_flags_only" else None,
        "delete_quote_ids": [], "delete_market_ids": [], "main_line": {}, "cache_after": None,
        "preserved_unproven_market_ids": []}
    for market in family["markets"]:
        line = Decimal(market["line_value"])
        quotes = _quotes(market)
        deletable = mode == "debug" and line not in selected_before
        observations = [(c, c["observed"][fk].get(line)) for c in relevant]
        observations = [(c, obs) for c, obs in observations if obs is not None]
        batches = {s["collected_at"] for q in quotes for s in q["snapshots"]}
        deletable = deletable and bool(observations) and len(batches) == 1 and batches.issubset(matching_batches)
        if deletable:
            batch = datetime.fromisoformat(next(iter(batches)))
            deletable = all(q["created_at"] and datetime.fromisoformat(q["created_at"]) >= batch for q in quotes)
        if deletable:
            # Every instrument must match an actual discarded provider tick.
            ticks = {(str(mid), str(oid), str(p.get("changedAt")), Decimal(str(p["price"])).quantize(Decimal(".001")))
                for c, (mid, raw_market) in observations
                for oid, outcome in OddspapiMainlineOutcomeExtractor._entries(raw_market.get("outcomes", {}))
                for p in OddspapiMainlineOutcomeExtractor._players(outcome) if p.get("price") is not None}
            deletable = all(q["exchange_side"] in (None, "back") and q["snapshots"] and all(
                any(mid == q["source_market_id"] and oid == q["source_outcome_id"] and _same_time(t, s["source_collected_at"])
                    and price == Decimal(s["odds_value"]) for mid, oid, t, price in ticks)
                for s in q["snapshots"]) for q in quotes)
        if deletable:
            action["delete_quote_ids"].extend(q["quote_id"] for q in quotes)
            if all(q["source"] == "oddspapi" for c in market["choices"] for q in c["quotes"]):
                action["delete_market_ids"].append(market["market_id"])
        else:
            for q in quotes:
                desired = line == selected
                if q["main_line"] is not desired:
                    action["main_line"][str(q["quote_id"])] = desired
            if line != selected and line not in selected_before:
                action["preserved_unproven_market_ids"].append(market["market_id"])
    if mode == "debug":
        rows = [r for r in latest["outcomes"] if r["bookmaker_slug"] == fk[0] and r["canonical_market_key"] == family["canonical_market_key"]]
        desired_ids = {(r["source_market_id"], r["source_outcome_id"]) for r in rows}
        if desired_ids != {(r["source_market_id"], r["source_outcome_id"]) for r in family["cache"]}:
            captured = datetime.fromisoformat(max(matching_batches))
            action["cache_after"] = sorted([
                _json(OddspapiMainlineCacheRepository._normalize_outcome_row(state["event_id"], latest["payload"]["fixtureId"],
                    str(latest["payload"]["sportId"]), row, captured_at=captured)) for row in rows
            ], key=lambda r: (r["source_market_id"], r["source_outcome_id"]))
    if not action["delete_quote_ids"] and not action["main_line"] and action["cache_after"] is None:
        return None, "already_correct"
    action["expected_sha256"] = _hash(family)
    action["after_sha256"] = _hash(_project(family, action))
    return action, "planned"


def _same_time(a, b):
    if not a or not b or a == "None":
        return False
    return datetime.fromisoformat(a.replace("Z", "+00:00")) == datetime.fromisoformat(b.replace("Z", "+00:00"))


def plan(output, debug_root, *, require_active):
    output = Path(output)
    if output.exists():
        raise FileExistsError(f"manifest already exists: {output}")
    files = _files(debug_root)
    index = MarketMappingRepository.build_index()
    events, skipped = [], []
    with db_manager.get_session() as session:
        event_ids = session.scalars(select(Market.event_id).join(MarketChoice).join(MarketChoiceQuote).where(
            MarketChoiceQuote.source == "oddspapi",
        ).distinct().order_by(Market.event_id)).all()
        for number, event_id in enumerate(event_ids, 1):
            state = _load_event(session, event_id)
            captures, issues = _captures(files.get(event_id, []), state, index, require_active)
            if issues:
                # An unreadable/mismatched capture may conceal a legitimate
                # earlier selection; cache fallback must never delete history.
                captures = []
            actions = []
            for key, family in state["families"].items():
                action, reason = _action(family, state, captures, index, require_active)
                if action:
                    actions.append(action)
                else:
                    skipped.append({"event_id": event_id, "family": key, "reason": reason})
            if issues:
                skipped.extend({"event_id": event_id, **issue} for issue in issues)
            if actions:
                events.append({"event_id": event_id, "starts_at": state["starts_at"],
                    "fixtures": state["fixtures"], "actions": actions})
            session.expunge_all()
            if number % 250 == 0:
                print(f"Scanned {number}/{len(event_ids)} events; planned={len(events)}", flush=True)
    manifest = build_manifest(strategy=STRATEGY, scope={"source": "oddspapi", "requires_line_value": True},
        parameters={"require_active_quotes": require_active, "preserve_prior_selections": True},
        events=sorted(events, key=lambda e: (e["starts_at"], e["event_id"])))
    write_json_atomic(output, manifest)
    summary = {"events_scanned": len(event_ids), "events_planned": len(events),
        "families_planned": sum(len(e["actions"]) for e in events),
        "quotes_to_delete": sum(len(a["delete_quote_ids"]) for e in events for a in e["actions"]),
        "markets_to_delete_if_empty": sum(len(a["delete_market_ids"]) for e in events for a in e["actions"]),
        "quote_flags_to_change": sum(len(a["main_line"]) for e in events for a in e["actions"]),
        "omissions": dict(Counter(r["reason"] for r in skipped)),
        "action_modes": dict(Counter(a["mode"] for e in events for a in e["actions"]))}
    write_json_atomic(output.with_name("plan_report.json"), {"summary": summary, "omissions": skipped})
    print(json.dumps(summary, ensure_ascii=False))
    return manifest


class LineRepair:
    """BackfillRunner's apply-only strategy; exported manifests need no raw files."""
    strategy_name = STRATEGY

    def apply_event(self, event_id, *, expected_state):
        with db_manager.get_session() as session:
            state = _load_event(session, event_id, lock=True)
            if state["starts_at"] != expected_state["starts_at"] or state["fixtures"] != expected_state["fixtures"]:
                raise BackfillConflict("event/fixture identity changed")
            pending = []
            for action in expected_state["actions"]:
                if action.get("cache_dependency") is not None:
                    dependency = [c for c in state["cache"] if c["bookmaker_slug"] == action["cache_donor"]]
                    if _hash(dependency) != _hash(action["cache_dependency"]):
                        raise BackfillConflict("cache donor changed")
                family = state["families"].get(action["family"])
                if family is None:
                    raise BackfillConflict(f"family missing: {action['family']}")
                digest = _hash(family)
                if digest == action["after_sha256"]:
                    continue
                if digest != action["expected_sha256"]:
                    raise BackfillConflict(f"quotes/history/cache changed for family {action['family']}")
                pending.append((family, action))
            for family, action in pending:
                removed = action["delete_quote_ids"]
                allowed = {q["quote_id"] for m in family["markets"] for q in _quotes(m)}
                if not set(removed).issubset(allowed) or not {int(k) for k in action["main_line"]}.issubset(allowed):
                    raise BackfillConflict("manifest action targets a different provider/family")
                if removed:
                    session.query(MarketChoiceSnapshot).filter(MarketChoiceSnapshot.quote_id.in_(removed)).delete(synchronize_session=False)
                    session.query(MarketChoiceQuote).filter(MarketChoiceQuote.quote_id.in_(removed), MarketChoiceQuote.source == "oddspapi").delete(synchronize_session=False)
                for quote_id, flag in action["main_line"].items():
                    session.query(MarketChoiceQuote).filter_by(quote_id=int(quote_id), source="oddspapi").update(
                        {"main_line": flag, "updated_at": MarketChoiceQuote.updated_at}, synchronize_session=False)
                market_ids = [m["market_id"] for m in family["markets"]]
                session.query(MarketChoice).filter(MarketChoice.market_id.in_(market_ids), ~MarketChoice.quotes.any()).delete(synchronize_session=False)
                session.query(Market).filter(Market.market_id.in_(market_ids), ~Market.choices.any()).delete(synchronize_session=False)
                if action["cache_after"] is not None:
                    session.query(OddspapiMainlineOutcomeCache).filter_by(event_id=event_id,
                        bookmaker_slug=family["bookmaker_slug"], canonical_market_key=family["canonical_market_key"]).delete(synchronize_session=False)
                    for row in action["cache_after"]:
                        data = dict(row)
                        data["captured_at"] = datetime.fromisoformat(data["captured_at"])
                        session.add(OddspapiMainlineOutcomeCache(**data))
            if pending:
                session.flush()
                session.expire_all()
                after = _load_event(session, event_id)
                for _, action in pending:
                    if _hash(after["families"].get(action["family"])) != action["after_sha256"]:
                        raise BackfillConflict(f"post-repair state mismatch for {action['family']}")
                invalidate_reporting(session)
        return {"event_id": event_id, "status": "APPLIED" if pending else "ALREADY_CORRECT",
            "families": len(pending), "quotes_deleted": sum(len(a["delete_quote_ids"]) for _, a in pending)}


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("audit", "plan"):
        p = sub.add_parser(name)
        p.add_argument("--debug-root", type=Path, default=Path("debug/oddspapi_odds_responses"))
        p.add_argument("--output", type=Path, required=True, help="Audit directory or plan manifest JSON")
        if name == "plan":
            p.add_argument("--require-active-quotes", action=argparse.BooleanOptionalAction,
                default=Config.ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES)
    p = sub.add_parser("apply")
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--checkpoint", type=Path, required=True)
    p.add_argument("--results", type=Path, required=True)
    p.add_argument("--expected-db-host", required=True)
    p.add_argument("--expected-db-port", type=int, required=True)
    p.add_argument("--expected-db-name", required=True)
    p.add_argument("--confirm-write", action="store_true", required=True)
    p.add_argument("--limit", type=int)
    args = parser.parse_args(argv)
    if args.command == "audit":
        audit(args.output, args.debug_root)
    elif args.command == "plan":
        plan(args.output, args.debug_root, require_active=args.require_active_quotes)
    else:
        url = db_manager.engine.url
        if (url.host, url.port or 5432, url.database) != (args.expected_db_host, args.expected_db_port, args.expected_db_name):
            raise ValueError("Database host/port/name do not match the explicitly selected destination")
        manifest = read_json(args.manifest)
        validate_manifest(manifest)
        if manifest["strategy"] != STRATEGY:
            raise ValueError("Not an OddspAPI line-repair manifest")
        return BackfillRunner(strategy=LineRepair(), manifest_path=args.manifest,
            checkpoint_path=args.checkpoint, results_path=args.results,
            lock_name=STRATEGY, refresh_views=False).apply(limit=args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
