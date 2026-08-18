"""Resolve cached OddsPapi mainLine outcome ids per bookmaker.

Cache rows are stored with ``bookmaker_slug``, but OddsPapi outcome ids are
shared across books. Historical persist therefore prefers a bookmaker's own
cached ids and, if that book has none, walks a configurable fallback list.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence


def _normalize_slug(value: object) -> str:
    return str(value or "").strip().lower()


def _normalize_ids(values: Collection[object] | None) -> set[str]:
    return {
        str(item).strip()
        for item in (values or ())
        if str(item).strip()
    }


def normalize_mainline_ids_by_bookmaker(
    cache_by_bookmaker: Mapping[str, Collection[object]] | None,
) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for slug, outcome_ids in (cache_by_bookmaker or {}).items():
        normalized_slug = _normalize_slug(slug)
        ids = _normalize_ids(outcome_ids)
        if not normalized_slug or not ids:
            continue
        grouped.setdefault(normalized_slug, set()).update(ids)
    return grouped


def resolve_mainline_outcome_ids(
    bookmaker_slug: str,
    cache_by_bookmaker: Mapping[str, Collection[object]] | None,
    fallback_priority: Sequence[str] | None,
) -> tuple[set[str], str | None]:
    """Return ``(outcome_ids, source_slug)`` for one bookmaker.

    ``source_slug`` is the cache donor: the book itself, or the first fallback
    that has rows. Both are empty when nothing can be used.
    """
    cache = normalize_mainline_ids_by_bookmaker(cache_by_bookmaker)
    slug = _normalize_slug(bookmaker_slug)
    if slug:
        own_ids = cache.get(slug) or set()
        if own_ids:
            return set(own_ids), slug

    for candidate in fallback_priority or ():
        fallback_slug = _normalize_slug(candidate)
        if not fallback_slug or fallback_slug == slug:
            continue
        fallback_ids = cache.get(fallback_slug) or set()
        if fallback_ids:
            return set(fallback_ids), fallback_slug
    return set(), None
