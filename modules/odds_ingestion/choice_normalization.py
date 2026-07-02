"""Canonical choice normalization, isolated from provider adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ChoiceNormalizationContext:
    market_family: str
    canonical_market_key: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    choice_group: str | None = None
    choice_index: int | None = None
    total_choices: int | None = None


@dataclass(frozen=True)
class ChoiceNormalizationResult:
    resolved: bool
    canonical_choice_name: str | None = None
    choice_group: str | None = None
    reason: str | None = None


class ChoiceNormalizer:
    _PREFIXED_CHOICE = re.compile(r"^\(\s*([-+]?\d+(?:\.\d+)?)\s*\)\s*(.+)$")

    @staticmethod
    def _clean_team_name(name: str) -> str:
        if not name:
            return ""
        # Convertir a minúsculas, remover acentos básicos, espacios, guiones y marcas comunes
        cleaned = name.strip().lower()
        cleaned = re.sub(r"[áäâà]", "a", cleaned)
        cleaned = re.sub(r"[éëêè]", "e", cleaned)
        cleaned = re.sub(r"[íïîì]", "i", cleaned)
        cleaned = re.sub(r"[óöôò]", "o", cleaned)
        cleaned = re.sub(r"[úüûù]", "u", cleaned)
        cleaned = re.sub(r"[\s\-\_\.\,\(\)]+", "", cleaned)
        # Remover marcas y sufijos comunes de equipos
        for marker in ("fc", "cf", "ud", "ca", "if", "sc", "afc", "club"):
            if cleaned.endswith(marker):
                cleaned = cleaned[:-len(marker)]
            if cleaned.startswith(marker):
                cleaned = cleaned[len(marker):]
        return cleaned

    @staticmethod
    def _team_side(value: str, context: ChoiceNormalizationContext) -> str | None:
        token = value.strip().casefold()
        if token in {"1", "2"}:
            return token
        # Limpieza básica para resolver slugs y discrepancias
        token_clean = ChoiceNormalizer._clean_team_name(value)
        if not token_clean:
            return None
        if context.home_team and token_clean == ChoiceNormalizer._clean_team_name(context.home_team):
            return "1"
        if context.away_team and token_clean == ChoiceNormalizer._clean_team_name(context.away_team):
            return "2"
        return None

    @staticmethod
    def _resolve_double_chance(token: str, context: ChoiceNormalizationContext) -> str | None:
        canonical = {"1x": "1x", "x2": "x2", "12": "12"}.get(token)
        if canonical:
            return canonical
        if " or " in token:
            parts = [p.strip() for p in token.split(" or ")]
            if len(parts) == 2:
                p1, p2 = parts[0], parts[1]
                if p1 == "draw" or p2 == "draw":
                    team_label = p2 if p1 == "draw" else p1
                    side = ChoiceNormalizer._team_side(team_label, context)
                    if side:
                        return "1x" if side == "1" else "x2"
                else:
                    side1 = ChoiceNormalizer._team_side(p1, context)
                    side2 = ChoiceNormalizer._team_side(p2, context)
                    if side1 and side2 and {side1, side2} == {"1", "2"}:
                        return "12"
        return None

    @staticmethod
    def normalize_choice_name(raw_choice_name: str, context: ChoiceNormalizationContext) -> ChoiceNormalizationResult:
        raw = str(raw_choice_name or "").strip()
        if not raw:
            return ChoiceNormalizationResult(False, reason="missing_choice_name")

        choice_group = context.choice_group
        value = raw
        match = ChoiceNormalizer._PREFIXED_CHOICE.match(raw)
        if match:
            choice_group = match.group(1).strip() or choice_group
            value = match.group(2).strip()

        token = value.casefold()
        family = context.market_family
        canonical = None
        if family == "side_3way":
            canonical = {"1": "1", "x": "x", "draw": "x", "2": "2"}.get(token)
        elif family in {"side_2way", "spread_2way"}:
            canonical = ChoiceNormalizer._team_side(value, context)
            if family == "spread_2way" and canonical and choice_group:
                try:
                    line_val = float(choice_group)
                    # CONVENCIÓN DE SPREAD / HANDICAP:
                    # En mercados spread_2way (como Asian Handicap / Point Spread), guardamos un único
                    # mercado en la base de datos cuya línea de referencia (choice_group) corresponde al
                    # equipo local (Home, canonical == "1").
                    # La línea del equipo visitante (Away, canonical == "2") es siempre el inverso matemático
                    # de la del local (-line_val).
                    # Por lo tanto, al normalizar la choice del equipo visitante ("2"), invertimos el signo 
                    # para que ambas choices ("1" y "2") coincidan en la misma línea del mercado local.
                    if canonical == "2":
                        line_val = -line_val
                    if line_val == 0:
                        choice_group = "0"
                    elif line_val.is_integer():
                        choice_group = str(int(line_val))
                    else:
                        choice_group = str(line_val).rstrip("0").rstrip(".")
                except (TypeError, ValueError):
                    pass
        elif family in {"total", "team_total"}:
            canonical = {"over": "over", "under": "under"}.get(token)
        elif family == "side_combo":
            canonical = ChoiceNormalizer._resolve_double_chance(token, context)
        elif family == "decision":
            canonical = {"yes": "yes", "no": "no"}.get(token)
        elif family == "goal_team":
            canonical = {"1": "1", "2": "2", "no goal": "no_goal", "no_goal": "no_goal", "no": "no_goal"}.get(token)
            canonical = canonical or ChoiceNormalizer._team_side(value, context)

        if canonical is None:
            if context.choice_index is not None and context.total_choices is not None:
                canonical = ChoiceNormalizer._fallback_by_position(family, context.choice_index, context.total_choices)
                if canonical:
                    # En mercados de spread, ajustar el signo si la elección resultante es "2".
                    if family == "spread_2way" and choice_group:
                        try:
                            line_val = float(choice_group)
                            if canonical == "2":
                                line_val = -line_val
                            if line_val == 0:
                                choice_group = "0"
                            elif line_val.is_integer():
                                choice_group = str(int(line_val))
                            else:
                                choice_group = str(line_val).rstrip("0").rstrip(".")
                        except (TypeError, ValueError):
                            pass

        if canonical is None:
            return ChoiceNormalizationResult(False, choice_group=choice_group, reason="unsupported_choice_for_market_family")
        return ChoiceNormalizationResult(True, canonical, choice_group)

    @staticmethod
    def _fallback_by_position(family: str, index: int, total: int) -> str | None:
        """
        Fallback positional mapping based on SofaScore odds response dominant patterns.
        Used as last resort when textual / team name matching fails.
        """
        if family == "side_3way" and total == 3:
            return {0: "1", 1: "x", 2: "2"}.get(index)
        if family in {"side_2way", "spread_2way"} and total == 2:
            return {0: "1", 1: "2"}.get(index)
        if family in {"total", "team_total"} and total == 2:
            return {0: "over", 1: "under"}.get(index)
        if family == "side_combo" and total == 3:
            return {0: "1x", 1: "x2", 2: "12"}.get(index)
        if family == "decision" and total == 2:
            return {0: "yes", 1: "no"}.get(index)
        if family == "goal_team" and total == 3:
            return {0: "1", 1: "no_goal", 2: "2"}.get(index)
        return None
