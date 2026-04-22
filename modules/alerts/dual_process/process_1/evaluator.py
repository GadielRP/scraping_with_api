"""Rule evaluation for Process 1."""

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from .candidate_search import AlertMatch

logger = logging.getLogger(__name__)

RULE_WEIGHTS = {"A": 4, "B": 3, "C": 2}
MAX_WEIGHT = 4
MIN_SAMPLES = 1

WINNER_NAMES = {
    "1": "Home",
    "X": "Draw",
    "2": "Away",
}

CONFIDENCE_LEVELS = {
    "identical": "high",
    "similar": "medium",
    "same_winning_side": "low",
}


@dataclass
class AlertPrediction:
    """Represents a prediction based on historical matches."""

    rule_type: str
    prediction: str
    winner_side: str
    point_diff: Optional[int]
    exact_score: Optional[str]
    sample_count: int
    confidence: str


class Process1Evaluator:
    """Encapsulates Process 1 rules, weighting and reporting helpers."""

    def evaluate_identical_results(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        return self._evaluate_rule(matches, "identical", lambda m: m.result_text, "exact score")

    def evaluate_similar_results(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        if not matches:
            return None

        winner_diff_groups = defaultdict(list)
        for match in matches:
            key = (match.winner_side, match.point_diff)
            winner_diff_groups[key].append(match)

        if not winner_diff_groups:
            return None

        most_common_pattern = max(winner_diff_groups.keys(), key=lambda k: len(winner_diff_groups[k]))
        most_common_matches = winner_diff_groups[most_common_pattern]
        if len(most_common_matches) < 2:
            return None

        sample_match = most_common_matches[0]
        prediction_text = self._create_prediction_text(sample_match, sample_match.point_diff, "similar")
        return AlertPrediction(
            rule_type="similar",
            prediction=prediction_text,
            winner_side=sample_match.winner_side,
            point_diff=sample_match.point_diff,
            exact_score=None,
            sample_count=len(most_common_matches),
            confidence="medium",
        )

    def evaluate_same_winning_side(self, matches: List[AlertMatch]) -> Optional[AlertPrediction]:
        if not matches:
            return None

        winner_groups = defaultdict(list)
        for match in matches:
            winner_groups[match.winner_side].append(match)

        if not winner_groups:
            return None

        most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
        most_common_matches = winner_groups[most_common_winner]
        if len(most_common_matches) < 2:
            return None

        point_diff = self.calculate_weighted_avg_point_diff(most_common_matches)
        sample_match = most_common_matches[0]
        prediction_text = self._create_prediction_text(sample_match, point_diff, "same_winning_side")
        return AlertPrediction(
            rule_type="same_winning_side",
            prediction=prediction_text,
            winner_side=sample_match.winner_side,
            point_diff=point_diff,
            exact_score=None,
            sample_count=len(most_common_matches),
            confidence="low",
        )

    def _evaluate_rule(
        self,
        matches: List[AlertMatch],
        rule_type: str,
        group_key_func,
        group_desc: str,
        use_weighted_avg: bool = False,
    ) -> Optional[AlertPrediction]:
        if not matches:
            return None

        groups = defaultdict(list)
        for match in matches:
            groups[group_key_func(match)].append(match)

        group_summary = ", ".join([f"{k}:{len(v)}" for k, v in groups.items()])
        logger.info(f"{rule_type.title()} rule grouping by {group_desc} -> {group_summary if group_summary else 'no groups'}")

        if len(groups) == 1:
            group_matches = list(groups.values())[0]
            sample_match = group_matches[0]
            point_diff = self.calculate_weighted_avg_point_diff(group_matches) if use_weighted_avg else sample_match.point_diff
            prediction_text = self._create_prediction_text(sample_match, point_diff, rule_type)

            return AlertPrediction(
                rule_type=rule_type,
                prediction=prediction_text,
                winner_side=sample_match.winner_side,
                point_diff=point_diff,
                exact_score=sample_match.result_text if rule_type == "identical" else None,
                sample_count=len(matches),
                confidence=CONFIDENCE_LEVELS[rule_type],
            )

        return None

    def create_mixed_prediction(
        self,
        candidates: List[AlertMatch],
        rule_type: str,
        match_count: int,
    ) -> Optional[AlertPrediction]:
        if rule_type == "identical":
            result_groups = {}
            for match in candidates:
                result_groups.setdefault(match.result_text, []).append(match)

            most_common_result = max(result_groups.keys(), key=lambda k: len(result_groups[k]))
            most_common_matches = result_groups[most_common_result]
            weighted_avg_point_diff = self.calculate_weighted_avg_point_diff_mixed(candidates)
            sample_match = most_common_matches[0]
            winner_name = WINNER_NAMES.get(sample_match.winner_side, "Unknown")

            prediction_text = (
                "Draw"
                if sample_match.winner_side == "X"
                else f"{winner_name} wins by point differential of: {weighted_avg_point_diff:.2f}"
            )

            return AlertPrediction(
                rule_type="identical",
                prediction=prediction_text,
                winner_side=sample_match.winner_side,
                point_diff=weighted_avg_point_diff,
                exact_score=most_common_result,
                sample_count=match_count,
                confidence="high",
            )

        if rule_type == "similar":
            return self.evaluate_similar_results(candidates)
        if rule_type == "same_winner":
            return self.evaluate_same_winning_side(candidates)
        return None

    def count_candidates_matching_rule(self, candidates: List[AlertMatch], rule_type: str) -> int:
        if not candidates:
            return 0

        if rule_type == "identical":
            result_groups = defaultdict(list)
            for match in candidates:
                result_groups[match.result_text].append(match)
            return sum(len(group) for group in result_groups.values() if len(group) >= 2)

        if rule_type == "similar":
            winner_diff_groups = defaultdict(list)
            for match in candidates:
                winner_diff_groups[(match.winner_side, match.point_diff)].append(match)
            if not winner_diff_groups:
                return 0
            largest_group_size = max(len(group) for group in winner_diff_groups.values())
            return largest_group_size if largest_group_size >= 2 else 0

        if rule_type == "same_winner":
            winner_groups = defaultdict(list)
            for match in candidates:
                winner_groups[match.winner_side].append(match)
            if not winner_groups:
                return 0
            most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
            most_common_count = len(winner_groups[most_common_winner])
            return most_common_count if most_common_count >= 2 else 0

        return 0

    def get_candidates_by_rule_tiers(self, candidates: List[AlertMatch]) -> Dict[str, List[AlertMatch]]:
        tier_candidates = {"A": [], "B": [], "C": []}
        assigned_candidates = set()

        tier_a_candidates = self.get_tier_a_candidates(candidates)
        tier_candidates["A"] = tier_a_candidates
        assigned_candidates.update(match.event_id for match in tier_a_candidates)

        remaining_after_a = [match for match in candidates if match.event_id not in assigned_candidates]
        tier_b_candidates = self.get_tier_b_candidates(candidates, remaining_after_a)
        tier_candidates["B"] = tier_b_candidates
        assigned_candidates.update(match.event_id for match in tier_b_candidates)

        remaining_after_b = [match for match in candidates if match.event_id not in assigned_candidates]
        tier_candidates["C"] = self.get_tier_c_candidates(candidates, remaining_after_b)
        return tier_candidates

    def get_tier_a_candidates(self, candidates: List[AlertMatch]) -> List[AlertMatch]:
        tier_a_candidates = []
        result_groups = defaultdict(list)
        for match in candidates:
            result_groups[match.result_text].append(match)
        for group in result_groups.values():
            if len(group) >= 2:
                tier_a_candidates.extend(group)
        return tier_a_candidates

    def get_tier_b_candidates(self, all_candidates: List[AlertMatch], remaining_candidates: List[AlertMatch]) -> List[AlertMatch]:
        if not remaining_candidates:
            return []

        all_winner_diff_groups = defaultdict(list)
        for match in all_candidates:
            all_winner_diff_groups[(match.winner_side, match.point_diff)].append(match)
        if not all_winner_diff_groups:
            return []

        most_common_winner_diff = max(all_winner_diff_groups.keys(), key=lambda k: len(all_winner_diff_groups[k]))
        if len(all_winner_diff_groups[most_common_winner_diff]) < 2:
            return []

        return [
            match
            for match in remaining_candidates
            if (match.winner_side, match.point_diff) == most_common_winner_diff
        ]

    def get_tier_c_candidates(self, all_candidates: List[AlertMatch], remaining_candidates: List[AlertMatch]) -> List[AlertMatch]:
        if not remaining_candidates:
            return []

        all_winner_groups = defaultdict(list)
        for match in all_candidates:
            all_winner_groups[match.winner_side].append(match)
        if not all_winner_groups:
            return []

        most_common_winner = max(all_winner_groups.keys(), key=lambda k: len(all_winner_groups[k]))
        if len(all_winner_groups[most_common_winner]) < 2:
            return []

        return [match for match in remaining_candidates if match.winner_side == most_common_winner]

    def get_rule_activations(self, candidates: List[AlertMatch]) -> Dict[str, Dict]:
        rule_activations = {}
        tier_candidates = self.get_candidates_by_rule_tiers(candidates)

        for tier, matches in tier_candidates.items():
            if matches:
                rule_activations[tier] = {
                    "count": len(matches),
                    "weight": RULE_WEIGHTS[tier],
                    "candidates": [
                        {
                            "event_id": match.event_id,
                            "participants": match.participants,
                            "result_text": match.result_text,
                            "winner_side": match.winner_side,
                            "point_diff": match.point_diff,
                        }
                        for match in matches
                    ],
                }

        return rule_activations

    def calculate_weighted_avg_point_diff_mixed(self, candidates: List[AlertMatch]) -> float:
        if not candidates:
            return 0

        tier_candidates = self.get_candidates_by_rule_tiers(candidates)
        total_weighted_diff = sum(
            match.point_diff * RULE_WEIGHTS[tier]
            for tier, matches in tier_candidates.items()
            for match in matches
        )
        total_weight = sum(len(matches) * RULE_WEIGHTS[tier] for tier, matches in tier_candidates.items())
        result = total_weighted_diff / total_weight if total_weight > 0 else 0
        return round(result, 6)

    def calculate_weighted_avg_point_diff(self, matches: List[AlertMatch]) -> float:
        total_weighted_diff = 0
        total_weight = 0
        weight = RULE_WEIGHTS["C"]

        for match in matches:
            total_weighted_diff += match.point_diff * weight
            total_weight += weight

        result = total_weighted_diff / total_weight if total_weight > 0 else 0
        return round(result, 6)

    def _create_prediction_text(self, match: AlertMatch, point_diff, rule_type: str) -> str:
        winner_name = WINNER_NAMES.get(match.winner_side, "Unknown")

        if match.winner_side == "X":
            return "Draw"
        if rule_type == "identical":
            if match.point_diff and match.point_diff > 0:
                return f"{winner_name} wins by point differential of: {match.point_diff}"
            return f"Exact score: {match.result_text}"
        if rule_type == "same_winning_side":
            return f"{winner_name} wins by point differential of: {point_diff:.2f}"
        return f"{winner_name} wins by point differential of: {point_diff}"

    def evaluate_candidates_with_new_logic(self, tier1_candidates: List[AlertMatch]) -> Dict:
        selected_tier = "Exact (Tier 1)"

        if not tier1_candidates:
            return {
                "status": "no_candidates",
                "selected_tier": None,
                "prediction": None,
                "confidence": 0,
            "successful_candidates": 0,
                "total_candidates": 0,
                "rule_activations": {},
                "tier1_candidates": tier1_candidates,
            }

        selected_candidates = tier1_candidates
        logger.info(f"[P1] Evaluating {len(selected_candidates)} exact candidates ({selected_tier})")

        rule_b_result = self.evaluate_similar_results(selected_candidates)
        rule_c_result = self.evaluate_same_winning_side(selected_candidates)

        tier_a_matches = self.count_candidates_matching_rule(selected_candidates, "identical")
        tier_b_matches = self.count_candidates_matching_rule(selected_candidates, "similar")
        tier_c_matches = self.count_candidates_matching_rule(selected_candidates, "same_winner")

        unique_matching_candidates = set()

        if tier_a_matches > 0:
            result_groups = defaultdict(list)
            for match in selected_candidates:
                result_groups[match.result_text].append(match)
            for group in result_groups.values():
                if len(group) >= 2:
                    unique_matching_candidates.update(match.event_id for match in group)

        if tier_b_matches > 0:
            winner_diff_groups = defaultdict(list)
            for match in selected_candidates:
                winner_diff_groups[(match.winner_side, match.point_diff)].append(match)
            if winner_diff_groups:
                largest_group = max(winner_diff_groups.values(), key=len)
                if len(largest_group) >= 2:
                    unique_matching_candidates.update(match.event_id for match in largest_group)

        if tier_c_matches > 0:
            winner_groups = defaultdict(list)
            for match in selected_candidates:
                winner_groups[match.winner_side].append(match)
            if winner_groups:
                most_common_winner = max(winner_groups.keys(), key=lambda k: len(winner_groups[k]))
                most_common_matches = winner_groups[most_common_winner]
                if len(most_common_matches) >= 2:
                    unique_matching_candidates.update(match.event_id for match in most_common_matches)

        total_matching_candidates = len(unique_matching_candidates)
        prediction_result = None

        if len(selected_candidates) == 0:
            status = "no_candidates"
            successful_candidates = 0
            total_candidates = 0
            confidence = 0
            logger.info(f"[DEBUG] Status: {status} (all candidates filtered out)")
        elif len(selected_candidates) == 1:
            status = "partial"
            confidence = 0
            successful_candidates = 1
            total_candidates = 1
            logger.info(f"[DEBUG] Status: {status} (single candidate - insufficient for prediction)")
        elif total_matching_candidates == len(selected_candidates):
            tier_assignments = self.get_candidates_by_rule_tiers(selected_candidates)
            all_activated_winner_sides = set()
            for tier_matches in tier_assignments.values():
                for match in tier_matches:
                    all_activated_winner_sides.add(match.winner_side)

            if len(all_activated_winner_sides) > 1:
                status = "no_match"
                confidence = 0
                successful_candidates = 0
                total_candidates = len(selected_candidates)
                conflict_sides = ", ".join(WINNER_NAMES.get(side, side) for side in sorted(all_activated_winner_sides))
                logger.info(
                    f"[CONFLICT] Activated tiers disagree on winner side ({conflict_sides}) - downgrading to no_match"
                )
            else:
                weighted_successes = (
                    len(tier_assignments["A"]) * RULE_WEIGHTS["A"]
                    + len(tier_assignments["B"]) * RULE_WEIGHTS["B"]
                    + len(tier_assignments["C"]) * RULE_WEIGHTS["C"]
                )
                max_possible_weight = len(selected_candidates) * MAX_WEIGHT
                confidence = (weighted_successes / max_possible_weight) * 100 if max_possible_weight > 0 else 0
                confidence = round(confidence, 1)

                if tier_a_matches > 0:
                    prediction_result = self.create_mixed_prediction(selected_candidates, "identical", tier_a_matches)
                    logger.info(f"[RULE A] {tier_a_matches}/{len(selected_candidates)} candidates have identical results")
                elif tier_b_matches > 0:
                    prediction_result = rule_b_result
                    logger.info(f"[RULE B] {tier_b_matches}/{len(selected_candidates)} candidates have similar results")
                elif tier_c_matches > 0:
                    prediction_result = rule_c_result
                    logger.info(f"[RULE C] {tier_c_matches}/{len(selected_candidates)} candidates have same winning side")

                successful_candidates = len(selected_candidates)
                total_candidates = len(selected_candidates)

                if prediction_result is not None:
                    status = "success"
                    logger.info(f"[DEBUG] Status: {status}")
                else:
                    status = "partial"
                    confidence = 0
                    logger.info(f"[DEBUG] Status: {status}")
        else:
            status = "no_match"
            successful_candidates = total_matching_candidates
            total_candidates = len(selected_candidates)
            confidence = 0
            logger.info(f"[DEBUG] Status: {status} (candidates failed rules)")

        return {
            "status": status,
            "selected_tier": selected_tier,
            "prediction": prediction_result,
            "confidence": confidence,
            "successful_candidates": successful_candidates,
            "total_candidates": total_candidates,
            "rule_activations": self.get_rule_activations(selected_candidates),
            "tier1_candidates": tier1_candidates,
        }


__all__ = [
    "AlertPrediction",
    "CONFIDENCE_LEVELS",
    "MAX_WEIGHT",
    "MIN_SAMPLES",
    "Process1Evaluator",
    "RULE_WEIGHTS",
    "WINNER_NAMES",
]
