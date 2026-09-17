"""Auto-link policy regressions for OddsPapi candidate matching."""

from __future__ import annotations

from modules.oddspapi.event_candidate_matcher import (
    EventCandidateScore,
    OddspapiEventCandidateMatcher,
)


def _candidate(**overrides) -> EventCandidateScore:
    base = dict(
        event_id=1,
        score=0.95,
        orientation="ordered",
        start_time_delta_minutes=0.0,
        sport_score=1.0,
        time_score=1.0,
        participant1_score=1.0,
        participant2_score=1.0,
        participants_score=1.0,
        tournament_score=1.0,
        both_teams_strong=True,
    )
    base.update(overrides)
    return EventCandidateScore(**base)


class TestTeamsAcceptableForAutoLink:
    matcher = OddspapiEventCandidateMatcher

    def test_strict_both_teams_strong_still_passes(self):
        candidate = _candidate(
            participant1_score=0.82,
            participant2_score=0.82,
            both_teams_strong=True,
        )
        assert self.matcher._teams_acceptable_for_auto_link(candidate) is True

    def test_asymmetric_names_relaxed_when_anchor_and_tournament_strong(self):
        # Case 1: Weston Bears vs Weston Workers Reserves
        candidate = _candidate(
            score=0.945,
            participant1_score=0.98,
            participant2_score=0.74,
            participants_score=0.86,
            tournament_score=1.0,
            both_teams_strong=False,
        )
        assert self.matcher._teams_acceptable_for_auto_link(candidate) is True

    def test_near_miss_team_threshold_relaxed_with_strong_context(self):
        # Case 3: Wuhan Jiangda vs Wuhan Chegu Jiangda (0.81 vs 0.82 gate)
        candidate = _candidate(
            score=0.963,
            participant1_score=0.81,
            participant2_score=1.0,
            participants_score=0.91,
            tournament_score=1.0,
            both_teams_strong=False,
        )
        assert self.matcher._teams_acceptable_for_auto_link(candidate) is True

    def test_weak_team_not_relaxed_without_strong_tournament(self):
        candidate = _candidate(
            participant1_score=0.98,
            participant2_score=0.74,
            tournament_score=0.40,
            both_teams_strong=False,
        )
        assert self.matcher._teams_acceptable_for_auto_link(candidate) is False

    def test_weak_team_not_relaxed_without_near_exact_time(self):
        candidate = _candidate(
            participant1_score=0.98,
            participant2_score=0.74,
            tournament_score=1.0,
            start_time_delta_minutes=20.0,
            time_score=0.70,
            both_teams_strong=False,
        )
        assert self.matcher._teams_acceptable_for_auto_link(candidate) is False


class TestStrongIdentityMatch:
    matcher = OddspapiEventCandidateMatcher

    def test_perfect_teams_and_time_bypass_tournament_mismatch(self):
        # Case 2: Sapphire Cup vs Women Club Friendly Games
        candidate = _candidate(
            score=0.902,
            participant1_score=1.0,
            participant2_score=1.0,
            participants_score=1.0,
            tournament_score=0.34,
            both_teams_strong=True,
        )
        assert self.matcher._is_strong_identity_match(candidate) is True

    def test_identity_still_requires_tournament_when_time_is_not_near_exact(self):
        candidate = _candidate(
            participant1_score=1.0,
            participant2_score=1.0,
            tournament_score=0.34,
            start_time_delta_minutes=20.0,
            time_score=0.70,
        )
        assert self.matcher._is_strong_identity_match(candidate) is False

    def test_classic_identity_path_with_tournament_alignment(self):
        candidate = _candidate(
            participant1_score=0.98,
            participant2_score=0.98,
            tournament_score=0.75,
            start_time_delta_minutes=12.0,
            time_score=0.85,
        )
        assert self.matcher._is_strong_identity_match(candidate) is True
