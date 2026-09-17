from modules.oddsportal.oddsportal_tab_normalizer import (
    get_group_tab_candidates,
    get_period_tab_candidates,
    normalize_tab_label,
    tab_label_matches,
)


def test_normalize_tab_label_removes_accents_and_collapses_whitespace():
    assert normalize_tab_label("Final del partido \nincluyendo prórroga") == "final del partido incluyendo proroga"


def test_period_candidates_match_spanish_half():
    assert tab_label_matches(
        "1er tiempo",
        get_period_tab_candidates("1ST_HALF", "1st Half", "es"),
    )


def test_group_candidates_match_spanish_over_under():
    assert tab_label_matches(
        "Más/Menos de",
        get_group_tab_candidates("OVER_UNDER", "Over/Under", "es"),
    )


def test_group_candidates_match_spanish_asian_handicap():
    assert tab_label_matches(
        "Hándicap asiático",
        get_group_tab_candidates("ASIAN_HANDICAP", "Asian Handicap", "es"),
    )


def test_period_candidates_match_spanish_ft_including_ot():
    assert tab_label_matches(
        "Final del partido incluyendo prórroga",
        get_period_tab_candidates("FT_INC_OT", "Full Time", "es"),
    )


def test_period_candidates_match_spanish_full_time():
    assert tab_label_matches(
        "Final del partido",
        get_period_tab_candidates("FULL_TIME", "Full Time", "es"),
    )


def test_full_time_does_not_match_ft_including_ot():
    assert not tab_label_matches(
        "Final del partido incluyendo prórroga",
        get_period_tab_candidates("FULL_TIME", "Full Time", "es"),
    )


def test_ft_including_ot_does_not_match_full_time_only():
    assert not tab_label_matches(
        "Final del partido",
        get_period_tab_candidates("FT_INC_OT", "Full Time", "es"),
    )
