"""Semantic readings must describe existing calculations, never replace them."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from datetime import timezone, datetime
from logging import getLogger
from importlib import import_module

import pytest

from modules.pillars.pillar_2_side_market.signal_engine import (
    _interpret_side_pair,
    _interpret_side_period,
)
from modules.pillars.pillar_3_totals_market_context.signal_engine import (
    _interpret_ou_pair,
)
from modules.pillars.pillar_4.trajectory_engine import (
    interpret_p4_result,
    _semantic_movement,
)
from modules.jobs.pre_start_check_job.pillar_result_logging import log_pillar_results
from modules.pillars.mining.adapters import (
    P2MiningAdapter,
    P3MiningAdapter,
    P4MiningAdapter,
    P5MiningAdapter,
)
from modules.pillars.mining.serialization import to_json_value
from modules.pillars.market_evaluation import FIRST_FIVE
from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult
from modules.pillars.pillar_2_side_market.signal_engine import interpret_p2_result
from modules.pillars.pillar_3_totals_market_context.signal_engine import (
    interpret_p3_result,
)
from modules.pillars.pillar_5.memory_score import interpret_p5_result
from tests.pillars.test_market_evaluation import quotes, evaluate, Memory, event
from tests.test_pillar_mining_repository import manager, snapshot, Repository


@pytest.mark.parametrize(
    "left,right,rep,state",
    [
        ("AWAY", "AWAY", "AWAY", "BOOK_CONSENSUS"),
        ("HOME", "AWAY", "NEUTRAL", "BOOK_DISAGREEMENT"),
        ("NEUTRAL", "NEUTRAL", "NEUTRAL", "NEUTRAL_STRUCTURE"),
        ("NEUTRAL", "AWAY", "AWAY", "PARTIAL_DIRECTIONAL_AGREEMENT"),
        ("AWAY", None, None, "SINGLE_BOOK_READING"),
        (None, None, None, "MISSING_INPUT"),
    ],
)
def test_side_readings(left, right, rep, state):
    reading = _interpret_side_pair(left, right, rep, rep is not None)
    assert reading["state"] == state
    if state == "SINGLE_BOOK_READING":
        assert reading["direction"] == "AWAY"
        assert reading["comparability"] == "PARTIAL"


def test_side_comparability_and_exchange():
    assert _interpret_side_pair("HOME", "AWAY", None, False)["direction"] is None
    assert (
        _interpret_side_pair("HOME", "AWAY", None, False)["reason"]
        == "INCOMPATIBLE_CONTRACT"
    )
    assert (
        _interpret_side_pair("AWAY", "AWAY", "AWAY", True, kind="exchange")["state"]
        == "BACK_LAY_CONSENSUS"
    )
    assert (
        _interpret_side_pair("HOME", "AWAY", "NEUTRAL", True, kind="exchange")["state"]
        == "BACK_LAY_DIVERGENCE"
    )
    assert (
        _interpret_side_pair("HOME", None, None, False, kind="exchange")["state"]
        == "SINGLE_EXCHANGE_READING"
    )


def test_period_consensus_requires_distinct_complete_market_branches():
    away = _interpret_side_pair("AWAY", "AWAY", "AWAY", True)
    home = _interpret_side_pair("HOME", "HOME", "HOME", True)
    assert _interpret_side_period({"1X2": away})["state"] == "PARTIAL_STRUCTURE"
    assert (
        _interpret_side_period({"1X2": away, "AH": away})["state"]
        == "MULTI_MARKET_CONSENSUS"
    )
    assert _interpret_side_period({"1X2": away, "AH": home})["direction"] == "MIXED"


def test_partial_period_keeps_only_a_consistent_available_direction():
    away = _interpret_side_pair("AWAY", "AWAY", "AWAY", True)
    single = _interpret_side_pair("AWAY", None, None, False)
    reading = _interpret_side_period({"1X2": away, "AH": single})
    assert (reading["state"], reading["direction"]) == ("PARTIAL_STRUCTURE", "AWAY")
    opposite = _interpret_side_pair("HOME", None, None, False)
    assert _interpret_side_period({"1X2": away, "AH": opposite})["direction"] is None
    disagreement = _interpret_side_pair("HOME", "AWAY", "AWAY", True)
    assert (
        _interpret_side_period({"1X2": disagreement, "AH": single})["direction"] is None
    )


def test_ou_different_lines_are_not_directional_conflict():
    reading = _interpret_ou_pair(
        {"LINE": Decimal("2.5"), "DIRECTION": "UNDER"},
        {"LINE": Decimal("2.25"), "DIRECTION": "OVER"},
        {},
    )
    assert reading["state"] == "DIFFERENT_LINE_STRUCTURE"
    assert reading["direction"] is None
    assert reading["line_gap"] == Decimal(".25")


def test_real_p3_reunites_line_evidence_from_separate_lanes():
    rows = quotes("Over/Under", book=302, line="1.0", market_id=70)
    rows += quotes("Over/Under", book=3, line="0.5", market_id=71)
    result = evaluate(3, rows)
    structure = result["INTERPRETATION"]["line_structures"]["FT"]
    assert structure["state"] == "DIFFERENT_LINE_STRUCTURE"
    assert {r["line"] for r in structure["readings"]} == {
        Decimal("1.0"),
        Decimal("0.5"),
    }
    assert structure["representative_direction"] is None


def test_p2_different_handicap_lines_and_first_five_keep_contract_identity():
    rows = quotes(group="Home/Away") + quotes(
        group="Home/Away", period=FIRST_FIVE, market_id=71
    )
    rows += quotes(group="Asian Handicap", book=302, line="-.5", market_id=72)
    rows += quotes(group="Asian Handicap", book=3, line="-1.0", market_id=73)
    result = evaluate(2, rows)
    interpretation = result["INTERPRETATION"]
    assert any(
        "FIRST_FIVE_INNINGS" in scope["periods"]
        for scope in interpretation["scopes"].values()
    )
    assert any(
        reading["comparability"] == "INCOMPARABLE_LINE"
        for reading in interpretation["line_structures"].values()
    )


@pytest.mark.parametrize(
    "interpret",
    [
        interpret_p2_result,
        interpret_p3_result,
        interpret_p4_result,
        interpret_p5_result,
    ],
)
@pytest.mark.parametrize("skipped", [False, True])
def test_unexecuted_or_failed_scope_keeps_its_execution_status(interpret, skipped):
    result = EvaluationResult(
        1,
        "example",
        "v",
        5,
        {},
        (
            ()
            if skipped
            else (SignalResult("execution", "ERROR", reason="CALCULATION_ERROR"),)
        ),
        (),
        skipped=skipped,
    ).to_dict()
    assert interpret(result)["status"] == ("SKIPPED" if skipped else "ERROR")


def test_real_p3_compares_books_and_exchange_on_their_own_lines():
    rows = quotes("Over/Under", book=302, line="2.5", market_id=70)
    rows += quotes("Over/Under", book=3, line="2.5", market_id=71)
    rows += quotes("Over/Under", book=4, side="back", line="2.25", market_id=72)
    rows += quotes("Over/Under", book=4, side="lay", line="2.25", market_id=72)
    result = evaluate(3, rows)
    structure = result["INTERPRETATION"]["line_structures"]["FT"]
    assert structure["state"] == "DIFFERENT_LINE_STRUCTURE"
    assert {r["bookmaker"] for r in structure["readings"]} == {
        "PINNACLE",
        "BET365",
        "BETFAIR",
    }


def temporal_result(
    *,
    endpoint=Decimal(".1"),
    net="NEGATIVE",
    final="NEGATIVE",
    pattern="UNIDIRECTIONAL",
    status="ACTIVE",
    gap=False,
):
    return {
        "status": "ACTIVE",
        "diagnostics": {"signals": {"reasons": {}}},
        "inputs": {"series": {"point_refs": ["a", "b"]}, "b": {"value": endpoint}},
        "analysis": {
            "series": {
                "market": {
                    "DOMAIN": "SIDE",
                    "VALUE_TYPE": "SIDE_EDGE",
                    "CHOICE_NAME": "SIDE_EDGE",
                    "MARKET_GROUP": "1X2",
                    "MARKET_PERIOD": "Full Time",
                    "MARKET_NAME": "1X2",
                    "CHOICE_GROUP_KEY": "default",
                    "CHOICE_GROUP": None,
                    "VIEW": "CHECKPOINT_VIEW",
                },
                "raw_temporal_features": {"GAP_PRESENT_RAW": gap},
                "structural_signals": {
                    "PATH_PATTERN_RAW": pattern,
                    "NET_DIRECTION_RAW": net,
                    "FINAL_RUN_DIRECTION_RAW": final,
                },
                "traceability": {"OPERATIVE_ENDPOINT_PRESENT": True},
                "status": status,
                "input_refs": ["series"],
            }
        },
    }


def test_p4_final_position_is_not_movement_direction():
    result = temporal_result()
    before = deepcopy(result)
    reading = interpret_p4_result(result)["series"]["series"]
    assert reading["endpoint_direction"] == "HOME"
    assert reading["net_move_direction"] == "AWAY"
    assert reading["final_run_direction"] == "AWAY"
    assert "direction" not in reading
    assert "direction_at_checkpoint" not in reading
    assert result == before


@pytest.mark.parametrize(
    "pattern,net,final,gap,status,expected",
    [
        ("REVERSAL", "NEGATIVE", "NEGATIVE", False, "ACTIVE", "FINAL_RUN_ALIGNED"),
        (
            "MULTI_REVERSAL",
            "NEGATIVE",
            "POSITIVE",
            False,
            "ACTIVE",
            "FINAL_RUN_COUNTERMOVE",
        ),
        ("NO_MOVEMENT", "ZERO", None, False, "ACTIVE", "STABLE_MARKET"),
        (
            "NO_MOVEMENT",
            "ZERO",
            None,
            False,
            "INSUFFICIENT_DATA",
            "INSUFFICIENT_TRAJECTORY",
        ),
        (None, "NEGATIVE", "NEGATIVE", True, "ACTIVE", "PARTIAL_TRAJECTORY"),
    ],
)
def test_p4_pattern_and_coverage(pattern, net, final, gap, status, expected):
    result = temporal_result(
        pattern=pattern, net=net, final=final, gap=gap, status=status
    )
    assert interpret_p4_result(result)["series"]["series"]["state"] == expected


@pytest.mark.parametrize(
    "value_type,domain,sign,expected",
    [
        ("SIDE_EDGE", "SIDE", "NEGATIVE", "AWAY"),
        ("OU_EDGE", "TOTALS", "NEGATIVE", "UNDER"),
        ("BOOK_REP_EDGE", "TOTALS", "POSITIVE", "OVER"),
        ("ODDS_PRICE", "SIDE", "NEGATIVE", "CHOICE_STRENGTHENING"),
        ("ODDS_PRICE", "SIDE", "POSITIVE", "CHOICE_WEAKENING"),
        ("LINE", "TOTALS", "POSITIVE", "LINE_UP"),
        ("EXCHANGE_SIZE", "SIDE", "ZERO", "SIZE_UNCHANGED"),
        ("BOOK_INTERNAL_GAP", "SIDE", "POSITIVE", "WIDENING"),
        ("BACK_LAY_RELATIVE_SPREAD", "SIDE", "NEGATIVE", "NARROWING"),
    ],
)
def test_p4_value_domain(value_type, domain, sign, expected):
    assert _semantic_movement(value_type, domain, sign) == expected


def test_p3_canonical_periods_deduplicate_lanes_without_losing_contracts():
    rows = quotes("Over/Under", book=302, line="2.5", price="2.2")
    rows[1]["odds_value"] = "1.8"
    rows += quotes("Over/Under", book=3, line="2.5", market_id=71)
    rows += quotes("Over/Under", period="1st Half", book=302, line="1.0", market_id=72)
    half = quotes(
        "Over/Under", period="1st Half", book=3, line="0.5", market_id=73, price="1.8"
    )
    half[1]["odds_value"] = "2.2"
    rows += half
    for i, line in enumerate(("0.5", "1.5", "2.25", "3.5"), 80):
        alternative = quotes("Over/Under", book=302, line=line, market_id=i)
        for row in alternative:
            row["main_line"] = False
        rows += alternative
    result = evaluate(3, rows)
    interpretation = result["INTERPRETATION"]
    ft, half = (
        interpretation["period_readings"]["FT"],
        interpretation["period_readings"]["1H"],
    )
    assert (
        ft["line"],
        ft["state"],
        ft["books_direction"],
        ft["representative_direction"],
    ) == (Decimal("2.5"), "PARTIAL_DIRECTIONAL_AGREEMENT", "UNDER", "UNDER")
    assert (
        half["pinnacle_line"],
        half["pinnacle_direction"],
        half["bet365_line"],
        half["bet365_direction"],
    ) == (Decimal("1.0"), "NEUTRAL", Decimal("0.5"), "OVER")
    assert half["state"] == "DIFFERENT_LINE_STRUCTURE"
    assert half["representative_direction"] is None
    assert len(interpretation["scopes"]) > 2
    assert len(interpretation["period_readings"]) == 2


def test_p3_multiple_valid_contracts_remain_explicit_without_arbitrary_selection():
    rows = []
    for i, line in enumerate(("2.5", "3.5"), 80):
        rows += quotes("Over/Under", book=302, line=line, market_id=i)
        rows += quotes("Over/Under", book=3, line=line, market_id=i + 10)
    reading = evaluate(3, rows)["INTERPRETATION"]["period_readings"]["FT"]
    assert reading["reason"] == "AMBIGUOUS_CANDIDATE"
    assert reading["representative_direction"] is None
    assert {r["line"] for r in reading["contract_readings"]} == {
        Decimal("2.5"),
        Decimal("3.5"),
    }


def test_stable_away_edge_preserves_endpoint_without_neutral_movement():
    result = temporal_result(
        endpoint=Decimal("-.1"), net="ZERO", final="NEUTRAL", pattern="NO_MOVEMENT"
    )
    interpretation = interpret_p4_result(result)
    for reading in (
        interpretation["series"]["series"],
        next(iter(interpretation["contracts"].values())),
    ):
        assert reading["endpoint_direction"] == "AWAY"
        assert reading["net_move_direction"] == "NO_MOVEMENT"
        assert reading["final_run_direction"] is None
        assert reading["state"] == "STABLE_MARKET"
        assert "direction" not in reading
    assert interpretation["version"] == 2


def test_over_endpoint_can_have_under_net_movement():
    result = temporal_result()
    result["analysis"]["series"]["market"].update(DOMAIN="TOTALS", VALUE_TYPE="OU_EDGE")
    reading = interpret_p4_result(result)["series"]["series"]
    assert (
        reading["endpoint_direction"],
        reading["net_move_direction"],
        reading["final_run_direction"],
    ) == ("OVER", "UNDER", "UNDER")


def test_p4_contract_uses_existing_representative_without_counting_components():
    result = temporal_result()
    base = result["analysis"]["series"]
    for key, value_type, bookmaker in (
        ("bet365", "SIDE_EDGE", "bet365"),
        ("books", "BOOK_REP_EDGE", "BOOKS_REPRESENTATIVE"),
    ):
        result["analysis"][key] = {
            **base,
            "market": {
                **base["market"],
                "VALUE_TYPE": value_type,
                "BOOKIE_NAME": bookmaker,
            },
        }
    contract = next(iter(interpret_p4_result(result)["contracts"].values()))
    assert contract["value_type"] == "BOOK_REP_EDGE"
    assert contract["evidence"] == {"series_ref": "books"}
    assert len(contract["series_refs"]) == 3
    assert contract["net_move_direction"] == "AWAY"
    result["analysis"]["exchange"] = {
        **base,
        "market": {
            **base["market"],
            "VALUE_TYPE": "EXCHANGE_REP_EDGE",
            "BOOKIE_NAME": "Betfair",
            "EXCHANGE_SIDE": "representative",
        },
    }
    contract = next(iter(interpret_p4_result(result)["contracts"].values()))
    assert contract["endpoint_direction"] is None
    assert set(contract["source_readings"]) == {"books", "exchange"}
    assert len(contract["series_refs"]) == 4


def test_p4_contracts_keep_views_and_lines_separate():
    result = temporal_result()
    base = result["analysis"]["series"]
    result["analysis"]["adaptive"] = {
        **base,
        "market": {**base["market"], "VIEW": "ADAPTIVE_VIEW"},
    }
    result["analysis"]["other_line"] = {
        **base,
        "market": {**base["market"], "CHOICE_GROUP_KEY": "0.5", "CHOICE_GROUP": "0.5"},
    }
    assert len(interpret_p4_result(result)["contracts"]) == 3


class Counts(Memory):
    def __init__(self, counts):
        self.counts = counts

    def summarize(self, *, key, **kwargs):
        from modules.pillars.pillar_5.calculation_models import MemorySample

        return MemorySample(key, sum(self.counts), *self.counts)


@pytest.mark.parametrize(
    "counts,state,direction",
    [
        ((1, 0, 0), "INSUFFICIENT_HISTORICAL_SAMPLE", "NONE"),
        ((1, 1, 1), "HISTORICAL_OUTCOME_TIE", "NONE"),
        ((0, 3, 0), "HISTORICAL_OUTCOME_READING", "DRAW"),
    ],
)
def test_p5_tied_counts_are_not_a_sporting_draw(counts, state, direction):
    result = evaluate(5, quotes(book=3), Counts(counts))
    reading = result["INTERPRETATION"]["bookmakers"]["3:1X2:Full Time"]
    assert reading["state"] == state
    assert reading["direction"] == direction
    assert reading["strength"] == result["analysis"]["3:1X2:Full Time"]["P5_STRENGTH"]
    signal = next(s for s in result["signals"] if s["key"] == "3:1X2:Full Time")
    assert {key: signal["evidence"][key] for key in ("bookie_id", "bookmaker", "market_group", "market_period")} == {
        "bookie_id": 3, "bookmaker": "bet365", "market_group": "1X2", "market_period": "Full Time"
    }


def test_p5_scoring_exception_preserves_contract_evidence(monkeypatch):
    def fail(**kwargs):
        raise ValueError("scoring failure")

    monkeypatch.setattr(import_module("modules.pillars.pillar_5.run_pillar_5"), "calculate_memory_profile", fail)
    result = evaluate(5, quotes(book=3), Counts((0, 3, 0)))
    signal = next(s for s in result["signals"] if s["key"] == "3:1X2:Full Time")
    assert (signal["status"], signal["reason"]) == ("ERROR", "CALCULATION_ERROR")
    assert signal["evidence"]["bookie_id"] == 3
    assert signal["evidence"]["market_group"] == "1X2"
    assert signal["evidence"]["market_period"] == "Full Time"
    assert signal["evidence"]["error_class"] == "ValueError"


def directional_quotes(pillar, book, *, side=None, line="2.5", reverse=False):
    rows = quotes("Over/Under" if pillar == 3 else "1X2", book=book, side=side, line=line if pillar == 3 else None)
    rows[0]["odds_value"], rows[-1]["odds_value"] = ("1.8", "2.1") if reverse else ("2.1", "1.8")
    return rows


def test_p3_single_book_summary_preserves_direction_and_line():
    result = evaluate(3, directional_quotes(3, 302))
    reading = result["EXECUTIVE_SUMMARY"]["full_time"]
    assert (reading["direction"], reading["line"], reading["state"]) == ("UNDER", Decimal("2.5"), "SINGLE_BOOK_READING")
    assert "UNDER" in result["EXECUTIVE_SUMMARY"]["summary"]


@pytest.mark.parametrize("pillar,direction", [(2, "AWAY"), (3, "UNDER")])
def test_exchange_only_summary_keeps_source_separate(pillar, direction):
    result = evaluate(pillar, directional_quotes(pillar, 4, side="back"))
    reading = result["EXECUTIVE_SUMMARY"]["full_time"]
    assert reading["direction"] is None
    sources = reading["source_readings"]["exchange"]
    assert len(sources) == 1
    assert (sources[0]["state"], sources[0]["direction"]) == ("SINGLE_EXCHANGE_READING", direction)
    assert "Exchange" in result["EXECUTIVE_SUMMARY"]["summary"]
    assert direction in result["EXECUTIVE_SUMMARY"]["summary"]


@pytest.mark.parametrize("pillar", [2, 3])
@pytest.mark.parametrize("reverse,state", [(False, "CROSS_SOURCE_CONSENSUS"), (True, "CROSS_SOURCE_CONFLICT")])
def test_source_summary_preserves_existing_comparison(pillar, reverse, state):
    rows = directional_quotes(pillar, 302) + directional_quotes(pillar, 3)
    rows += directional_quotes(pillar, 4, side="back", reverse=reverse)
    rows += directional_quotes(pillar, 4, side="lay", reverse=reverse)
    result = evaluate(pillar, rows)
    sources = result["EXECUTIVE_SUMMARY"]["full_time"]["source_readings"]
    assert any(r["status"] == "COMPUTED" for r in sources["books"])
    assert any(r["status"] == "COMPUTED" for r in sources["exchange"])
    assert any(r["state"] == state for r in sources["book_exchange"])
    assert len(sources["exchange"]) == 1
    assert "Books/exchange" in result["EXECUTIVE_SUMMARY"]["summary"]


@pytest.mark.parametrize("pillar", [2, 3])
def test_source_summary_does_not_merge_different_lines(pillar):
    group = "Over/Under" if pillar == 3 else "Asian Handicap"
    rows = quotes(group, book=302, line="0.5") + quotes(group, book=3, line="0.5")
    rows += quotes(group, book=4, side="back", line="1.0") + quotes(group, book=4, side="lay", line="1.0")
    if pillar == 2:
        rows += directional_quotes(2, 302)
    result = evaluate(pillar, rows)
    sources = result["EXECUTIVE_SUMMARY"]["full_time"]["source_readings"]
    comparisons = [r for r in sources["book_exchange"] if r["market"] == ("AH" if pillar == 2 else "Over/Under")]
    assert any(r["reason"] == "INCOMPATIBLE_CONTRACT" and r["direction"] is None for r in comparisons)
    assert any(r["line"] == Decimal("1.0") if "line" in r else r["left_line"] == Decimal("1.0") for r in sources["exchange"] if r["market"] == ("AH" if pillar == 2 else "Over/Under"))


def test_interpretation_does_not_create_analytical_signals_or_metrics():
    result = evaluate(2, quotes(book=302) + quotes(book=3))
    before = deepcopy(result)
    without = {k: v for k, v in result.items() if k != "INTERPRETATION"}
    adapter = P2MiningAdapter()
    a, b = adapter.build(event(), result), adapter.build(event(), without)
    assert a.units == b.units
    assert a.output_payload["INTERPRETATION"] == result["INTERPRETATION"]
    assert result == before


@pytest.mark.parametrize(
    "pillar,adapter", [(2, P2MiningAdapter), (3, P3MiningAdapter), (4, P4MiningAdapter), (5, P5MiningAdapter)]
)
def test_interpretation_round_trip_preserves_persisted_result(manager, pillar, adapter):
    rows = quotes(book=302) + quotes(book=3)
    rows += quotes("Over/Under", book=302) + quotes("Over/Under", book=3)
    rows += quotes(book=302, minute=30, price="1.9") + quotes(
        book=3, minute=30, price="1.9"
    )
    rows += quotes("Over/Under", book=4, side="back", market_id=81)
    rows += quotes(book=4, side="back", market_id=82)
    result = evaluate(pillar, rows)
    original = snapshot(event_id=900)
    run = replace(
        adapter().build(event(), result), evaluation_id=original.evaluation_id
    )
    with manager.get_session() as session:
        Repository.append_evaluation(original, session=session)
        run_id = Repository.append_run(run, session=session)
    stored = Repository.get_result(run_id)["result"]
    assert stored["INTERPRETATION"] == to_json_value(result["INTERPRETATION"])
    assert stored["EXECUTIVE_SUMMARY"] == to_json_value(result["EXECUTIVE_SUMMARY"])
    assert stored["diagnostics"] == result["diagnostics"]
    assert len(stored["signals"]) == len(result["signals"])
    assert (
        adapter()
        .build(event(), {k: v for k, v in result.items() if k != "INTERPRETATION"})
        .units
        == run.units
    )


@pytest.mark.parametrize(
    "pillar,module_name,function_name",
    [
        (2, "pillar_2_side_market.run_pillar_2", "interpret_p2_result"),
        (3, "pillar_3_totals_market_context.run_pillar_3", "interpret_p3_result"),
        (4, "pillar_4.run_pillar_4", "interpret_p4_result"),
        (5, "pillar_5.run_pillar_5", "interpret_p5_result"),
    ],
)
def test_interpretation_is_additive_to_all_analytical_outputs(
    monkeypatch, pillar, module_name, function_name
):
    rows = quotes(book=302, minute=30) + quotes(book=302, minute=5)
    rows += quotes(book=3, minute=30) + quotes(book=3, minute=5)
    rows += quotes("Over/Under", book=302, minute=30, market_id=71)
    rows += quotes("Over/Under", book=302, minute=5, market_id=71)
    actual = evaluate(pillar, rows)
    monkeypatch.setattr(
        import_module("modules.pillars." + module_name),
        function_name,
        lambda result: {},
    )
    baseline = evaluate(pillar, rows)
    actual.pop("INTERPRETATION")
    baseline.pop("INTERPRETATION")
    actual.pop("EXECUTIVE_SUMMARY")
    baseline.pop("EXECUTIVE_SUMMARY")
    assert actual == baseline


def test_compact_logger_only_renders_enabled_pillars(caplog):
    result = evaluate(2, quotes(book=302) + quotes(book=3))
    logger = getLogger("test.official_interpretation")
    with caplog.at_level("INFO", logger=logger.name):
        log_pillar_results(
            logger,
            evaluation_id="example",
            event_id=900,
            evaluation_as_of=datetime.now(timezone.utc),
            participants="Home vs Away",
            results={"pillar_2": result},
            enabled_pillars={f"pillar_{n}": n == 2 for n in range(1, 6)},
        )
    assert len(caplog.records) == 1
    assert "🏁 P2" in caplog.text
    assert "EXECUTIVE_SUMMARY:" in caplog.text
    assert "NEUTRAL" in caplog.text
    assert "INTERPRETATION_DETAIL:" not in caplog.text
    assert "PIN_EDGE" not in caplog.text
    assert "analysis_ref" not in caplog.text


@pytest.mark.parametrize(
    "pillar,block,omitted",
    [(3, "full_time", "scopes"), (4, "side", "series")],
)
def test_logger_renders_native_compact_readings_without_detailed_enumeration(
    caplog, pillar, block, omitted
):
    rows = quotes("Over/Under", book=302, minute=30) + quotes("Over/Under", book=302)
    rows += quotes("Over/Under", book=3, minute=30) + quotes("Over/Under", book=3)
    result = evaluate(pillar, rows)
    logger = getLogger("test.compact_semantics")
    with caplog.at_level("INFO", logger=logger.name):
        log_pillar_results(
            logger,
            evaluation_id="example",
            event_id=900,
            evaluation_as_of=datetime.now(timezone.utc),
            participants="Home vs Away",
            results={f"pillar_{pillar}": result},
            enabled_pillars={f"pillar_{n}": n == pillar for n in range(1, 6)},
        )
    assert len(caplog.records) == 1
    assert f"{block}:" in caplog.text
    assert f"{omitted}:" not in caplog.text
    if pillar == 4:
        assert "net_move_direction:" in caplog.text
        assert "endpoint_direction: NEUTRAL" in caplog.text


@pytest.mark.parametrize("debug", [False, True])
def test_summary_logger_normal_and_debug(caplog, debug):
    result = evaluate(3, directional_quotes(3, 4, side="back"))
    logger = getLogger("test.source_summary_logging")
    with caplog.at_level("INFO", logger=logger.name):
        log_pillar_results(logger, evaluation_id="example", event_id=900,
                           evaluation_as_of=datetime.now(timezone.utc), participants="Home vs Away",
                           results={"pillar_3": result}, enabled_pillars={f"pillar_{n}": n == 3 for n in range(1, 6)}, debug_mode=debug)
    assert len(caplog.records) == 1
    assert "Exchange" in caplog.text and "UNDER" in caplog.text
    assert ("INTERPRETATION_DETAIL:" in caplog.text) == debug
