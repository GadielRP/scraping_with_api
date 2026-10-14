"""Compact P4 observations and one worked checkpoint trajectory."""

from collections import Counter, defaultdict


def log_trajectory_inputs(logger, extraction, selection):
    logger.info(
        "P4 timing | event=%s | target=T-%s | nominal=%s | evaluation=%s | "
        "latest checkpoint=%s | selected FT=%s (%s)",
        extraction.event_id,
        extraction.target_minute,
        extraction.nominal_target_as_of,
        extraction.evaluation_as_of,
        extraction.operative_as_of,
        selection.get("selected_period"),
        selection.get("reason"),
    )
    logger.info(
        "P4 inputs | event=%s | selections=%s | with endpoint=%s | excluded by time=%s | "
        "invalid observations=%s | ambiguous selections=%s | missing endpoints=%s",
        extraction.event_id,
        extraction.source_series_seen,
        extraction.endpoint_series_present,
        extraction.excluded_future_points,
        len(extraction.invalid_inputs),
        len(extraction.ambiguous_inputs),
        len(extraction.missing_endpoint_details),
    )
    logger.info(
        "P4 flow | event=%s | adaptive and checkpoint trajectories -> legs -> "
        "net/path/velocity -> runs/turns/corrections -> windows; "
        "derived edges/gaps/spreads use common checkpoints; one worked price example below",
        extraction.event_id,
    )


def _log_example(logger, event_id, series_id, series, inputs):
    market, trace = series["market"], series["traceability"]
    logger.info(
        "P4 example | event=%s | series=%s | book=%s (%s) | market=%s / %s | "
        "line=%s | choice=%s | side=%s | view=%s | status=%s | observations=%s | "
        "missing checkpoints=%s | endpoint present=%s",
        event_id,
        series_id,
        market["BOOKIE_NAME"],
        market["BOOKIE_ID"],
        market["MARKET_GROUP"],
        market["MARKET_PERIOD"],
        market["CHOICE_GROUP"],
        market["CHOICE_NAME"],
        market["EXCHANGE_SIDE"] or "regular",
        market["VIEW"],
        series["status"],
        trace["OBSERVATION_COUNT"],
        trace["MISSING_TARGET_MINUTES"],
        trace["OPERATIVE_ENDPOINT_PRESENT"],
    )
    refs = inputs[series["input_refs"][0]]["point_refs"]
    for index, ref in enumerate(refs, start=1):
        point = inputs[ref]
        observation = inputs[point["observation_ref"]]
        logger.info(
            "P4 example point | event=%s | point=%s | target=T-%s | value=%s | effective=%s | source=%s | collected=%s | basis=%s | kind=%s",
            event_id,
            index,
            point["target_minute"],
            point["value"],
            point["effective_at"],
            observation.get("SOURCE_COLLECTED_AT"),
            observation.get("COLLECTED_AT"),
            point["time_basis"],
            point["observation_kind"],
        )
    if series["status"] != "ACTIVE":
        logger.info(
            "P4 example | event=%s | movement unavailable: endpoint present=%s; "
            "observations=%s (at least two required)",
            event_id,
            trace["OPERATIVE_ENDPOINT_PRESENT"],
            trace["OBSERVATION_COUNT"],
        )
        return
    for leg in series["legs"]:
        start, end = inputs[leg["FROM_POINT_ID"]], inputs[leg["TO_POINT_ID"]]
        logger.info(
            "P4 example leg | event=%s | leg=%s | delta=end-start=%s-%s=%s | "
            "elapsed=(end_time-start_time)/60=%s min | velocity=delta/elapsed=%s | "
            "contiguous=%s | direction=%s | move_share=abs(delta)/path= %s",
            event_id,
            leg["ORDINAL"],
            end["value"],
            start["value"],
            leg["DELTA_RAW"],
            leg["ELAPSED_MINUTES_ACTUAL"],
            leg["VELOCITY_RAW"],
            leg["CONTIGUOUS_RAW"],
            leg["DIRECTION_RAW"],
            leg["MOVE_SHARE_RAW"],
        )
    features, signals = series["raw_temporal_features"], series["structural_signals"]
    logger.info(
        "P4 example path | event=%s | net=last-first=%s-%s=%s | "
        "path=sum(abs(contiguous deltas))=%s | efficiency=abs(net)/path= %s | "
        "velocity=net/elapsed=%s | gap=%s | pattern=%s | net_direction=%s | final_run=%s",
        event_id,
        inputs[refs[-1]]["value"],
        inputs[refs[0]]["value"],
        features["NET_MOVE_RAW"],
        features["PATH_LENGTH_RAW"],
        features["PATH_EFFICIENCY_RAW"],
        features["VELOCITY_RAW"],
        features["GAP_PRESENT_RAW"],
        signals["PATH_PATTERN_RAW"],
        signals["NET_DIRECTION_RAW"],
        signals["FINAL_RUN_DIRECTION_RAW"],
    )
    runs = features["DIRECTIONAL_RUNS"]
    turning = signals["TURNING_STRUCTURE_RAW"] or {}
    logger.info(
        "P4 example structure | event=%s | runs(direction,delta)=%s | "
        "sign_changes=%s | turns(type,structure)=%s | acceleration=%s | deceleration=%s",
        event_id,
        [(run["DIRECTION"], run["DELTA_RAW"]) for run in runs],
        turning.get("SIGN_CHANGE_COUNT_RAW"),
        [(zone["TYPE"], zone["STRUCTURE"]) for zone in turning.get("ZONES", [])],
        signals["ACCELERATION_RAW"],
        signals["DECELERATION_RAW"],
    )
    correction = signals["CORRECTION_RAW"] or {}
    logger.info(
        "P4 example correction | event=%s | state=%s | ratio=abs(correction)/abs(initial)=%s | "
        "retention=max(0,1-ratio)=%s | overshoot=%s | overshoot_magnitude=%s",
        event_id,
        correction.get("STATE"),
        correction.get("CORRECTION_RATIO_RAW"),
        correction.get("MOVE_RETENTION_RAW"),
        correction.get("OVERSHOOT_RAW"),
        correction.get("OVERSHOOT_MAGNITUDE_RAW"),
    )
    for window in features["WINDOWS"]:
        logger.info(
            "P4 example window | event=%s | window=%s | net=%s | path= %s | "
            "share=window_path/total_path= %s | pattern=%s",
            event_id,
            window["WINDOW"],
            window["WINDOW_NET_MOVE_RAW"],
            window["WINDOW_PATH_LENGTH_RAW"],
            window["MOVE_SHARE_BY_WINDOW"],
            window["WINDOW_PATTERN_RAW"],
        )
    logger.info(
        "P4 example dominance | event=%s | largest_delta_legs=%s | largest_path_windows=%s",
        event_id,
        features["DOMINANT_SEGMENTS_RAW"],
        features["DOMINANT_WINDOWS_RAW"],
    )


def log_trajectory_diagnostics(logger, result):
    event_id = result["event_id"]
    groups, sources = defaultdict(list), defaultdict(list)
    for series in result["analysis"].values():
        market = series["market"]
        groups[(market["VIEW"], market["VALUE_TYPE"])].append(series)
        if market["VALUE_TYPE"] == "ODDS_PRICE":
            key = (
                market["BOOKIE_NAME"],
                market["BOOKIE_ID"],
                market["MARKET_GROUP"],
                market["MARKET_PERIOD"],
                market["EXCHANGE_SIDE"],
            )
            sources[key].append(series)
    for (view, value_type), series in sorted(groups.items()):
        logger.info(
            "P4 observations | event=%s | view=%s | value_type=%s | series=%s | "
            "states=%s | patterns=%s",
            event_id,
            view,
            value_type,
            len(series),
            dict(Counter(item["status"] for item in series)),
            dict(
                Counter(
                    item["structural_signals"].get("PATH_PATTERN_RAW")
                    for item in series
                    if item["status"] == "ACTIVE"
                )
            ),
        )
    for (name, book, family, period, side), series in sources.items():
        logger.info(
            "P4 source | event=%s | book=%s (%s) | market=%s / %s | side=%s | "
            "price_series_both_views=%s | observations_min=%s | observations_max=%s | "
            "missing_endpoint_series=%s; movement requires endpoint and at least two observations",
            event_id,
            name,
            book,
            family,
            period,
            side or "regular",
            dict(Counter(item["status"] for item in series)),
            min(item["traceability"]["OBSERVATION_COUNT"] for item in series),
            max(item["traceability"]["OBSERVATION_COUNT"] for item in series),
            sum(
                not item["traceability"]["OPERATIVE_ENDPOINT_PRESENT"]
                for item in series
            ),
        )
    logger.info(
        "P4 coverage | event=%s | all_statuses=%s | missing contracts do not block independent trajectories",
        event_id,
        dict(Counter(cell["status"] for cell in result["coverage"])),
    )
    for status in ("INVALID", "AMBIGUOUS", "INCOMPLETE", "MISSING"):
        cells = [cell for cell in result["coverage"] if cell["status"] == status]
        for cell in cells[:3]:
            logger.info(
                "P4 coverage example | event=%s | book=%s | market=%s / %s | line=%s | "
                "side=%s | status=%s | missing=%s",
                event_id,
                cell["bookie_id"],
                cell["family"],
                cell["period"],
                cell["line_value"],
                cell["exchange_side"] or "regular",
                cell["status"],
                cell["missing"],
            )
        if len(cells) > 3:
            logger.info(
                "P4 coverage | event=%s | status=%s | additional_contracts=%s (details in result.coverage)",
                event_id,
                status,
                len(cells) - 3,
            )
    candidates = [
        (key, series)
        for key, series in result["analysis"].items()
        if series["market"]["VIEW"] == "CHECKPOINT_VIEW"
        and series["market"]["VALUE_TYPE"] == "ODDS_PRICE"
    ]
    if candidates:
        series_id, series = min(
            candidates,
            key=lambda item: (
                item[1]["status"] != "ACTIVE",
                bool(item[1]["traceability"]["MISSING_TARGET_MINUTES"]),
            ),
        )
        _log_example(logger, event_id, series_id, series, result["inputs"])
