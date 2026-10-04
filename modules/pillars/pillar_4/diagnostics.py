"""Human-readable P4 diagnostics projected from the evaluated result."""

from collections import Counter


def log_trajectory_diagnostics(logger, result):
    checkpoint = result["checkpoint"]
    logger.info(
        "P4 timing | event=%s | target=T-%s | nominal=%s | evaluation=%s | "
        "latest checkpoint=%s | selected FT=%s (%s)",
        result["event_id"], result["target_minute"],
        checkpoint.get("nominal_target_as_of"), checkpoint.get("evaluation_as_of"),
        checkpoint.get("operative_as_of"), result["selected_full_time_period"],
        result["selection"].get("reason"),
    )
    for diagnostic in result["diagnostics"]:
        if "excluded_future_points" in diagnostic:
            logger.info(
                "P4 inputs | selections=%s | with endpoint=%s | excluded by time=%s | invalid observations=%s | "
                "ambiguous selections=%s | missing endpoints=%s",
                diagnostic["source_series_seen"], diagnostic["endpoint_series_present"],
                diagnostic["excluded_future_points"], len(diagnostic["invalid"]),
                len(diagnostic["ambiguous"]), len(diagnostic["missing_endpoints"]),
            )
    source_status = {}
    for series in result["analysis"].values():
        market, trace = series["market"], series["traceability"]
        if market["VALUE_TYPE"] != "ODDS_PRICE":
            continue
        points = [result["inputs"][ref]
                  for ref in result["inputs"][series["input_refs"][0]]["point_refs"]]
        endpoint = next((point for point in points
                         if point["observation_kind"] == "OPERATIVE_ENDPOINT"), None)
        observation = result["inputs"].get(endpoint["observation_ref"], {}) if endpoint else {}
        key = (market["BOOKIE_NAME"], market["BOOKIE_ID"], market["MARKET_GROUP"],
               market["MARKET_PERIOD"], market["EXCHANGE_SIDE"])
        source_status.setdefault(key, Counter())[series["status"]] += 1
        movement = (
            "unavailable: selected endpoint is missing"
            if not trace["OPERATIVE_ENDPOINT_PRESENT"] else
            "unavailable: at least two observations are needed"
            if trace["OBSERVATION_COUNT"] < 2 else
            "available; metrics requiring continuity may be blocked"
            if trace["MISSING_TARGET_MINUTES"] else "available"
        )
        logger.info(
            "P4 series | book=%s (%s) | market=%s / %s | line=%s | choice=%s | "
            "side=%s | view=%s | status=%s | observations=%s | missing checkpoints=%s | "
            "endpoint price=%s effective=%s collected=%s | net move=%s | path=%s | movement=%s",
            market["BOOKIE_NAME"], market["BOOKIE_ID"], market["MARKET_GROUP"],
            market["MARKET_PERIOD"], market["CHOICE_GROUP"], market["CHOICE_NAME"],
            market["EXCHANGE_SIDE"] or "regular", market["VIEW"], series["status"],
            trace["OBSERVATION_COUNT"], trace["MISSING_TARGET_MINUTES"],
            endpoint["value"] if endpoint else "unavailable",
            observation.get("EFFECTIVE_AT"), observation.get("COLLECTED_AT"),
            series["raw_temporal_features"].get("NET_MOVE_RAW"),
            series["structural_signals"].get("PATH_PATTERN_RAW"),
            movement,
        )
    for (name, book, family, period, side), statuses in source_status.items():
        logger.info(
            "P4 source | book=%s (%s) | market=%s / %s | side=%s | series=%s | "
            "source gaps do not downgrade other calculated signals",
            name, book, family, period, side or "regular", dict(statuses),
        )
    for cell in result["coverage"]:
        if cell["status"] in {"MISSING", "INCOMPLETE", "INVALID", "AMBIGUOUS"}:
            logger.info(
                "P4 coverage | book=%s | market=%s / %s | line=%s | side=%s | "
                "status=%s | missing=%s",
                cell["bookie_id"], cell["family"], cell["period"], cell["line_value"],
                cell["exchange_side"] or "regular", cell["status"], cell["missing"],
            )
