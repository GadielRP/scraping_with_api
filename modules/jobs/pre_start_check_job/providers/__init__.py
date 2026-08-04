"""Provider-specific pre-start odds acquisition, grouped by provider.

Each subpackage plugs into the shared candidate plan built by the pre-start
check job (see ``event_candidate_builder`` and ``odds_source_state`` one
level up) and exposes a single ``run_<provider>_pre_start_odds`` entrypoint
with the same call shape, registered in ``run_pre_start_check_job``.
"""
