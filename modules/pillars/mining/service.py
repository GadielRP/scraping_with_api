"""Mandatory, transaction-owned instrumentation of independent pillar results."""

from dataclasses import replace
from .contracts import validate_mining_run
from .fingerprint import content_hash


class PillarMiningService:
    def __init__(self, writer, adapters):
        self.writer = writer
        self.adapters = dict(adapters)

    def persist(self, pillar_id, event_context, result, *, evaluation_id, session):
        run = replace(
            self.adapters[pillar_id].build(event_context, result),
            evaluation_id=evaluation_id,
        )
        run = replace(
            run,
            inputs={
                name: {"evaluation_input_ref": content_hash(point)}
                for name, point in run.inputs.items()
            },
        )
        validate_mining_run(run)
        self.writer.append_run(run, session=session)
        return run
