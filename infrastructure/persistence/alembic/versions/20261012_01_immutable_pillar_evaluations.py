"""Start immutable pillar history; the previous mining history is excluded.

No backfill. Operational events, results and the P5 materialized view are retained.
"""

from alembic import op
import sqlalchemy as sa
from infrastructure.persistence.mining_immutability import install_immutability

revision = "20261012_01"
down_revision = "20261007_01"
branch_labels = depends_on = None

# Fixed DDL: future ORM changes cannot alter this revision.
DDL = (
    "CREATE TABLE pillar_evaluations (\n\tevaluation_id VARCHAR(36) NOT NULL, \n\tevent_id INTEGER NOT NULL, \n\tevaluation_as_of TIMESTAMP WITH TIME ZONE NOT NULL, \n\tschema_version SMALLINT NOT NULL, \n\tcontent_hash VARCHAR(64) NOT NULL, \n\tcontext JSONB NOT NULL, \n\tinputs JSONB NOT NULL, \n\tconfiguration JSONB NOT NULL, \n\tversions JSONB NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tPRIMARY KEY (evaluation_id)\n)",
    "CREATE INDEX ix_pillar_evaluation_event_time ON pillar_evaluations (event_id, evaluation_as_of)",
    "CREATE TABLE event_outcome_observations (\n\tobservation_id VARCHAR(36) NOT NULL, \n\tevent_id INTEGER NOT NULL, \n\tsource VARCHAR(50) NOT NULL, \n\tsource_event_id TEXT, \n\tsource_observed_at TIMESTAMP WITH TIME ZONE, \n\treceived_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tpayload JSONB NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tPRIMARY KEY (observation_id)\n)",
    "CREATE INDEX ix_outcome_event_received ON event_outcome_observations (event_id, received_at)",
    "CREATE TABLE pillar_mining_runs (\n\tid BIGSERIAL NOT NULL, \n\tevent_id INTEGER NOT NULL, \n\tevaluation_id VARCHAR(36) NOT NULL, \n\tcontent_hash VARCHAR(64) NOT NULL, \n\tpillar_id VARCHAR(100) NOT NULL, \n\tresult_scope VARCHAR(100) NOT NULL, \n\tengine_version VARCHAR(150) NOT NULL, \n\tpayload_schema_version SMALLINT NOT NULL, \n\tstatus VARCHAR(20) NOT NULL, \n\tevaluation_minute SMALLINT, \n\ttarget_minute SMALLINT, \n\tcalculated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tsport VARCHAR(50) NOT NULL, \n\tcompetition_id INTEGER, \n\tcontext JSONB NOT NULL, \n\tinputs JSONB NOT NULL, \n\tdiagnostics JSONB NOT NULL, \n\toutput_payload JSONB NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT ck_pillar_mining_run_payload_schema_version_positive CHECK (payload_schema_version >= 1), \n\tCONSTRAINT ck_pillar_mining_run_status CHECK (status IN ('ACTIVE', 'INSUFFICIENT_DATA', 'ERROR', 'SKIPPED')), \n\tCONSTRAINT uq_pillar_mining_run_identity UNIQUE (evaluation_id, pillar_id, result_scope), \n\tFOREIGN KEY(evaluation_id) REFERENCES pillar_evaluations (evaluation_id)\n)",
    "CREATE INDEX idx_pillar_mining_run_event_pillar_evaluation ON pillar_mining_runs (event_id, pillar_id, evaluation_minute)",
    "CREATE INDEX idx_pillar_mining_run_pillar_status_calculated ON pillar_mining_runs (pillar_id, status, calculated_at)",
    "CREATE INDEX idx_pillar_mining_run_sport_pillar ON pillar_mining_runs (sport, pillar_id)",
    "CREATE TABLE pillar_mining_units (\n\tid BIGSERIAL NOT NULL, \n\trun_id BIGINT NOT NULL, \n\tparent_unit_id BIGINT, \n\tunit_type VARCHAR(50) NOT NULL, \n\tunit_key VARCHAR(512) NOT NULL, \n\tordinal INTEGER, \n\tmodule_id VARCHAR(100), \n\tstatus VARCHAR(20) NOT NULL, \n\tsignal_axis VARCHAR(50), \n\tis_valid BOOLEAN, \n\tscore_name VARCHAR(100), \n\tscore NUMERIC, \n\tdirection VARCHAR(50), \n\tstrength VARCHAR(50), \n\ttarget_minute SMALLINT, \n\tmarket_type_id SMALLINT, \n\tline_value NUMERIC, \n\tchoice_name TEXT, \n\tbookie_id INTEGER, \n\treason VARCHAR(80), \n\tquote_id BIGINT, \n\tsource VARCHAR(50), \n\texchange_side VARCHAR(20), \n\texchange_level SMALLINT, \n\tdimensions JSONB NOT NULL, \n\tpayload JSONB NOT NULL, \n\tdiagnostics JSONB NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT ck_pillar_mining_unit_status CHECK ((unit_type = 'summary' AND status IN ('ACTIVE', 'INSUFFICIENT_DATA', 'ERROR', 'SKIPPED')) OR (unit_type != 'summary' AND status IN ('COMPUTED', 'BLOCKED', 'ERROR'))), \n\tCONSTRAINT ck_mining_blocked_reason CHECK (status != 'BLOCKED' OR (reason IS NOT NULL AND length(reason) > 0)), \n\tCONSTRAINT uq_pillar_mining_unit_identity UNIQUE (run_id, unit_key), \n\tFOREIGN KEY(run_id) REFERENCES pillar_mining_runs (id) ON DELETE CASCADE, \n\tFOREIGN KEY(parent_unit_id) REFERENCES pillar_mining_units (id) ON DELETE CASCADE\n)",
    "CREATE INDEX idx_mining_unit_status_reason ON pillar_mining_units (status, reason)",
    "CREATE INDEX idx_pillar_mining_unit_bookie ON pillar_mining_units (bookie_id)",
    "CREATE INDEX idx_pillar_mining_unit_dimensions_gin ON pillar_mining_units USING gin (dimensions)",
    "CREATE INDEX idx_pillar_mining_unit_market_type ON pillar_mining_units (market_type_id, line_value)",
    "CREATE INDEX idx_pillar_mining_unit_parent ON pillar_mining_units (parent_unit_id)",
    "CREATE INDEX idx_pillar_mining_unit_run_parent ON pillar_mining_units (run_id, parent_unit_id)",
    "CREATE INDEX idx_pillar_mining_unit_type_status ON pillar_mining_units (unit_type, status)",
    "CREATE TABLE pillar_mining_metric_values (\n\tid BIGSERIAL NOT NULL, \n\tunit_id BIGINT NOT NULL, \n\tmetric_name TEXT NOT NULL, \n\tmetric_group VARCHAR(100), \n\tvalue_type VARCHAR(10) NOT NULL, \n\tnumeric_value NUMERIC, \n\ttext_value TEXT, \n\tboolean_value BOOLEAN, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tPRIMARY KEY (id), \n\tCONSTRAINT ck_pillar_mining_metric_typed_value CHECK ((value_type = 'number' AND numeric_value IS NOT NULL AND text_value IS NULL AND boolean_value IS NULL) OR (value_type = 'text' AND numeric_value IS NULL AND text_value IS NOT NULL AND boolean_value IS NULL) OR (value_type = 'boolean' AND numeric_value IS NULL AND text_value IS NULL AND boolean_value IS NOT NULL)), \n\tCONSTRAINT uq_pillar_mining_metric_name UNIQUE (unit_id, metric_name), \n\tFOREIGN KEY(unit_id) REFERENCES pillar_mining_units (id) ON DELETE CASCADE\n)",
    "CREATE INDEX idx_pillar_mining_metric_numeric ON pillar_mining_metric_values (metric_name, numeric_value)",
    "CREATE INDEX idx_pillar_mining_metric_text ON pillar_mining_metric_values (metric_name, text_value)",
    "CREATE TABLE p5_memory_samples (\n\tsample_id VARCHAR(36) NOT NULL, \n\trun_id BIGINT, \n\tquery JSONB NOT NULL, \n\tcutoff TIMESTAMP WITH TIME ZONE NOT NULL, \n\tpolicy_version VARCHAR(80) NOT NULL, \n\tsample_size INTEGER NOT NULL, \n\twins_home INTEGER NOT NULL, \n\twins_draw INTEGER NOT NULL, \n\twins_away INTEGER NOT NULL, \n\tacquired_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tcreated_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tPRIMARY KEY (sample_id), \n\tFOREIGN KEY(run_id) REFERENCES pillar_mining_runs (id) ON DELETE CASCADE\n)",
    "CREATE INDEX ix_p5_memory_samples_run_id ON p5_memory_samples (run_id)",
    "CREATE TABLE p5_memory_sample_members (\n\tsample_id VARCHAR(36) NOT NULL, \n\tevent_id INTEGER NOT NULL, \n\tstarts_at TIMESTAMP WITH TIME ZONE NOT NULL, \n\tcompetition_id INTEGER, \n\tseason_id INTEGER, \n\tcountry TEXT, \n\todds_home NUMERIC(8, 3) NOT NULL, \n\todds_draw NUMERIC(8, 3), \n\todds_away NUMERIC(8, 3) NOT NULL, \n\thome_score INTEGER NOT NULL, \n\taway_score INTEGER NOT NULL, \n\twinner_side VARCHAR(30) NOT NULL, \n\toutcome VARCHAR(10) NOT NULL, \n\tlast_sync_at TIMESTAMP WITH TIME ZONE, \n\tPRIMARY KEY (sample_id, event_id), \n\tFOREIGN KEY(sample_id) REFERENCES p5_memory_samples (sample_id) ON DELETE CASCADE\n)",
    "CREATE INDEX ix_p5_sample_page ON p5_memory_sample_members (sample_id, starts_at, event_id)",
)


def upgrade():
    for name in (
        "p5_memory_sample_members",
        "p5_memory_samples",
        "pillar_mining_metric_values",
        "pillar_mining_units",
        "pillar_mining_runs",
    ):
        op.execute(sa.text(f"DROP TABLE IF EXISTS {name}"))
    for statement in DDL:
        op.execute(sa.text(statement))
    install_immutability(op.get_bind())


def downgrade():
    raise RuntimeError(
        "The no-backfill mining transition is irreversible; restore a backup to revert it"
    )
