"""Add frozen historical populations for P5 v4 without changing old results."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261002_01"
down_revision = "20261001_01"
branch_labels = None
depends_on = None


def _sample_columns():
    return (
        sa.Column("sample_id", sa.String(36), primary_key=True),
        sa.Column(
            "run_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            sa.ForeignKey("pillar_mining_runs.id", ondelete="CASCADE"),
        ),
        sa.Column(
            "query", sa.JSON().with_variant(JSONB(), "postgresql"), nullable=False
        ),
        sa.Column("cutoff", sa.DateTime(timezone=True), nullable=False),
        sa.Column("policy_version", sa.String(80), nullable=False),
        sa.Column("sample_size", sa.Integer(), nullable=False),
        sa.Column("wins_home", sa.Integer(), nullable=False),
        sa.Column("wins_draw", sa.Integer(), nullable=False),
        sa.Column("wins_away", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )


def _member_columns():
    return (
        sa.Column(
            "sample_id",
            sa.String(36),
            sa.ForeignKey("p5_memory_samples.sample_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("event_id", sa.Integer(), primary_key=True),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("competition_id", sa.Integer()),
        sa.Column("season_id", sa.Integer()),
        sa.Column("country", sa.Text()),
        sa.Column("odds_home", sa.Numeric(8, 3), nullable=False),
        sa.Column("odds_draw", sa.Numeric(8, 3)),
        sa.Column("odds_away", sa.Numeric(8, 3), nullable=False),
        sa.Column("home_score", sa.Integer(), nullable=False),
        sa.Column("away_score", sa.Integer(), nullable=False),
        sa.Column("winner_side", sa.String(30), nullable=False),
        sa.Column("outcome", sa.String(10), nullable=False),
        sa.Column("last_sync_at", sa.DateTime(timezone=True)),
    )


_INDEXES = (
    ("ix_p5_memory_samples_run_id", "p5_memory_samples", ("run_id",)),
    (
        "ix_p5_sample_page",
        "p5_memory_sample_members",
        ("sample_id", "starts_at", "event_id"),
    ),
)


def _validate_existing_table(inspector, name, columns, dialect):
    """Adopt prior ORM-created tables only when their audit contract matches.

    A name-only IF NOT EXISTS would hide schema drift and incorrectly advance
    Alembic. Keep this revision self-contained rather than importing live ORM
    models, whose definitions may change in later application versions.
    """
    expected = {column.name: column for column in columns}
    actual = {column["name"]: column for column in inspector.get_columns(name)}
    problems = []
    if actual.keys() != expected.keys():
        problems.append(
            f"columns differ (missing={sorted(expected.keys() - actual.keys())}, "
            f"unexpected={sorted(actual.keys() - expected.keys())})"
        )
    for key in sorted(expected.keys() & actual.keys()):
        expected_type = expected[key].type.compile(dialect=dialect)
        actual_type = actual[key]["type"].compile(dialect=dialect)
        if actual_type != expected_type:
            problems.append(f"{key} type is {actual_type}, expected {expected_type}")
        if actual[key]["nullable"] != expected[key].nullable:
            problems.append(f"{key} nullability differs")
    expected_pk = [column.name for column in columns if column.primary_key]
    if inspector.get_pk_constraint(name)["constrained_columns"] != expected_pk:
        problems.append(f"primary key must be {expected_pk}")
    expected_fks = {
        ((column.name,), fk.target_fullname, (fk.ondelete or "").upper())
        for column in columns
        for fk in column.foreign_keys
    }
    actual_fks = {
        (
            tuple(fk["constrained_columns"]),
            (
                (
                    f"{fk['referred_schema']}."
                    if fk.get("referred_schema")
                    not in (None, inspector.default_schema_name)
                    else ""
                )
                + f"{fk['referred_table']}.{'.'.join(fk['referred_columns'])}"
            ),
            (fk.get("options", {}).get("ondelete") or "").upper(),
        )
        for fk in inspector.get_foreign_keys(name)
    }
    if actual_fks != expected_fks:
        problems.append("foreign keys must match the expected ON DELETE CASCADE targets")
    indexes = {index["name"]: index for index in inspector.get_indexes(name)}
    for index_name, table_name, index_columns in _INDEXES:
        index = indexes.get(index_name) if table_name == name else None
        if index is not None and (
            tuple(index["column_names"]) != index_columns
            or index["unique"]
            or index.get("dialect_options", {}).get("postgresql_where") is not None
        ):
            problems.append(f"index {index_name} has an incompatible definition")
    if problems:
        raise RuntimeError(
            f"Cannot adopt existing {name}: incompatible schema: " + "; ".join(problems)
        )


def upgrade():
    tables = {
        "p5_memory_samples": _sample_columns(),
        "p5_memory_sample_members": _member_columns(),
    }
    # Offline SQL targets a fresh schema. Online upgrades also reconcile tables
    # previously created outside Alembic, preserving their rows.
    inspector = None if op.get_context().as_sql else sa.inspect(op.get_bind())
    existing = {
        name for name in tables if inspector is not None and inspector.has_table(name)
    }
    for name in existing:
        _validate_existing_table(inspector, name, tables[name], op.get_bind().dialect)
    # Validate every pre-existing table before making any schema changes.
    for name, columns in tables.items():
        if name not in existing:
            op.create_table(name, *columns)
    for index_name, name, index_columns in _INDEXES:
        present = (
            {index["name"] for index in inspector.get_indexes(name)}
            if name in existing
            else set()
        )
        if index_name not in present:
            op.create_index(index_name, name, list(index_columns))


def downgrade():
    op.drop_table("p5_memory_sample_members")
    op.drop_table("p5_memory_samples")
