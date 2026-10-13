"""Database enforcement for the append-only audit boundary."""

from sqlalchemy import text

IMMUTABLE_TABLES = (
    "pillar_evaluations",
    "pillar_mining_runs",
    "pillar_mining_units",
    "pillar_mining_metric_values",
    "event_outcome_observations",
)


def install_immutability(connection):
    if connection.dialect.name == "sqlite":
        for table in IMMUTABLE_TABLES:
            for operation in ("UPDATE", "DELETE"):
                connection.execute(
                    text(
                        f"CREATE TRIGGER IF NOT EXISTS immutable_{table}_{operation.lower()} "
                        f"BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable audit record'); END"
                    )
                )
        for operation in ("UPDATE", "DELETE"):
            connection.execute(
                text(
                    f"CREATE TRIGGER IF NOT EXISTS immutable_p5_sample_{operation.lower()} "
                    f"BEFORE {operation} ON p5_memory_samples WHEN OLD.run_id IS NOT NULL "
                    "BEGIN SELECT RAISE(ABORT, 'immutable captured sample'); END"
                )
            )
        for operation in ("INSERT", "UPDATE", "DELETE"):
            row = "NEW" if operation == "INSERT" else "OLD"
            extra = (
                " OR EXISTS (SELECT 1 FROM p5_memory_samples WHERE sample_id=NEW.sample_id AND run_id IS NOT NULL)"
                if operation == "UPDATE"
                else ""
            )
            connection.execute(
                text(
                    f"CREATE TRIGGER IF NOT EXISTS immutable_p5_member_{operation.lower()} "
                    f"BEFORE {operation} ON p5_memory_sample_members WHEN EXISTS "
                    f"(SELECT 1 FROM p5_memory_samples WHERE sample_id={row}.sample_id AND run_id IS NOT NULL){extra} "
                    "BEGIN SELECT RAISE(ABORT, 'immutable captured sample member'); END"
                )
            )
        return
    connection.execute(
        text(
            """CREATE OR REPLACE FUNCTION reject_audit_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable audit record'; END; $$"""
        )
    )
    for table in IMMUTABLE_TABLES:
        connection.execute(
            text(
                f"CREATE TRIGGER immutable_{table} BEFORE UPDATE OR DELETE ON {table} "
                "FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation()"
            )
        )
    connection.execute(
        text("""CREATE OR REPLACE FUNCTION guard_p5_sample() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
        IF OLD.run_id IS NOT NULL THEN RAISE EXCEPTION 'immutable captured sample'; END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END; $$""")
    )
    connection.execute(
        text(
            "CREATE TRIGGER immutable_p5_sample BEFORE UPDATE OR DELETE ON p5_memory_samples "
            "FOR EACH ROW EXECUTE FUNCTION guard_p5_sample()"
        )
    )
    connection.execute(
        text("""CREATE OR REPLACE FUNCTION guard_p5_member() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE owner text; BEGIN
        IF TG_OP = 'INSERT' THEN owner := NEW.sample_id; ELSE owner := OLD.sample_id; END IF;
        IF EXISTS (SELECT 1 FROM p5_memory_samples WHERE sample_id=owner AND run_id IS NOT NULL)
        THEN RAISE EXCEPTION 'immutable captured sample member'; END IF;
        IF TG_OP = 'UPDATE' AND EXISTS (SELECT 1 FROM p5_memory_samples WHERE sample_id=NEW.sample_id AND run_id IS NOT NULL)
        THEN RAISE EXCEPTION 'immutable captured sample member'; END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END; $$""")
    )
    connection.execute(
        text(
            "CREATE TRIGGER immutable_p5_member BEFORE INSERT OR UPDATE OR DELETE ON p5_memory_sample_members "
            "FOR EACH ROW EXECUTE FUNCTION guard_p5_member()"
        )
    )
