"""Allow second-yellow dismissals in discipline detail and totals."""
from alembic import op

revision = "20261110_01"
down_revision = "20261104_02"
branch_labels = depends_on = None

_TYPE_CHECKS = (
    ("disciplinary_incidents", "ck_discipline_type"),
    ("disciplinary_incidents_totals", "ck_discipline_totals_type"),
)


def _replace_type_checks(condition):
    for table, constraint in _TYPE_CHECKS:
        op.drop_constraint(constraint, table, type_="check")
        op.create_check_constraint(constraint, table, condition)


def upgrade():
    _replace_type_checks("type IN ('yellow_card', 'red_card', 'red_card_second_yellow', 'ejection', 'foul_out')")


def downgrade():
    # PostgreSQL rejects the narrower checks if second-yellow records still exist.
    _replace_type_checks("type IN ('yellow_card', 'red_card', 'ejection', 'foul_out')")
