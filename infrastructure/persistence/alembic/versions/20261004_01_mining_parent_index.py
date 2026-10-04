"""Index the self-referencing mining FK used by cascading graph replacement."""

from contextlib import nullcontext

from alembic import op
import sqlalchemy as sa

revision = '20261004_01'
down_revision = '20261003_01'
branch_labels = depends_on = None

TABLE = 'pillar_mining_units'
INDEX = 'idx_pillar_mining_unit_parent'


def upgrade():
    context = op.get_context()
    postgres = context.dialect.name == 'postgresql'
    rebuild = False
    if not context.as_sql:
        existing = next(
            (index for index in sa.inspect(op.get_bind()).get_indexes(TABLE)
             if index['name'] == INDEX), None,
        )
        if existing is not None:
            options = existing.get('dialect_options', {})
            if (existing['column_names'] != ['parent_unit_id'] or existing['unique']
                    or options.get('postgresql_where') is not None
                    or options.get('sqlite_where') is not None):
                raise RuntimeError(f'{INDEX} exists with an incompatible definition')
            valid = not postgres or op.get_bind().scalar(sa.text(
                'SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)'
            ), {'name': INDEX})
            if valid:
                return
            # A failed concurrent build can leave an invalid index. Only this
            # migration's matching index is removed before retrying the build.
            rebuild = True
    with context.autocommit_block() if postgres else nullcontext():
        if rebuild:
            op.drop_index(INDEX, table_name=TABLE, postgresql_concurrently=True)
        op.create_index(
            INDEX, TABLE, ['parent_unit_id'], unique=False,
            **({'postgresql_concurrently': True} if postgres else {}),
        )


def downgrade():
    context = op.get_context()
    postgres = context.dialect.name == 'postgresql'
    with context.autocommit_block() if postgres else nullcontext():
        op.drop_index(
            INDEX, table_name=TABLE, if_exists=True,
            **({'postgresql_concurrently': True} if postgres else {}),
        )
