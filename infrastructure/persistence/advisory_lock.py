"""Session exclusion with no transaction retained during external I/O."""

from contextlib import contextmanager
from sqlalchemy import text
from .database import db_manager


@contextmanager
def exclusive_slot(namespace, slot):
    if db_manager.engine.dialect.name != "postgresql":
        yield True
        return
    with db_manager.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        parameters = {"namespace": namespace, "slot": slot}
        acquired = connection.execute(
            text("SELECT pg_try_advisory_lock(:namespace, :slot)"), parameters
        ).scalar()
        try:
            yield acquired
        finally:
            if acquired:
                connection.execute(text("SELECT pg_advisory_unlock(:namespace, :slot)"), parameters)
