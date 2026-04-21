import logging
from contextlib import contextmanager
from threading import local


logger = logging.getLogger(__name__)
_LOG_CONTEXT = local()


class _OddsPortalLogPrefixFilter(logging.Filter):
    """Prefix this module's logs with the active worker/priming label."""

    def filter(self, record: logging.LogRecord) -> bool:
        prefix = getattr(_LOG_CONTEXT, "prefix", None)
        if prefix and not getattr(record, "_op_prefix_applied", False):
            record.msg = f"{prefix} {record.msg}"
            record._op_prefix_applied = True
        return True


if not any(isinstance(existing_filter, _OddsPortalLogPrefixFilter) for existing_filter in logger.filters):
    logger.addFilter(_OddsPortalLogPrefixFilter())


@contextmanager
def _log_prefix(prefix: str | None):
    previous_prefix = getattr(_LOG_CONTEXT, "prefix", None)
    _LOG_CONTEXT.prefix = prefix
    try:
        yield
    finally:
        if previous_prefix is None:
            try:
                delattr(_LOG_CONTEXT, "prefix")
            except AttributeError:
                pass
        else:
            _LOG_CONTEXT.prefix = previous_prefix


log_prefix = _log_prefix

