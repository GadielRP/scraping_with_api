"""Low-overhead process breadcrumbs for crashes, OOM kills, and long jobs."""

from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from datetime import datetime, timezone
import faulthandler
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Iterator

logger = logging.getLogger(__name__)

_STATE_PATH = Path('logs') / 'runtime_state.json'
_FATAL_PATH = Path('logs') / 'fatal_python.log'
_LOCK = threading.Lock()
_STOP = threading.Event()
_THREAD: threading.Thread | None = None
_FATAL_HANDLE = None
_STATE: dict = {}
_OPERATIONS = {}
_CGROUP_V2_ROOT = Path('/sys/fs/cgroup')
_CGROUP_V1_MEMORY_ROOT = Path('/sys/fs/cgroup/memory')


@lru_cache(maxsize=1)
def _windows_memory_api():
    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD),
            ('PeakWorkingSetSize', ctypes.c_size_t), ('WorkingSetSize', ctypes.c_size_t),
            ('QuotaPeakPagedPoolUsage', ctypes.c_size_t), ('QuotaPagedPoolUsage', ctypes.c_size_t),
            ('QuotaPeakNonPagedPoolUsage', ctypes.c_size_t), ('QuotaNonPagedPoolUsage', ctypes.c_size_t),
            ('PagefileUsage', ctypes.c_size_t), ('PeakPagefileUsage', ctypes.c_size_t),
        ]
    current = ctypes.windll.kernel32.GetCurrentProcess
    current.restype = wintypes.HANDLE
    info = ctypes.windll.psapi.GetProcessMemoryInfo
    info.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessMemoryCounters), wintypes.DWORD]
    info.restype = wintypes.BOOL
    return ProcessMemoryCounters, current, info


def _get_rss_mb_windows() -> float | None:
    """Return current working set without rebuilding ctypes classes per sample."""
    try:
        import ctypes
        counters_type, current, info = _windows_memory_api()
        counters = counters_type()
        counters.cb = ctypes.sizeof(counters_type)
        if not info(current(), ctypes.byref(counters), counters.cb):
            return None
        return round(counters.WorkingSetSize / (1024 * 1024), 1)
    except (ImportError, AttributeError, OSError, ValueError, TypeError):
        return None


def get_rss_mb() -> float | None:
    """Return current resident memory without adding a psutil dependency."""
    try:
        with open('/proc/self/status', encoding='ascii') as handle:
            for line in handle:
                if line.startswith('VmRSS:'):
                    return round(int(line.split()[1]) / 1024, 1)
    except (OSError, ValueError, IndexError):
        pass

    try:
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports KiB; macOS reports bytes.
        divisor = 1024 * 1024 if rss > 10_000_000 else 1024
        return round(rss / divisor, 1)
    except (ImportError, OSError, ValueError):
        pass

    # Windows has neither /proc nor the resource module.
    if os.name == 'nt':
        return _get_rss_mb_windows()
    return None


def get_memory_limit_mb() -> float | None:
    """Return the cgroup memory ceiling when the process is container-limited."""
    candidates = (
        Path('/sys/fs/cgroup/memory.max'),
        Path('/sys/fs/cgroup/memory/memory.limit_in_bytes'),
    )
    for path in candidates:
        try:
            raw_value = path.read_text(encoding='ascii').strip()
            if not raw_value or raw_value == 'max':
                continue
            limit_bytes = int(raw_value)
            # Some cgroup v1 hosts expose a huge sentinel for "unlimited".
            if limit_bytes <= 0 or limit_bytes >= 1 << 60:
                continue
            return round(limit_bytes / (1024 * 1024), 1)
        except (OSError, ValueError):
            continue
    return None


def _bytes_to_mb(value: int | None) -> float | None:
    if value is None:
        return None
    return round(value / (1024 * 1024), 1)


def _read_int(path: Path) -> int | None:
    try:
        raw_value = path.read_text(encoding='ascii').strip()
        if not raw_value or raw_value == 'max':
            return None
        return int(raw_value)
    except (OSError, ValueError):
        return None


def _read_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in path.read_text(encoding='ascii').splitlines():
            key, raw_value = line.split(maxsplit=1)
            values[key] = int(raw_value)
    except (OSError, ValueError):
        return {}
    return values


def get_cgroup_memory_snapshot() -> dict[str, float | int | None]:
    """Return total cgroup memory, cache composition, and OOM counters."""
    v2_current = _CGROUP_V2_ROOT / 'memory.current'
    if v2_current.exists():
        memory_stat = _read_key_values(_CGROUP_V2_ROOT / 'memory.stat')
        memory_events = _read_key_values(_CGROUP_V2_ROOT / 'memory.events')
        return {
            'current_mb': _bytes_to_mb(_read_int(v2_current)),
            'kernel_peak_mb': _bytes_to_mb(
                _read_int(_CGROUP_V2_ROOT / 'memory.peak')
            ),
            'anon_mb': _bytes_to_mb(memory_stat.get('anon')),
            'file_mb': _bytes_to_mb(memory_stat.get('file')),
            'oom': memory_events.get('oom'),
            'oom_kill': memory_events.get('oom_kill'),
        }

    v1_current = _CGROUP_V1_MEMORY_ROOT / 'memory.usage_in_bytes'
    memory_stat = _read_key_values(_CGROUP_V1_MEMORY_ROOT / 'memory.stat')
    return {
        'current_mb': _bytes_to_mb(_read_int(v1_current)),
        'kernel_peak_mb': _bytes_to_mb(
            _read_int(_CGROUP_V1_MEMORY_ROOT / 'memory.max_usage_in_bytes')
        ),
        'anon_mb': _bytes_to_mb(
            memory_stat.get('total_rss', memory_stat.get('rss'))
        ),
        'file_mb': _bytes_to_mb(
            memory_stat.get('total_cache', memory_stat.get('cache'))
        ),
        'oom': _read_int(_CGROUP_V1_MEMORY_ROOT / 'memory.failcnt'),
        'oom_kill': None,
    }


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_previous_state() -> dict | None:
    try:
        return json.loads(_STATE_PATH.read_text(encoding='utf-8'))
    except (OSError, ValueError, TypeError):
        return None


def _write_state() -> None:
    with _LOCK:
        _STATE['heartbeat_at_utc'] = _utc_iso()
        _STATE['rss_mb'] = get_rss_mb()
        cgroup_memory = get_cgroup_memory_snapshot()
        _STATE['cgroup_memory_mb'] = cgroup_memory.get('current_mb')
        _STATE['cgroup_anon_mb'] = cgroup_memory.get('anon_mb')
        _STATE['cgroup_file_mb'] = cgroup_memory.get('file_mb')
        _STATE['cgroup_oom'] = cgroup_memory.get('oom')
        _STATE['cgroup_oom_kill'] = cgroup_memory.get('oom_kill')
        payload = dict(_STATE)
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = _STATE_PATH.with_suffix('.tmp')
        temporary_path.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True),
            encoding='utf-8',
        )
        os.replace(temporary_path, _STATE_PATH)


def _heartbeat_loop(interval_seconds):
    last_persisted = 0
    while not _STOP.wait(0.5):
        rss = get_rss_mb()
        with _LOCK:
            for operation in _OPERATIONS.values():
                peak = operation.get('process_peak_rss_mb')
                if rss is not None and (peak is None or rss > peak):
                    operation['process_peak_rss_mb'] = rss
            _STATE['active_operations'] = [dict(item) for item in _OPERATIONS.values()]
        if time.monotonic() - last_persisted >= interval_seconds:
            try:
                _write_state()
            except Exception:
                logger.exception('Could not persist runtime heartbeat')
            last_persisted = time.monotonic()


def start_runtime_observability(interval_seconds: int = 30) -> None:
    """Start one process heartbeat and report evidence from an unclean exit."""
    global _THREAD, _FATAL_HANDLE

    if _THREAD is not None and _THREAD.is_alive():
        return

    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    previous = _read_previous_state()
    if previous and not previous.get('clean_shutdown', False):
        logger.critical(
            'Previous process ended without a clean shutdown: pid=%s '
            'last_heartbeat_utc=%s active_operations=%s rss_mb=%s '
            'cgroup_memory_mb=%s cgroup_oom_kill=%s',
            previous.get('pid'),
            previous.get('heartbeat_at_utc'),
            previous.get('active_operations'),
            previous.get('rss_mb'),
            previous.get('cgroup_memory_mb'),
            previous.get('cgroup_oom_kill'),
        )

    try:
        _FATAL_HANDLE = _FATAL_PATH.open('a', encoding='utf-8')
        _FATAL_HANDLE.write(
            f'\n=== process pid={os.getpid()} started_at_utc={_utc_iso()} ===\n'
        )
        _FATAL_HANDLE.flush()
        faulthandler.enable(file=_FATAL_HANDLE, all_threads=True)
    except OSError:
        logger.exception('Could not enable Python fatal-error logging')

    with _LOCK:
        _STATE.clear()
        _STATE.update(
            pid=os.getpid(),
            started_at_utc=_utc_iso(),
            clean_shutdown=False,
            active_operations=[],
        )
    _STOP.clear()
    _write_state()

    _THREAD = threading.Thread(
        target=_heartbeat_loop,
        args=(max(5, int(interval_seconds)),),
        name='runtime-observability',
        daemon=True,
    )
    _THREAD.start()
    logger.info(
        'Runtime observability started pid=%s rss_mb=%s cgroup=%s '
        'state_file=%s',
        os.getpid(),
        get_rss_mb(),
        get_cgroup_memory_snapshot(),
        _STATE_PATH,
    )


def mark_clean_shutdown() -> None:
    """Persist a clean exit marker; safe to call repeatedly."""
    global _FATAL_HANDLE

    if not _STATE:
        return
    _STOP.set()
    with _LOCK:
        _STATE['clean_shutdown'] = True
        _STATE['active_operations'] = []
    try:
        _write_state()
    except Exception:
        logger.exception('Could not persist clean shutdown marker')

    if _FATAL_HANDLE is not None:
        try:
            _FATAL_HANDLE.flush()
        except OSError:
            pass


@contextmanager
def observe_operation(name):
    """Track concurrent operations; memory samples describe the entire process."""
    from uuid import uuid4
    from shared.execution_context import current_execution
    context = current_execution()
    key = uuid4().hex
    started = time.monotonic()
    start_rss = get_rss_mb()
    operation = dict(name=name, run_id=context.run_id if context else key,
                     thread=threading.current_thread().name, started_at_utc=_utc_iso(),
                     process_peak_rss_mb=start_rss)
    with _LOCK:
        _OPERATIONS[key] = operation
        _STATE['active_operations'] = [dict(item) for item in _OPERATIONS.values()]
    logger.info('Operation started %s process_rss_mb=%s', operation, start_rss)
    try:
        yield
    finally:
        with _LOCK:
            _OPERATIONS.pop(key, None)
            _STATE['active_operations'] = [dict(item) for item in _OPERATIONS.values()]
        logger.info('✅ Operation finished name=%s run_id=%s duration_s=%.3f '
                    'process_rss_mb=%s process_peak_rss_mb=%s', name, operation['run_id'],
                    time.monotonic() - started, get_rss_mb(), operation['process_peak_rss_mb'])
        if _STATE:
            try:
                _write_state()
            except Exception:
                logger.warning('Could not persist operation breadcrumb')
