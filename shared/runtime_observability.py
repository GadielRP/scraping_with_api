"""Low-overhead process breadcrumbs for crashes, OOM kills, and long jobs."""

from __future__ import annotations

from contextlib import contextmanager
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
_CGROUP_V2_ROOT = Path('/sys/fs/cgroup')
_CGROUP_V1_MEMORY_ROOT = Path('/sys/fs/cgroup/memory')


def _get_rss_mb_windows() -> float | None:
    """Return current working-set size on Windows via WinAPI (no psutil)."""
    try:
        import ctypes
        from ctypes import wintypes
    except ImportError:
        return None

    class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    try:
        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
        get_current_process = ctypes.windll.kernel32.GetCurrentProcess
        get_current_process.restype = wintypes.HANDLE
        get_process_memory_info = ctypes.windll.psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
            wintypes.DWORD,
        ]
        get_process_memory_info.restype = wintypes.BOOL
        if not get_process_memory_info(
            get_current_process(),
            ctypes.byref(counters),
            counters.cb,
        ):
            return None
        return round(counters.WorkingSetSize / (1024 * 1024), 1)
    except (AttributeError, OSError, ValueError, TypeError):
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


def _heartbeat_loop(interval_seconds: int) -> None:
    while not _STOP.wait(interval_seconds):
        try:
            _write_state()
        except Exception:
            logger.exception('Could not persist runtime heartbeat')


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
            'last_heartbeat_utc=%s active_operation=%s rss_mb=%s '
            'cgroup_memory_mb=%s cgroup_oom_kill=%s',
            previous.get('pid'),
            previous.get('heartbeat_at_utc'),
            previous.get('active_operation'),
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
            active_operation=None,
            operation_started_at_utc=None,
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
        _STATE['active_operation'] = None
        _STATE['operation_started_at_utc'] = None
    try:
        _write_state()
    except Exception:
        logger.exception('Could not persist clean shutdown marker')

    if _FATAL_HANDLE is not None:
        try:
            _FATAL_HANDLE.flush()
        except OSError:
            pass


def _sample_operation_peak(
    stop_event: threading.Event,
    peak_holder: dict[str, float | None],
    sample_interval_seconds: float,
) -> None:
    """Track short RSS spikes and persist a breadcrumb every five seconds."""
    last_persisted = time.monotonic()
    while not stop_event.wait(sample_interval_seconds):
        rss_mb = get_rss_mb()
        cgroup_memory_mb = get_cgroup_memory_snapshot().get('current_mb')
        with _LOCK:
            current_peak = peak_holder.get('rss_mb')
            if rss_mb is not None and (
                current_peak is None or rss_mb > current_peak
            ):
                peak_holder['rss_mb'] = rss_mb
                _STATE['active_operation_peak_rss_mb'] = rss_mb
            current_cgroup_peak = peak_holder.get('cgroup_mb')
            if isinstance(cgroup_memory_mb, (int, float)) and (
                current_cgroup_peak is None
                or cgroup_memory_mb > current_cgroup_peak
            ):
                peak_holder['cgroup_mb'] = cgroup_memory_mb
                _STATE['active_operation_peak_cgroup_memory_mb'] = (
                    cgroup_memory_mb
                )

        if time.monotonic() - last_persisted >= 5:
            try:
                _write_state()
            except Exception:
                logger.warning('Could not persist operation memory sample')
            last_persisted = time.monotonic()


@contextmanager
def observe_operation(name: str) -> Iterator[None]:
    """Log and persist operation duration plus RSS before and after."""
    started = time.monotonic()
    start_rss = get_rss_mb()
    memory_limit_mb = get_memory_limit_mb()
    start_cgroup = get_cgroup_memory_snapshot()
    start_cgroup_mb = start_cgroup.get('current_mb')
    peak_holder = {
        'rss_mb': start_rss,
        'cgroup_mb': (
            float(start_cgroup_mb)
            if isinstance(start_cgroup_mb, (int, float))
            else None
        ),
    }
    sampler_stop = threading.Event()
    sampler_thread = None
    with _LOCK:
        _STATE['active_operation'] = name
        _STATE['operation_started_at_utc'] = _utc_iso()
        _STATE['active_operation_peak_rss_mb'] = start_rss
        _STATE['active_operation_peak_cgroup_memory_mb'] = start_cgroup_mb
        _STATE['memory_limit_mb'] = memory_limit_mb
    try:
        _write_state()
    except Exception:
        logger.warning('Could not persist start breadcrumb for %s', name)

    if start_rss is not None or start_cgroup_mb is not None:
        sampler_thread = threading.Thread(
            target=_sample_operation_peak,
            args=(sampler_stop, peak_holder, 0.25),
            name='operation-memory-sampler',
            daemon=True,
        )
        sampler_thread.start()

    logger.info(
        'Operation started name=%s rss_mb=%s cgroup_memory_mb=%s '
        'cgroup_anon_mb=%s cgroup_file_mb=%s memory_limit_mb=%s '
        'cgroup_oom=%s cgroup_oom_kill=%s',
        name,
        start_rss,
        start_cgroup_mb,
        start_cgroup.get('anon_mb'),
        start_cgroup.get('file_mb'),
        memory_limit_mb,
        start_cgroup.get('oom'),
        start_cgroup.get('oom_kill'),
    )
    try:
        yield
    except BaseException:
        logger.exception(
            'Operation failed name=%s duration_s=%.1f rss_mb=%s',
            name,
            time.monotonic() - started,
            get_rss_mb(),
        )
        raise
    finally:
        sampler_stop.set()
        if sampler_thread is not None:
            sampler_thread.join(timeout=1)
        end_rss = get_rss_mb()
        end_cgroup = get_cgroup_memory_snapshot()
        end_cgroup_mb = end_cgroup.get('current_mb')
        peak_rss = peak_holder.get('rss_mb')
        if end_rss is not None and (peak_rss is None or end_rss > peak_rss):
            peak_rss = end_rss
        peak_cgroup = peak_holder.get('cgroup_mb')
        if isinstance(end_cgroup_mb, (int, float)) and (
            peak_cgroup is None or end_cgroup_mb > peak_cgroup
        ):
            peak_cgroup = float(end_cgroup_mb)
        process_headroom_mb = (
            round(memory_limit_mb - peak_rss, 1)
            if memory_limit_mb is not None and peak_rss is not None
            else None
        )
        cgroup_headroom_mb = (
            round(memory_limit_mb - peak_cgroup, 1)
            if memory_limit_mb is not None and peak_cgroup is not None
            else None
        )
        headroom_mb = (
            cgroup_headroom_mb
            if cgroup_headroom_mb is not None
            else process_headroom_mb
        )
        with _LOCK:
            _STATE['active_operation'] = None
            _STATE['operation_started_at_utc'] = None
            _STATE['active_operation_peak_rss_mb'] = None
            _STATE['active_operation_peak_cgroup_memory_mb'] = None
            _STATE['last_operation'] = name
            _STATE['last_operation_peak_rss_mb'] = peak_rss
            _STATE['last_operation_peak_cgroup_memory_mb'] = peak_cgroup
            _STATE['last_operation_finished_at_utc'] = _utc_iso()
        try:
            _write_state()
        except Exception:
            logger.warning('Could not persist finish breadcrumb for %s', name)
        logger.info(
            'Operation finished name=%s duration_s=%.1f rss_mb=%s '
            'peak_rss_mb=%s cgroup_memory_mb=%s '
            'peak_cgroup_memory_mb=%s cgroup_anon_mb=%s cgroup_file_mb=%s '
            'memory_limit_mb=%s headroom_mb=%s '
            'process_headroom_mb=%s rss_delta_mb=%s '
            'cgroup_oom=%s cgroup_oom_kill=%s',
            name,
            time.monotonic() - started,
            end_rss,
            peak_rss,
            end_cgroup_mb,
            peak_cgroup,
            end_cgroup.get('anon_mb'),
            end_cgroup.get('file_mb'),
            memory_limit_mb,
            headroom_mb,
            process_headroom_mb,
            (
                round(end_rss - start_rss, 1)
                if end_rss is not None and start_rss is not None
                else None
            ),
            end_cgroup.get('oom'),
            end_cgroup.get('oom_kill'),
        )


__all__ = [
    'get_cgroup_memory_snapshot',
    'get_memory_limit_mb',
    'get_rss_mb',
    'mark_clean_shutdown',
    'observe_operation',
    'start_runtime_observability',
]
