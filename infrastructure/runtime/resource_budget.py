"""Defer another maintenance unit under sustained host/cgroup pressure."""

from pathlib import Path
from infrastructure.settings.job_execution import JobExecutionSettings
from shared.execution_context import Priority, WorkDeferred, current_priority


def available_memory_mb():
    samples = []
    try:
        fields = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        samples.append(int(fields["MemAvailable"].split()[0]) / 1024)
    except (OSError, ValueError, KeyError):
        pass
    try:
        maximum = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if maximum != "max":
            current = int(Path("/sys/fs/cgroup/memory.current").read_text())
            samples.append((int(maximum) - current) / 1024**2)
    except (OSError, ValueError):
        pass
    return min(samples) if samples else None


def check_maintenance_capacity():
    if current_priority() != Priority.MAINTENANCE:
        return
    available = available_memory_mb()
    minimum = JobExecutionSettings().maintenance_min_available_mb
    if available is not None and available < minimum:
        raise WorkDeferred(
            f"Insufficient memory headroom available_mb={available:.1f} minimum_mb={minimum}"
        )
