"""Bounded retry memory for missing odds, preserving attempts at critical moments."""

from collections import OrderedDict
from threading import Lock
from time import monotonic


class MissingOddsCooldown:
    def __init__(self, seconds, capacity):
        self.seconds, self.capacity = seconds, capacity
        self._entries = OrderedDict()
        self._lock = Lock()

    def allows(self, source_id, moment, critical_moments):
        with self._lock:
            entry = self._entries.get(source_id)
            if entry is None:
                return True
            expires, attempted_moment = entry
            if monotonic() >= expires:
                self._entries.pop(source_id)
                return True
            return moment in critical_moments and moment != attempted_moment

    def missing(self, source_id, moment):
        with self._lock:
            self._entries[source_id] = (monotonic() + self.seconds, moment)
            self._entries.move_to_end(source_id)
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)

    def available(self, source_id):
        with self._lock:
            self._entries.pop(source_id, None)
