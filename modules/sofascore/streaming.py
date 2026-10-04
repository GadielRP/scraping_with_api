"""Bulk JSON on disk; libcurl writes chunks without building Response.content."""

from contextlib import contextmanager
from tempfile import TemporaryFile
import re
import ijson
from ijson.common import ObjectBuilder
from infrastructure.settings.job_execution import JobExecutionSettings
from shared.execution_context import check_execution_budget


class BoundedBody:
    def __init__(self, file, maximum):
        self.file, self.maximum = file, maximum
        self.size = 0
        self.error = None

    def write(self, data):
        try:
            check_execution_budget()
            if self.size + len(data) > self.maximum:
                raise ValueError("Provider response exceeds byte budget")
            count = self.file.write(data)
            self.size += count
            return count
        except BaseException as exc:
            self.error = exc
            return 0  # Abort libcurl; the caller re-raises the original error.

    def seek(self, *args):
        return self.file.seek(*args)

    def truncate(self):
        self.size = 0
        return self.file.truncate()

    def read(self, *args):
        return self.file.read(*args)


@contextmanager
def bulk_document(client, endpoint):
    limits = JobExecutionSettings()
    # data/ is disk-backed in compose; TemporaryFile is unlinked automatically on POSIX.
    from pathlib import Path

    directory = Path("data/runtime")
    directory.mkdir(parents=True, exist_ok=True)
    with TemporaryFile(dir=directory) as file:
        body = BoundedBody(file, limits.response_max_bytes)
        response = client.request_json(endpoint, body_file=body)
        if body.error:
            raise body.error
        if response is None:
            raise RuntimeError(f"Incomplete provider response: {endpoint}")
        yield body


class TokenBoundedReader:
    """Reject oversized JSON tokens before the SAX parser constructs a scalar."""

    _STRING_BOUNDARY = re.compile(rb'["\\]')
    _TOKEN_BOUNDARY = re.compile(rb'["{}\[\]:,\s]')

    def __init__(self, document, maximum, maximum_depth):
        self.document, self.maximum = document, maximum
        self.maximum_depth, self.depth = maximum_depth, 0
        self.string = self.escaped = False
        self.token_bytes = 0

    def read(self, size=-1):
        data = self.document.read(min(size, 65536) if size >= 0 else 65536)
        offset = 0
        while offset < len(data):
            if self.escaped:
                self.token_bytes += 1
                self.escaped = False
                offset += 1
            else:
                pattern = self._STRING_BOUNDARY if self.string else self._TOKEN_BOUNDARY
                boundary = pattern.search(data, offset)
                self.token_bytes += (boundary.start() if boundary else len(data)) - offset
                if self.token_bytes > self.maximum:
                    raise ValueError("Provider JSON token exceeds byte budget")
                if boundary is None:
                    break
                byte = data[boundary.start()]
                offset = boundary.end()
                if not self.string:
                    self.depth += int(byte in (123, 91)) - int(byte in (125, 93))
                    if self.depth > self.maximum_depth:
                        raise ValueError("Provider JSON nesting exceeds depth budget")
                if self.string and byte == 92:
                    self.token_bytes += 1
                    self.escaped = True
                else:
                    if byte == 34:
                        self.string = not self.string
                    self.token_bytes = 0
            if self.token_bytes > self.maximum:
                raise ValueError("Provider JSON token exceeds byte budget")
        return data


def document_entries(document, field, *, mapping=False, controls=None):
    """Build a single bounded member; validate the entire JSON before completion."""
    controls = controls if controls is not None else {}
    limits = JobExecutionSettings()
    maximum = limits.json_item_max_bytes
    found = False
    builder = None
    depth = 0
    size = 0
    member_key = None
    member_prefix = None
    for prefix, event, value in ijson.parse(
        TokenBoundedReader(document, maximum, limits.json_max_depth), use_float=True
    ):
        check_execution_budget()
        if prefix == field and event == ("start_map" if mapping else "start_array"):
            found = True
        if prefix == "hasNextPage" and event == "boolean":
            controls["hasNextPage"] = value
        if mapping and prefix == field and event == "map_key":
            member_key = value
            member_prefix = f"{field}.{value}"
        target = member_prefix if mapping else f"{field}.item"
        if (
            builder is None
            and prefix == target
            and event not in ("map_key", "end_array", "end_map")
        ):
            builder, depth, size = ObjectBuilder(), 0, 0
        if builder is not None:
            size += len(str(value).encode("utf-8")) + 16
            if size > maximum:
                raise ValueError(f"Provider JSON element exceeds budget field={field}")
            builder.event(event, value)
            depth += int(event in ("start_map", "start_array"))
            depth -= int(event in ("end_map", "end_array"))
            if depth == 0:
                yield (member_key, builder.value) if mapping else builder.value
                builder = None
    if not found:
        raise ValueError(f"Provider JSON missing required collection: {field}")
