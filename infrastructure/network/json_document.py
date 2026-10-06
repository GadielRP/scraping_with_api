"""Bounded JSON documents independent of the HTTP library and provider schema."""

from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryFile
from typing import BinaryIO, Iterator, Protocol
import re

import ijson
from ijson.common import ObjectBuilder

from infrastructure.settings.job_execution import JobExecutionSettings
from shared.execution_context import check_execution_budget


class JsonDocument:
    """A disk-backed response body with an enforced download size limit."""

    def __init__(self, file: BinaryIO, maximum: int):
        self.file = file
        self.maximum = maximum
        self.size = 0

    def write(self, data: bytes) -> int:
        check_execution_budget()
        if self.size + len(data) > self.maximum:
            raise ValueError("Provider response exceeds byte budget")
        count = self.file.write(data)
        self.size += count
        return count

    def reset(self) -> None:
        """Discard a failed attempt before retrying the same download."""
        self.file.seek(0)
        self.file.truncate()
        self.size = 0

    def seek(self, *args):
        return self.file.seek(*args)

    def read(self, size=-1):
        return self.file.read(size)


class JsonDownloader(Protocol):
    def download_json(
        self, endpoint: str, document: JsonDocument, params: dict | None = None
    ) -> None:
        """Write a complete successful body, or raise; never decode it to objects."""
        ...


@contextmanager
def open_json_document(
    client: JsonDownloader, endpoint: str, params: dict | None = None
) -> Iterator[JsonDocument]:
    """Own the temporary body for the download and all incremental parsing."""
    directory = Path("data/runtime")
    directory.mkdir(parents=True, exist_ok=True)
    with TemporaryFile(dir=directory) as file:
        document = JsonDocument(file, JobExecutionSettings().response_max_bytes)
        client.download_json(endpoint, document, params=params)
        document.seek(0)
        yield document


class TokenBoundedReader:
    """Reject oversized tokens and nesting before the parser allocates them."""

    _STRING_BOUNDARY = re.compile(rb'["\\]')
    _TOKEN_BOUNDARY = re.compile(rb'["{}\[\]:,\s]')

    def __init__(self, document, maximum, maximum_depth):
        self.document, self.maximum = document, maximum
        self.maximum_depth, self.depth = maximum_depth, 0
        self.string = self.escaped = False
        self.token_bytes = 0

    def read(self, size=-1):
        check_execution_budget()
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


def document_entries(document, field: str | tuple[str, ...], *, mapping=False, controls=None):
    """Yield bounded members; exhaustion validates the whole JSON document.

    Use an empty field for a root array, or a tuple for supported alternative
    collection paths. Only the requested scalar metadata keys are captured.
    """
    fields = (field,) if isinstance(field, str) else field
    controls = controls if controls is not None else {}
    limits = JobExecutionSettings()
    maximum = limits.json_item_max_bytes
    collection = None
    builder = None
    depth = size = 0
    member_key = member_prefix = None
    for prefix, event, value in ijson.parse(
        TokenBoundedReader(document, maximum, limits.json_max_depth), use_float=True
    ):
        if collection is None and prefix in fields and event == (
            "start_map" if mapping else "start_array"
        ):
            collection = prefix
        if prefix in controls and event in ("boolean", "string", "number", "null"):
            controls[prefix] = value
        if collection is None:
            continue
        if mapping and prefix == collection and event == "map_key":
            member_key = value
            member_prefix = f"{collection}.{value}" if collection else str(value)
        target = member_prefix if mapping else f"{collection}.item" if collection else "item"
        if builder is None and prefix == target and event not in (
            "map_key", "end_array", "end_map"
        ):
            builder, depth, size = ObjectBuilder(), 0, 0
        if builder is not None:
            size += len(str(value).encode("utf-8")) + 16
            if size > maximum:
                raise ValueError(f"Provider JSON element exceeds budget field={collection}")
            builder.event(event, value)
            depth += int(event in ("start_map", "start_array"))
            depth -= int(event in ("end_map", "end_array"))
            if depth == 0:
                check_execution_budget()
                yield (member_key, builder.value) if mapping else builder.value
                builder = None
    if collection is None:
        raise ValueError(f"Provider JSON missing required collection: {fields}")
