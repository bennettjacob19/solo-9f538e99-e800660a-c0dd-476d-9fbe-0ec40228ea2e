"""Retention buffer used by the gateway to hold bytes for the slow client.

The buffer enforces a hard cap on the total body it keeps (memory + disk
combined) and prefers memory until ``memory_limit_bytes`` is reached, then
spills to an anonymous temporary file.  Peaks are tracked so the web page can
show how much the gateway actually retained.
"""

from __future__ import annotations

import tempfile
from collections import deque


class RetentionBuffer:
    def __init__(self, capacity_bytes: int, memory_limit_bytes: int) -> None:
        self.capacity_bytes = max(0, int(capacity_bytes))
        self.memory_limit_bytes = max(
            0, min(int(memory_limit_bytes), self.capacity_bytes)
        )
        self._items: deque[list] = deque()  # ["mem", bytes] | ["disk", offset, length]
        self._mem_bytes = 0
        self._disk_bytes = 0
        self._file = tempfile.TemporaryFile() if self.capacity_bytes else None
        self._file_end = 0
        self._peak_total = 0
        self._peak_memory = 0
        self._peak_disk = 0
        self._dropped = 0
        self._closed = False

    # ------------------------------------------------------------------ basics
    @property
    def used_bytes(self) -> int:
        return self._mem_bytes + self._disk_bytes

    @property
    def available_bytes(self) -> int:
        return self.capacity_bytes - self.used_bytes

    @property
    def dropped_bytes(self) -> int:
        return self._dropped

    def append(self, data: bytes) -> int:
        """Append as much as fits; return the accepted byte count."""
        if not data or self._closed:
            return 0
        room = self.available_bytes
        if room <= 0:
            self._dropped += len(data)
            return 0
        accepted = data[:room]
        self._dropped += len(data) - len(accepted)

        mem_room = self.memory_limit_bytes - self._mem_bytes
        take_mem = min(len(accepted), max(0, mem_room))
        if take_mem:
            chunk = accepted[:take_mem]
            self._items.append(["mem", chunk])
            self._mem_bytes += len(chunk)
        rest = accepted[take_mem:]
        if rest:
            assert self._file is not None
            self._file.seek(self._file_end)
            self._file.write(rest)
            self._items.append(["disk", self._file_end, len(rest)])
            self._file_end += len(rest)
            self._disk_bytes += len(rest)

        self._peak_total = max(self._peak_total, self.used_bytes)
        self._peak_memory = max(self._peak_memory, self._mem_bytes)
        self._peak_disk = max(self._peak_disk, self._disk_bytes)
        return len(accepted)

    def read(self, max_bytes: int) -> bytes:
        """Pop and return up to ``max_bytes`` in FIFO order."""
        if max_bytes <= 0:
            return b""
        out = bytearray()
        while self._items and len(out) < max_bytes:
            want = max_bytes - len(out)
            item = self._items[0]
            if item[0] == "mem":
                buf: bytes = item[1]
                take = min(want, len(buf))
                out += buf[:take]
                self._mem_bytes -= take
                if take == len(buf):
                    self._items.popleft()
                else:
                    item[1] = buf[take:]
            else:
                _, offset, length = item
                take = min(want, length)
                assert self._file is not None
                self._file.seek(offset)
                out += self._file.read(take)
                self._disk_bytes -= take
                if take == length:
                    self._items.popleft()
                else:
                    item[1] = offset + take
                    item[2] = length - take
        return bytes(out)

    # ----------------------------------------------------------------- metrics
    def stats(self) -> dict:
        return {
            "capacity_bytes": self.capacity_bytes,
            "memory_limit_bytes": self.memory_limit_bytes,
            "used_bytes": self.used_bytes,
            "used_memory_bytes": self._mem_bytes,
            "used_disk_bytes": self._disk_bytes,
            "peak_bytes": self._peak_total,
            "peak_memory_bytes": self._peak_memory,
            "peak_disk_bytes": self._peak_disk,
            "dropped_bytes": self._dropped,
            "spilled": self._peak_disk > 0,
        }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._items.clear()
        self._mem_bytes = 0
        self._disk_bytes = 0
        if self._file is not None:
            self._file.close()
            self._file = None


def describe_segments(start: int, end: int) -> list[dict]:
    """Inclusive byte range helper for missing-segment reporting."""
    if end < start:
        return []
    return [{"start": start, "end": end, "length": end - start + 1}]
