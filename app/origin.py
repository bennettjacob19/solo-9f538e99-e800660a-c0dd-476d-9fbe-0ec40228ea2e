"""A small, deterministic local origin server for the distribution drill.

The origin keeps two versions of an image layer.  Both versions have exactly
the same length but different bytes, so a paused client cannot notice a swap
from the size alone.  The main download snapshots the version when the request
starts; therefore an in-flight response keeps serving the old version after
``/origin/switch`` is called.

Range requests are conditional (``If-Range`` with the strong ETag).  An old
ETag after a version swap makes the origin fall back to a full ``200`` response
for the *new* version, which is exactly what a correct gateway must detect.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class OriginVersion:
    name: str
    data: bytes
    sha256: str
    etag: str

    @property
    def size(self) -> int:
        return len(self.data)


def build_version(name: str, size: int, seed: int) -> OriginVersion:
    rng = random.Random(seed)
    data = rng.randbytes(size)
    digest = hashlib.sha256(data).hexdigest()
    # A quoted value without the W/ prefix is a strong validator.
    return OriginVersion(name=name, data=data, sha256=digest, etag=f'"{name}-{digest}"')


@dataclass
class _RequestRecord:
    id: int
    kind: str
    range_header: Optional[str]
    if_range: Optional[str]
    requested_version: str
    etag: str
    status: int
    started_at: str
    bytes_sent: int = 0
    finished_at: Optional[str] = None
    outcome: str = "in_flight"


class OriginStore:
    """Two equal-length versions, request accounting and snapshot semantics."""

    def __init__(self, size: int, chunk_size: int) -> None:
        if size <= 0:
            raise ValueError("layer size must be positive")
        self.chunk_size = max(1, chunk_size)
        self._versions = {
            "v1": build_version("v1", size, seed=20261123),
            "v2": build_version("v2", size, seed=20261124),
        }
        self._current_name = "v1"
        self._lock = threading.Lock()
        self._reset_stats_locked()

    # ------------------------------------------------------------------ state
    def _reset_stats_locked(self) -> None:
        self._main_requests = 0
        self._range_requests = 0
        self._bytes_sent = 0
        self._request_seq = 0
        self._requests: list[_RequestRecord] = []

    def reset(self, size: Optional[int] = None) -> None:
        with self._lock:
            if size is not None and size != self._versions["v1"].size:
                self._versions = {
                    "v1": build_version("v1", size, seed=20261123),
                    "v2": build_version("v2", size, seed=20261124),
                }
            self._current_name = "v1"
            self._reset_stats_locked()

    def current(self) -> OriginVersion:
        with self._lock:
            return self._versions[self._current_name]

    def current_name(self) -> str:
        with self._lock:
            return self._current_name

    def version_names(self) -> list[str]:
        return list(self._versions)

    def get(self, name: str) -> OriginVersion:
        try:
            return self._versions[name]
        except KeyError as exc:  # pragma: no cover - defensive
            raise KeyError(f"unknown origin version {name!r}") from exc

    def switch(self, name: str = "v2") -> dict:
        with self._lock:
            if name not in self._versions:
                raise KeyError(f"unknown origin version {name!r}")
            previous = self._current_name
            self._current_name = name
            return {
                "from": previous,
                "to": name,
                "changed": previous != name,
                "at": _utc_now(),
            }

    # --------------------------------------------------------------- metrics
    def begin_request(
        self,
        *,
        kind: str,
        range_header: Optional[str],
        if_range: Optional[str],
        version: OriginVersion,
        status: int,
    ) -> _RequestRecord:
        with self._lock:
            self._request_seq += 1
            record = _RequestRecord(
                id=self._request_seq,
                kind=kind,
                range_header=range_header,
                if_range=if_range,
                requested_version=version.name,
                etag=version.etag,
                status=status,
                started_at=_utc_now(),
            )
            self._requests.append(record)
            if len(self._requests) > 50:
                del self._requests[: len(self._requests) - 50]
            if kind == "main":
                self._main_requests += 1
            elif kind == "range":
                self._range_requests += 1
            return record

    def record_sent(self, record: _RequestRecord, amount: int) -> None:
        if amount <= 0:
            return
        with self._lock:
            record.bytes_sent += amount
            self._bytes_sent += amount

    def finish_request(self, record: _RequestRecord, outcome: str = "ok") -> None:
        with self._lock:
            record.finished_at = _utc_now()
            record.outcome = outcome

    def stats(self) -> dict:
        with self._lock:
            current = self._versions[self._current_name]
            return {
                "current_version": self._current_name,
                "current_etag": current.etag,
                "current_sha256": current.sha256,
                "main_requests": self._main_requests,
                "range_requests": self._range_requests,
                "total_requests": len(self._requests),
                "bytes_sent": self._bytes_sent,
                "requests": [
                    {
                        "id": r.id,
                        "kind": r.kind,
                        "range": r.range_header,
                        "if_range": r.if_range,
                        "version": r.requested_version,
                        "etag": r.etag,
                        "status": r.status,
                        "bytes_sent": r.bytes_sent,
                        "outcome": r.outcome,
                        "started_at": r.started_at,
                        "finished_at": r.finished_at,
                    }
                    for r in reversed(self._requests)
                ],
            }

    def describe_versions(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "name": v.name,
                    "size": v.size,
                    "sha256": v.sha256,
                    "etag": v.etag,
                }
                for v in self._versions.values()
            ]


def _parse_range(header: str, size: int) -> Optional[tuple[int, int]]:
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", header.strip())
    if not match:
        return None
    raw_start, raw_end = match.groups()
    if raw_start == "" and raw_end == "":
        return None
    if raw_start == "":
        suffix = int(raw_end)
        if suffix <= 0:
            return None
        start = max(0, size - suffix)
        end = size - 1
    else:
        start = int(raw_start)
        end = int(raw_end) if raw_end else size - 1
    if start >= size or start > end:
        return None
    return start, min(end, size - 1)


def _stream_body(
    store: OriginStore,
    version: OriginVersion,
    start: int,
    end: int,
    record: _RequestRecord,
):
    async def generator():
        try:
            pos = start
            while pos <= end:
                chunk = version.data[pos : min(pos + store.chunk_size, end + 1)]
                pos += len(chunk)
                store.record_sent(record, len(chunk))
                yield chunk
                await asyncio.sleep(0)
            store.finish_request(record, "ok")
        except (asyncio.CancelledError, GeneratorExit):
            store.finish_request(record, "aborted")
            raise

    return generator()


def make_origin_router(store: OriginStore) -> APIRouter:
    router = APIRouter(prefix="/origin", tags=["origin"])

    @router.get("/layer")
    async def get_layer(request: Request):
        # Snapshot the version before streaming.  This is what keeps an
        # already-started main download on the old bytes after a swap.
        version = store.current()
        range_header = request.headers.get("range")
        if_range = request.headers.get("if-range")

        if range_header and if_range is not None and if_range != version.etag:
            # RFC 9110: a failed If-Range means "ignore Range", so the full
            # representation of the *current* version is returned as 200.
            record = store.begin_request(
                kind="range",
                range_header=range_header,
                if_range=if_range,
                version=version,
                status=200,
            )
            headers = {
                "ETag": version.etag,
                "Content-Length": str(version.size),
                "Accept-Ranges": "bytes",
                "X-Content-SHA256": version.sha256,
                "X-Origin-Version": version.name,
                "X-If-Range-Matched": "false",
            }
            return StreamingResponse(
                _stream_body(store, version, 0, version.size - 1, record),
                status_code=200,
                headers=headers,
                media_type="application/octet-stream",
            )

        if range_header:
            parsed = _parse_range(range_header, version.size)
            if parsed is None:
                record = store.begin_request(
                    kind="range",
                    range_header=range_header,
                    if_range=if_range,
                    version=version,
                    status=416,
                )
                store.finish_request(record, "invalid_range")
                return Response(
                    status_code=416,
                    headers={
                        "Content-Range": f"bytes */{version.size}",
                        "ETag": version.etag,
                        "X-Origin-Version": version.name,
                    },
                )
            start, end = parsed
            record = store.begin_request(
                kind="range",
                range_header=range_header,
                if_range=if_range,
                version=version,
                status=206,
            )
            headers = {
                "ETag": version.etag,
                "Content-Length": str(end - start + 1),
                "Content-Range": f"bytes {start}-{end}/{version.size}",
                "Accept-Ranges": "bytes",
                "X-Content-SHA256": version.sha256,
                "X-Origin-Version": version.name,
                "X-If-Range-Matched": "true" if if_range else "not_sent",
            }
            return StreamingResponse(
                _stream_body(store, version, start, end, record),
                status_code=206,
                headers=headers,
                media_type="application/octet-stream",
            )

        record = store.begin_request(
            kind="main",
            range_header=None,
            if_range=None,
            version=version,
            status=200,
        )
        headers = {
            "ETag": version.etag,
            "Content-Length": str(version.size),
            "Accept-Ranges": "bytes",
            "X-Content-SHA256": version.sha256,
            "X-Origin-Version": version.name,
        }
        return StreamingResponse(
            _stream_body(store, version, 0, version.size - 1, record),
            status_code=200,
            headers=headers,
            media_type="application/octet-stream",
        )

    @router.post("/switch")
    async def switch_version(version: str = "v2"):
        try:
            result = store.switch(version)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, **result, "state": store.stats()}

    @router.post("/reset")
    async def reset_origin(size_bytes: Optional[int] = None):
        if size_bytes is not None and not (
            1024 <= size_bytes <= 512 * 1024 * 1024
        ):
            raise HTTPException(
                status_code=400,
                detail="size_bytes must be between 1024 and 536870912",
            )
        store.reset(size_bytes)
        return {
            "ok": True,
            "state": store.stats(),
            "versions": store.describe_versions(),
        }

    @router.get("/state")
    async def origin_state():
        return {
            "state": store.stats(),
            "versions": store.describe_versions(),
        }

    return router
