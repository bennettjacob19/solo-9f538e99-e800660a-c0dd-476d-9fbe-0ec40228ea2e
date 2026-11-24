"""A small, deterministic local origin server for the distribution drill.

The origin keeps two versions of an image layer (``v1`` / ``v2``).  Both
versions have exactly the same length but different bytes, so a paused client
cannot notice a swap from the size alone.  The main download snapshots the
version *generation* when the request starts; therefore an in-flight response
keeps serving the old bytes and the old headers after ``/origin/reset``
resizes the layer or ``/origin/switch`` swaps the version.

Memory budget
-------------
A layer may be 128 MiB while the origin container has a 96 MiB memory limit,
so layer bodies are **never held in RAM**.  They are generated once with the
exact same deterministic Mersenne-Twister sequence as the legacy in-memory
``build_version`` and streamed into anonymous layer files (constant-size
chunks).  Serving uses positional file reads; seeking to a range offset reads
zero body bytes, so a ``206`` response for a range no larger than 64 KiB
reads/generates at most the range length (far below the 1 MiB work budget).
Every request record reports the actual body-byte work
(``body_bytes_used`` and a breakdown).

Generations and reclamation
---------------------------
Every size-changing reset builds a new ``_Generation`` (two files) before
publishing it atomically.  If building fails, the previous generation stays
current and downloadable.  Stream leases keep superseded generations alive
until the last old download finishes or disconnects; then their files are
unlinked and their resources are reclaimed.

Range requests are conditional (``If-Range`` with the strong ETag).  An old
ETag after a version swap makes the origin fall back to a full ``200`` response
for the *new* version, which is exactly what a correct gateway must detect.
"""

from __future__ import annotations

import asyncio
import atexit
import hashlib
import os
import random
import re
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response, StreamingResponse


# Fixed seeds keep "same size + version -> same body / ETag / SHA-256" forever.
V1_SEED = 20261123
V2_SEED = 20261124

# Must stay a multiple of 4: ``Random.randbytes(n)`` is
# ``Random.getrandbits(8*n).to_bytes(n, "little")`` and for block sizes
# divisible by 4 (32 bits per MT word) concatenating block calls is byte-identical
# to one monolithic ``randbytes(size)`` call, including a short final block.
LAYER_BUILD_CHUNK = 64 * 1024

# A 206 whose requested range is <= this many bytes has an explicit work budget
# of SMALL_RANGE_WORK_LIMIT bytes for *locating + sending* the body.
SMALL_RANGE_LIMIT = 64 * 1024
SMALL_RANGE_WORK_LIMIT = 1024 * 1024


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(frozen=True)
class OriginVersion:
    # NOTE: the first four fields keep the historical positional/keyword
    # contract of the in-memory version object.  File-backed versions pass
    # ``data=b""`` plus ``declared_size`` / ``path`` / ``generation``.
    name: str = ""
    data: bytes = b""
    sha256: str = ""
    etag: str = ""
    declared_size: Optional[int] = None
    path: Optional[str] = None
    generation: int = 0

    @property
    def size(self) -> int:
        if self.declared_size is not None:
            return self.declared_size
        return len(self.data)


def build_version(name: str, size: int, seed: int) -> OriginVersion:
    """Legacy full in-memory builder.

    Kept byte-for-byte and signature-compatible for tests/diagnostics and for
    callers that want the whole object at once.  The store itself does not use
    this for large layers; it uses :func:`iter_layer_bytes` into files.
    """
    rng = random.Random(seed)
    data = rng.randbytes(size)
    digest = hashlib.sha256(data).hexdigest()
    # A quoted value without the W/ prefix is a strong validator.
    return OriginVersion(name=name, data=data, sha256=digest, etag=f'"{name}-{digest}"')


def iter_layer_bytes(
    seed: int, size: int, chunk_size: int = LAYER_BUILD_CHUNK
):
    """Yield the exact bytes of the legacy monolithic ``randbytes(size)`` call.

    ``chunk_size`` is rounded down to a multiple of 4 (minimum 4) so every
    non-final block consumes whole 32-bit MT words; the final block may be
    short.  Verified equal to ``random.Random(seed).randbytes(size)`` for all
    sizes.
    """
    if size < 0:
        raise ValueError("size must be non-negative")
    block = max(4, (int(chunk_size) // 4) * 4)
    rng = random.Random(seed)
    remaining = size
    while remaining > 0:
        count = min(block, remaining)
        yield rng.randbytes(count)
        remaining -= count


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
    # Actual body-byte work for locating + sending this response.
    range_length: Optional[int] = None
    positioning_bytes: int = 0
    storage_read_bytes: int = 0
    generated_bytes: int = 0

    @property
    def body_bytes_used(self) -> int:
        """Total body bytes read/generated for positioning and sending."""
        return (
            self.positioning_bytes
            + self.storage_read_bytes
            + self.generated_bytes
        )

    def work_limit_bytes(self) -> Optional[int]:
        """The 1 MiB budget applies only to 206 with a <= 64 KiB range."""
        if self.status == 206 and self.range_length is not None:
            if self.range_length <= SMALL_RANGE_LIMIT:
                return SMALL_RANGE_WORK_LIMIT
        return None

    def work_within_limit(self) -> Optional[bool]:
        limit = self.work_limit_bytes()
        if limit is None:
            return None
        return self.body_bytes_used <= limit


@dataclass
class _Generation:
    """An immutable (size, v1, v2) triple backed by two layer files."""

    gen_id: int
    size: int
    directory: Path
    versions: dict[str, OriginVersion] = field(default_factory=dict)
    paths: list[Path] = field(default_factory=list)
    users: int = 0
    retired: bool = False
    removed: bool = False


class GenerationLease:
    """Idempotent claim on one generation for one in-flight response.

    ``release`` can be called from both the body generator's ``finally`` and a
    response-level safety net; exactly one decrement/reclaim happens.
    """

    __slots__ = ("store", "gen_id", "released")

    def __init__(self, store: "OriginStore", gen_id: int) -> None:
        self.store = store
        self.gen_id = gen_id
        self.released = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self.store._release_gen_id(self.gen_id)


class OriginStore:
    """Deterministic file-backed versions, request accounting and snapshots."""

    def __init__(
        self,
        size: int,
        chunk_size: int,
        data_dir: "str | Path | None" = None,
    ) -> None:
        if size <= 0:
            raise ValueError("layer size must be positive")
        self.chunk_size = max(1, chunk_size)

        if data_dir is None:
            data_dir = os.environ.get("DRILL_ORIGIN_DATA_DIR")
        self._owns_tmpdir: Optional[tempfile.TemporaryDirectory] = None
        if data_dir:
            self._data_dir = Path(data_dir)
            self._data_dir.mkdir(parents=True, exist_ok=True)
        else:
            self._owns_tmpdir = tempfile.TemporaryDirectory(prefix="origin-layers-")
            self._data_dir = Path(self._owns_tmpdir.name)

        self._lock = threading.RLock()
        self._reset_lock = threading.Lock()
        self._gen_seq = 1
        self._gens: dict[int, _Generation] = {}
        generation = self._build_generation(1, size)
        self._gens[1] = generation
        self._current_gen_id = 1
        self._current_name = "v1"
        self._reclaimed_generations = 0
        self._reclaim_errors: list[str] = []
        self._reset_stats_locked()
        atexit.register(self.close)

    # ------------------------------------------------------------ generation
    def _build_generation(self, gen_id: int, size: int) -> _Generation:
        """Generate v1/v2 files + digests with bounded memory.

        On any failure all partial files created by this generation are removed
        and the exception propagates; the caller's current generation is
        untouched.
        """
        generation = _Generation(gen_id=gen_id, size=size, directory=self._data_dir)
        seeds = (("v1", V1_SEED), ("v2", V2_SEED))
        try:
            for name, seed in seeds:
                path = self._data_dir / f"layer-g{gen_id:04d}-{name}.bin"
                digest = self._write_layer_file(path, seed, size)
                generation.paths.append(path)
                etag = f'"{name}-{digest}"'
                generation.versions[name] = OriginVersion(
                    name=name,
                    data=b"",
                    sha256=digest,
                    etag=etag,
                    declared_size=size,
                    path=str(path),
                    generation=gen_id,
                )
        except BaseException:
            for partial in generation.paths:
                try:
                    partial.unlink(missing_ok=True)
                except OSError:
                    pass
            raise
        return generation

    @staticmethod
    def _write_layer_file(path: Path, seed: int, size: int) -> str:
        digest = hashlib.sha256()
        # Buffered write; the streamed chunks are the only large-ish objects.
        with open(path, "wb") as handle:
            for chunk in iter_layer_bytes(seed, size):
                handle.write(chunk)
                digest.update(chunk)
        return digest.hexdigest()

    def _delete_generation_files(self, generation: _Generation) -> None:
        for path in generation.paths:
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:  # pragma: no cover - defensive
                self._reclaim_errors.append(f"{path}: {exc}")
        generation.removed = True

    def close(self) -> None:
        """Drop all generations and, when owned, the temporary data dir."""
        with self._lock:
            generations = list(self._gens.values())
            self._gens.clear()
        for generation in generations:
            self._delete_generation_files(generation)
        if self._owns_tmpdir is not None:
            try:
                self._owns_tmpdir.cleanup()
            except OSError:  # pragma: no cover - defensive
                pass
            self._owns_tmpdir = None

    # ------------------------------------------------------------------ state
    def _reset_stats_locked(self) -> None:
        self._main_requests = 0
        self._range_requests = 0
        self._bytes_sent = 0
        self._request_seq = 0
        self._requests: list[_RequestRecord] = []

    def reset(self, size: Optional[int] = None) -> dict:
        """Reset to v1; optionally resize atomically.

        Returns a small summary.  A size equal to the current one reuses the
        exact same layer files (matching the historical behaviour).  A new size
        builds a fresh generation before the swap; build errors leave the old
        generation current and downloadable.
        """
        with self._reset_lock:
            with self._lock:
                current_gen = self._gens[self._current_gen_id]
                target_size = current_gen.size if size is None else int(size)
                same_size = target_size == current_gen.size

            if same_size:
                with self._lock:
                    self._current_name = "v1"
                    self._reset_stats_locked()
                return {"resized": False, "generation": current_gen.gen_id, "size": target_size}

            with self._lock:
                self._gen_seq += 1
                gen_id = self._gen_seq

            # Heavy work happens outside the store lock so concurrent downloads
            # keep streaming; the event loop is not blocked because the reset
            # route is a sync endpoint (run in a worker thread).
            new_generation = self._build_generation(gen_id, target_size)

            victims: list[_Generation] = []
            with self._lock:
                old_current = self._gens[self._current_gen_id]
                self._gens[gen_id] = new_generation
                self._current_gen_id = gen_id
                self._current_name = "v1"
                old_current.retired = True
                for gid, generation in list(self._gens.items()):
                    if generation.retired and generation.users <= 0:
                        victims.append(generation)
                        del self._gens[gid]
                self._reset_stats_locked()

        # File deletion must never run under the lock.
        for generation in victims:
            self._delete_generation_files(generation)
            with self._lock:
                self._reclaimed_generations += 1
        return {"resized": True, "generation": gen_id, "size": target_size}

    # ------------------------------------------------------- version snapshots
    def current(self) -> OriginVersion:
        with self._lock:
            generation = self._gens[self._current_gen_id]
            return generation.versions[self._current_name]

    def lease_current(self) -> "tuple[OriginVersion, GenerationLease]":
        """Atomically snapshot the current version and lease its generation.

        The caller MUST eventually call :meth:`GenerationLease.release` once
        (the streamed body and the response wrapper both do so idempotently).
        """
        with self._lock:
            generation = self._gens[self._current_gen_id]
            generation.users += 1
            version = generation.versions[self._current_name]
            return version, GenerationLease(self, generation.gen_id)

    def release_generation(self, gen_id: int) -> None:
        """Raw one-shot decrement; prefer :class:`GenerationLease` in routes."""
        self._release_gen_id(gen_id)

    def _release_gen_id(self, gen_id: int) -> None:
        victim: Optional[_Generation] = None
        with self._lock:
            generation = self._gens.get(gen_id)
            if generation is None:
                return
            generation.users -= 1
            if generation.users < 0:
                generation.users = 0
            if generation.users == 0 and generation.retired:
                victim = generation
                del self._gens[gen_id]
        if victim is not None:
            self._delete_generation_files(victim)
            with self._lock:
                self._reclaimed_generations += 1

    def current_name(self) -> str:
        with self._lock:
            return self._current_name

    def version_names(self) -> list[str]:
        with self._lock:
            generation = self._gens[self._current_gen_id]
            return list(generation.versions)

    def get(self, name: str) -> OriginVersion:
        with self._lock:
            generation = self._gens[self._current_gen_id]
            try:
                return generation.versions[name]
            except KeyError as exc:  # pragma: no cover - defensive
                raise KeyError(f"unknown origin version {name!r}") from exc

    def switch(self, name: str = "v2") -> dict:
        with self._lock:
            generation = self._gens[self._current_gen_id]
            if name not in generation.versions:
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
        range_length: Optional[int] = None,
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
                range_length=range_length,
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

    def record_storage_read(self, record: _RequestRecord, amount: int) -> None:
        if amount <= 0:
            return
        with self._lock:
            record.storage_read_bytes += amount

    def finish_request(self, record: _RequestRecord, outcome: str = "ok") -> None:
        with self._lock:
            record.finished_at = _utc_now()
            record.outcome = outcome

    # ------------------------------------------------------------------ reads
    def open_layer_file(self, version: OriginVersion):
        """Open an unbuffered per-request handle (no hidden readahead accounting)."""
        assert version.path is not None, "file-backed version required"
        handle = open(version.path, "rb", buffering=0)
        # Seek performs no body reads: locating ``start`` costs 0 body bytes.
        return handle

    @staticmethod
    def seek_to(handle, offset: int, record: _RequestRecord) -> None:
        handle.seek(offset)
        record.positioning_bytes = 0

    def read_body_chunk(
        self, handle, wanted: int, record: _RequestRecord
    ) -> bytes:
        """Read up to ``wanted`` bytes, metering every body byte read."""
        pieces: list[bytes] = []
        remaining = wanted
        while remaining > 0:
            buf = handle.read(remaining)
            if not buf:
                break
            pieces.append(buf)
            remaining -= len(buf)
            self.record_storage_read(record, len(buf))
        return b"".join(pieces)

    # ------------------------------------------------------------------ stats
    def _storage_snapshot_locked(self) -> dict:
        return {
            "data_dir": str(self._data_dir),
            "reclaimed_generations": self._reclaimed_generations,
            "reclaim_errors": list(self._reclaim_errors[-5:]),
            "generation_count": len(self._gens),
            "small_range_limit_bytes": SMALL_RANGE_LIMIT,
            "small_range_work_limit_bytes": SMALL_RANGE_WORK_LIMIT,
            "generations": [
                {
                    "id": generation.gen_id,
                    "size": generation.size,
                    "users": generation.users,
                    "retired": generation.retired,
                    "removed": generation.removed,
                    "versions": sorted(generation.versions),
                    "files": [path.name for path in generation.paths],
                }
                for generation in sorted(self._gens.values(), key=lambda g: g.gen_id)
            ],
        }

    def stats(self) -> dict:
        with self._lock:
            current = self._gens[self._current_gen_id]
            current_version = current.versions[self._current_name]
            storage = self._storage_snapshot_locked()
            return {
                "current_version": self._current_name,
                "current_generation": self._current_gen_id,
                "current_etag": current_version.etag,
                "current_sha256": current_version.sha256,
                "main_requests": self._main_requests,
                "range_requests": self._range_requests,
                "total_requests": len(self._requests),
                "bytes_sent": self._bytes_sent,
                "small_range_limit_bytes": SMALL_RANGE_LIMIT,
                "small_range_work_limit_bytes": SMALL_RANGE_WORK_LIMIT,
                "storage": storage,
                "requests": [self._record_json(r) for r in reversed(self._requests)],
            }

    @staticmethod
    def _record_json(r: _RequestRecord) -> dict:
        limit = r.work_limit_bytes()
        used = r.body_bytes_used
        return {
            "id": r.id,
            "kind": r.kind,
            "range": r.range_header,
            "if_range": r.if_range,
            "version": r.requested_version,
            "etag": r.etag,
            "status": r.status,
            "bytes_sent": r.bytes_sent,
            "range_length": r.range_length,
            # Actual locate+send body work.  ``body_bytes_used`` is the headline
            # number: positioning bytes + bytes read/generated to send.
            "positioning_bytes": r.positioning_bytes,
            "storage_read_bytes": r.storage_read_bytes,
            "read_bytes": r.storage_read_bytes + r.positioning_bytes,
            "generated_bytes": r.generated_bytes,
            "body_bytes_used": used,
            "work_bytes": used,
            "work_limit_bytes": limit,
            "work_within_limit": r.work_within_limit(),
            # Self-describing nested view for tooling.
            "work": {
                "body_bytes_used": used,
                "positioning_bytes": r.positioning_bytes,
                "read_bytes": r.storage_read_bytes + r.positioning_bytes,
                "storage_read_bytes": r.storage_read_bytes,
                "generated_bytes": r.generated_bytes,
                "applicable": limit is not None,
                "range_max_bytes": SMALL_RANGE_LIMIT,
                "limit_bytes": limit,
                "within_limit": r.work_within_limit(),
            },
            "outcome": r.outcome,
            "started_at": r.started_at,
            "finished_at": r.finished_at,
        }

    def describe_versions(self) -> list[dict]:
        with self._lock:
            generation = self._gens[self._current_gen_id]
            return [
                {
                    "name": v.name,
                    "size": v.size,
                    "sha256": v.sha256,
                    "etag": v.etag,
                    "generation": v.generation,
                }
                for v in generation.versions.values()
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


class LeasedStreamingResponse(StreamingResponse):
    """StreamingResponse that always settles its generation lease.

    Starlette only iterates (and therefore finalizes) the body generator once
    response.start has been sent.  This wrapper additionally closes the async
    iterator and releases the idempotent lease from a ``finally``, covering a
    client that disappears before iteration starts.
    """

    def __init__(self, *args, lease: GenerationLease, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._lease = lease

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            aclose = getattr(self.body_iterator, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except BaseException:  # noqa: BLE001 - must still release lease
                    pass
            self._lease.release()


def _stream_body(
    store: OriginStore,
    version: OriginVersion,
    start: int,
    end: int,
    record: _RequestRecord,
    lease: GenerationLease,
):
    async def generator():
        handle = None
        try:
            handle = store.open_layer_file(version)
            store.seek_to(handle, start, record)
            pos = start
            while pos <= end:
                wanted = min(store.chunk_size, end - pos + 1)
                chunk = store.read_body_chunk(handle, wanted, record)
                if not chunk:
                    raise OSError(
                        f"layer file truncated at {pos} "
                        f"(generation {version.generation})"
                    )
                pos += len(chunk)
                store.record_sent(record, len(chunk))
                yield chunk
                await asyncio.sleep(0)
            store.finish_request(record, "ok")
        except (asyncio.CancelledError, GeneratorExit):
            store.finish_request(record, "aborted")
            raise
        except Exception as exc:
            store.finish_request(record, f"error:{type(exc).__name__}")
            raise
        finally:
            if handle is not None:
                try:
                    handle.close()
                except OSError:
                    pass
            # Idempotent with the response wrapper's safety-net release.
            lease.release()

    return generator()


def make_origin_router(store: OriginStore) -> APIRouter:
    router = APIRouter(prefix="/origin", tags=["origin"])

    @router.get("/layer")
    async def get_layer(request: Request):
        # Atomically snapshot + lease the current generation.  The lease keeps
        # the layer file alive even if a reset retires this generation while
        # the response is in flight.
        version, lease = store.lease_current()
        lease_transferred = False
        try:
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
                    range_length=version.size,
                )
                headers = {
                    "ETag": version.etag,
                    "Content-Length": str(version.size),
                    "Accept-Ranges": "bytes",
                    "X-Content-SHA256": version.sha256,
                    "X-Origin-Version": version.name,
                    "X-If-Range-Matched": "false",
                }
                response = LeasedStreamingResponse(
                    _stream_body(store, version, 0, version.size - 1, record, lease),
                    lease=lease,
                    status_code=200,
                    headers=headers,
                    media_type="application/octet-stream",
                )
                lease_transferred = True
                return response

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
                range_length = end - start + 1
                record = store.begin_request(
                    kind="range",
                    range_header=range_header,
                    if_range=if_range,
                    version=version,
                    status=206,
                    range_length=range_length,
                )
                headers = {
                    "ETag": version.etag,
                    "Content-Length": str(range_length),
                    "Content-Range": f"bytes {start}-{end}/{version.size}",
                    "Accept-Ranges": "bytes",
                    "X-Content-SHA256": version.sha256,
                    "X-Origin-Version": version.name,
                    "X-If-Range-Matched": "true" if if_range else "not_sent",
                }
                response = LeasedStreamingResponse(
                    _stream_body(store, version, start, end, record, lease),
                    lease=lease,
                    status_code=206,
                    headers=headers,
                    media_type="application/octet-stream",
                )
                lease_transferred = True
                return response

            record = store.begin_request(
                kind="main",
                range_header=None,
                if_range=None,
                version=version,
                status=200,
                range_length=version.size,
            )
            headers = {
                "ETag": version.etag,
                "Content-Length": str(version.size),
                "Accept-Ranges": "bytes",
                "X-Content-SHA256": version.sha256,
                "X-Origin-Version": version.name,
            }
            response = LeasedStreamingResponse(
                _stream_body(store, version, 0, version.size - 1, record, lease),
                lease=lease,
                status_code=200,
                headers=headers,
                media_type="application/octet-stream",
            )
            lease_transferred = True
            return response
        finally:
            # Non-stream responses (416) release here; streamed responses set
            # lease_transferred and the LeasedStreamingResponse guarantees an
            # idempotent release when streaming finishes / is disconnected.
            if not lease_transferred:
                lease.release()

    @router.post("/switch")
    async def switch_version(version: str = "v2"):
        try:
            result = store.switch(version)
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, **result, "state": store.stats()}

    @router.post("/reset")
    def reset_origin(size_bytes: Optional[int] = None):
        if size_bytes is not None and not (
            1024 <= size_bytes <= 512 * 1024 * 1024
        ):
            raise HTTPException(
                status_code=400,
                detail="size_bytes must be between 1024 and 536870912",
            )
        try:
            summary = store.reset(size_bytes)
        except Exception as exc:
            # Build/publish failed: the previous generation stays current and
            # remains downloadable.
            raise HTTPException(
                status_code=500,
                detail=f"origin reset failed, previous layer retained: "
                f"{type(exc).__name__}: {exc}",
            ) from exc
        return {
            "ok": True,
            **summary,
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
