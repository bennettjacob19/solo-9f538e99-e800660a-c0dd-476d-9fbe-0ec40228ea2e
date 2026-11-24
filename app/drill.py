"""Drill orchestration: one shared main download, two receiving routes.

Fast and slow routes both consume the *same* upstream HTTP response.  The slow
route pauses after three quarters of the layer, the gateway keeps as much of the
remaining tail as its retention budget allows, and once the fast route is done
the slow route resumes from that buffer.  Any tail bytes the gateway could not
retain are fetched from the origin with a conditional range request.  That
backfill must prove it still serves the same strong ETag and the exact byte
range the main download had; otherwise the slow route reports a missing
segment instead of silently mixing versions.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

from .retention import RetentionBuffer, describe_segments

STRONG_ETAG_RE = re.compile(r'^"[^"]*"$')
CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")

STATUS_IDLE = "idle"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_ERROR = "error"

PHASE_IDLE = "idle"
PHASE_MAIN = "main_download"
PHASE_PAUSED = "slow_paused"
PHASE_RESUME = "slow_resuming"
PHASE_BACKFILL = "backfill"
PHASE_DONE = "complete"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}Z"


@dataclass
class DrillParams:
    size_bytes: int = 8 * 1024 * 1024
    chunk_size: int = 64 * 1024
    chunk_delay_ms: int = 15
    retention_bytes: int = 1024 * 1024
    retention_memory_bytes: int = 512 * 1024
    backfill_bytes: int = 2 * 1024 * 1024
    auto_switch_on_pause: bool = False
    switch_version: str = "v2"
    pause_fraction: float = 0.75

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "DrillParams":
        def as_int(name: str, default: int, low: int, high: int) -> int:
            raw = payload.get(name, default)
            try:
                value = int(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be an integer") from exc
            if value < low or value > high:
                raise ValueError(f"{name} must be between {low} and {high}")
            return value

        size = as_int("size_bytes", cls.size_bytes, 1024, 128 * 1024 * 1024)
        chunk_size = as_int("chunk_size", cls.chunk_size, 1024, 4 * 1024 * 1024)
        delay = as_int("chunk_delay_ms", cls.chunk_delay_ms, 0, 2000)
        retention = as_int(
            "retention_bytes", cls.retention_bytes, 0, 128 * 1024 * 1024
        )
        memory = as_int(
            "retention_memory_bytes",
            min(cls.retention_memory_bytes, retention),
            0,
            128 * 1024 * 1024,
        )
        if memory > retention:
            raise ValueError("retention_memory_bytes cannot exceed retention_bytes")
        backfill = as_int(
            "backfill_bytes", cls.backfill_bytes, 0, 128 * 1024 * 1024
        )
        pause_fraction = float(payload.get("pause_fraction", cls.pause_fraction))
        if not 0.0 < pause_fraction < 1.0:
            raise ValueError("pause_fraction must be between 0 and 1")
        switch_version = str(payload.get("switch_version", cls.switch_version))
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,32}", switch_version):
            raise ValueError("switch_version is invalid")
        return cls(
            size_bytes=size,
            chunk_size=chunk_size,
            chunk_delay_ms=delay,
            retention_bytes=retention,
            retention_memory_bytes=memory,
            backfill_bytes=backfill,
            auto_switch_on_pause=bool(payload.get("auto_switch_on_pause", False)),
            switch_version=switch_version,
            pause_fraction=pause_fraction,
        )

    def public(self) -> dict[str, Any]:
        return {
            "size_bytes": self.size_bytes,
            "chunk_size": self.chunk_size,
            "chunk_delay_ms": self.chunk_delay_ms,
            "retention_bytes": self.retention_bytes,
            "retention_memory_bytes": self.retention_memory_bytes,
            "backfill_bytes": self.backfill_bytes,
            "auto_switch_on_pause": self.auto_switch_on_pause,
            "switch_version": self.switch_version,
            "pause_fraction": self.pause_fraction,
        }


@dataclass
class HashingSink:
    name: str
    received_bytes: int = 0
    _hasher: Any = field(default_factory=hashlib.sha256)

    def feed(self, data: bytes) -> None:
        if data:
            self._hasher.update(data)
            self.received_bytes += len(data)

    def digest(self) -> str:
        return self._hasher.hexdigest()


class DrillRun:
    def __init__(self, params: DrillParams, origin_base: str) -> None:
        self.run_id = uuid.uuid4().hex[:12]
        self.params = params
        self.origin_base = origin_base.rstrip("/")
        self.status = STATUS_RUNNING
        self.phase = PHASE_MAIN
        self.error: Optional[str] = None
        self.started_monotonic = time.monotonic()
        self.finished_monotonic: Optional[float] = None
        self.started_at = _now_iso()
        self.finished_at: Optional[str] = None
        self.events: list[dict[str, str]] = []
        self._task: Optional[asyncio.Task] = None

        self.total_bytes = 0
        self.main_etag: Optional[str] = None
        self.main_etag_strong = False
        self.expected_sha256: Optional[str] = None
        self.main_bytes_streamed = 0
        self.main_complete = False

        self.fast = HashingSink("fast")
        self.slow = HashingSink("slow")
        self.fast_complete = False

        self.pause_threshold = 0
        self.slow_paused = False
        self.slow_paused_at: Optional[int] = None
        self.slow_resumed = False
        self.retention = RetentionBuffer(
            params.retention_bytes, params.retention_memory_bytes
        )

        self.backfill = {
            "attempted": False,
            "quota_bytes": params.backfill_bytes,
            "used_bytes": 0,
            "requested_start": None,
            "requested_end": None,
            "received_bytes": 0,
            "status": "not_needed",
            "etag_verified": False,
            "range_verified": False,
            "rejected_reason": None,
        }
        self.origin_switch: dict[str, Any] = {"triggered": False, "source": None}

    # ------------------------------------------------------------------ utils
    def log(self, kind: str, message: str) -> None:
        self.events.append({"at": _now_iso(), "kind": kind, "message": message})
        if len(self.events) > 200:
            del self.events[: len(self.events) - 200]

    def _check_quota(self) -> dict[str, Any]:
        """Expected coverage of the tail once the slow route pauses."""
        tail = max(0, self.total_bytes - (self.slow_paused_at or 0))
        retained = min(tail, self.params.retention_bytes)
        refetch = max(0, tail - retained)
        return {
            "tail_bytes": tail,
            "retained_at_most": retained,
            "refetch_needed_bytes": refetch,
            "retention_quota_bytes": self.params.retention_bytes,
            "backfill_quota_bytes": self.params.backfill_bytes,
            "combined_quota_bytes": self.params.retention_bytes
            + self.params.backfill_bytes,
            "combined_covers_tail": tail
            <= self.params.retention_bytes + self.params.backfill_bytes,
        }

    # -------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"drill-{self.run_id}")

    async def cancel(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self.retention.close()

    async def _origin_post(self, path: str, params: dict[str, Any] | None = None) -> dict:
        # Generating/resetting a 128 MiB layer can take seconds on a slow disk;
        # keep this generous so the drill does not fail on a cold origin.
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            resp = await client.post(self.origin_base + path, params=params)
            resp.raise_for_status()
            return resp.json()

    async def _auto_switch(self) -> None:
        try:
            self.origin_switch = {"triggered": True, "source": "auto_on_pause"}
            result = await self._origin_post(
                "/origin/switch", {"version": self.params.switch_version}
            )
            self.origin_switch.update(
                {
                    "from": result.get("from"),
                    "to": result.get("to"),
                    "changed": result.get("changed"),
                    "at": result.get("at"),
                }
            )
            self.log(
                "origin",
                f"源站已在慢端暂停时切换到 {result.get('to')}（等长新版，"
                f"主下载继续使用 {result.get('from')}）",
            )
        except Exception as exc:  # pragma: no cover - best effort
            self.origin_switch["error"] = str(exc)
            self.log("error", f"自动切换源站失败：{exc}")

    async def switch_origin(self, version: str, source: str) -> dict[str, Any]:
        result = await self._origin_post("/origin/switch", {"version": version})
        self.origin_switch = {
            "triggered": True,
            "source": source,
            "from": result.get("from"),
            "to": result.get("to"),
            "changed": result.get("changed"),
            "at": result.get("at"),
        }
        self.log(
            "origin",
            f"源站切换请求（{source}）：{result.get('from')} -> {result.get('to')}",
        )
        return result

    async def _run(self) -> None:
        try:
            await self._origin_post(
                "/origin/reset", {"size_bytes": self.params.size_bytes}
            )
            self.log("start", f"演练 {self.run_id} 启动，先重置源站")
            await self._main_download()
            self.fast_complete = True
            self.log("fast", f"快端收齐 {self.fast.received_bytes} 字节")
            if not self.slow_paused and self.total_bytes:
                # Defensive: normally the pause threshold is well below the
                # end, but keep semantics even for tiny layers.
                self._enter_pause(self.total_bytes)
            self.phase = PHASE_RESUME
            await self._resume_slow()
            self.status = STATUS_DONE
            self.phase = PHASE_DONE
            outcome = self._outcome()
            self.log("done", f"演练结束，结果：{outcome}")
        except asyncio.CancelledError:
            self.status = STATUS_ERROR
            self.error = "run cancelled"
            raise
        except Exception as exc:
            self.status = STATUS_ERROR
            self.error = f"{type(exc).__name__}: {exc}"
            self.log("error", self.error)
        finally:
            self.finished_at = _now_iso()
            if self.finished_monotonic is None:
                self.finished_monotonic = time.monotonic()
            self.retention.close()
            if self.status == STATUS_RUNNING:
                self.status = STATUS_DONE
                self.phase = PHASE_DONE

    # --------------------------------------------------------------- transfer
    async def _main_download(self) -> None:
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream(
                "GET", self.origin_base + "/origin/layer"
            ) as resp:
                if resp.status_code != 200:
                    raise RuntimeError(f"main download returned {resp.status_code}")
                etag = resp.headers.get("etag")
                sha = resp.headers.get("x-content-sha256")
                length = resp.headers.get("content-length")
                if etag is None or sha is None or length is None:
                    raise RuntimeError("main response missing ETag/SHA-256/Length")
                self.main_etag = etag
                self.main_etag_strong = bool(STRONG_ETAG_RE.match(etag))
                if not self.main_etag_strong:
                    raise RuntimeError(f"origin ETag is not strong: {etag!r}")
                self.expected_sha256 = sha
                self.total_bytes = int(length)
                self.pause_threshold = max(
                    1, math.ceil(self.total_bytes * self.params.pause_fraction)
                )
                self.log(
                    "main",
                    f"唯一主下载已建立：{self.total_bytes} 字节，强 ETag {etag}",
                )

                async for chunk in resp.aiter_bytes(self.params.chunk_size):
                    if not chunk:
                        continue
                    self.main_bytes_streamed += len(chunk)
                    paused_now = self._fan_out(chunk)
                    if paused_now and self.params.auto_switch_on_pause:
                        # The main response was snapshotted by the origin when
                        # the stream began, so swapping now keeps the running
                        # download on the original bytes.
                        await self._auto_switch()
                    if self.params.chunk_delay_ms:
                        await asyncio.sleep(self.params.chunk_delay_ms / 1000.0)
                    else:
                        await asyncio.sleep(0)
                self.main_complete = True
                self.log(
                    "main",
                    f"主下载结束，共 {self.main_bytes_streamed} 字节"
                    f"（快端同步收齐）",
                )

    def _fan_out(self, chunk: bytes) -> bool:
        """Feed both routes; return True when the slow route just paused."""
        self.fast.feed(chunk)
        if not self.slow_paused:
            room = self.pause_threshold - self.slow.received_bytes
            if room >= len(chunk):
                self.slow.feed(chunk)
                return False
            if room > 0:
                self.slow.feed(chunk[:room])
            self._enter_pause(self.slow.received_bytes)
            self.retention.append(chunk[room:])
            return True
        self.retention.append(chunk)
        return False

    def _enter_pause(self, at_bytes: int) -> None:
        if self.slow_paused:
            return
        self.slow_paused = True
        self.slow_paused_at = at_bytes
        self.phase = PHASE_PAUSED
        self.log(
            "slow",
            f"慢端在 {at_bytes}/{self.total_bytes} 字节处暂停；"
            "网关开始为其留存正文",
        )

    async def _resume_slow(self) -> None:
        self.slow_resumed = True
        self.log(
            "slow",
            f"快端已收齐，慢端续传：先读网关留存 "
            f"{self.retention.used_bytes} 字节",
        )
        while self.retention.used_bytes > 0:
            data = self.retention.read(self.params.chunk_size)
            if not data:
                break
            self.slow.feed(data)
            await asyncio.sleep(0)
        missing = self.total_bytes - self.slow.received_bytes
        if missing <= 0:
            self.backfill["status"] = "not_needed"
            return
        self.phase = PHASE_BACKFILL
        await self._backfill(missing)

    async def _backfill(self, missing: int) -> None:
        info = self.backfill
        info["attempted"] = True
        quota_left = self.params.backfill_bytes - info["used_bytes"]
        affordable = min(missing, max(0, quota_left))
        if affordable <= 0:
            info["status"] = "quota_exhausted"
            info["rejected_reason"] = "backfill quota is zero"
            self.log(
                "backfill",
                f"补段额度不足：还缺 {missing} 字节，额度余量 {max(0, quota_left)} 字节",
            )
            return
        start = self.slow.received_bytes
        end = start + affordable - 1
        info["requested_start"] = start
        info["requested_end"] = end
        headers = {
            "Range": f"bytes={start}-{end}",
            "If-Range": self.main_etag or "",
        }
        self.log(
            "backfill",
            f"向源站补段 bytes={start}-{end}，If-Range={self.main_etag}",
        )
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream(
                "GET", self.origin_base + "/origin/layer", headers=headers
            ) as resp:
                resp_etag = resp.headers.get("etag")
                info["response_status"] = resp.status_code
                info["response_etag"] = resp_etag
                info["etag_verified"] = bool(
                    resp_etag
                    and STRONG_ETAG_RE.match(resp_etag)
                    and resp_etag == self.main_etag
                )
                content_range = resp.headers.get("content-range", "")
                match = CONTENT_RANGE_RE.match(content_range)
                info["response_content_range"] = content_range or None
                info["range_verified"] = bool(
                    match
                    and int(match.group(1)) == start
                    and int(match.group(2)) == end
                    and int(match.group(3)) == self.total_bytes
                )
                resp_sha = resp.headers.get("x-content-sha256")
                sha_verified = resp_sha == self.expected_sha256

                reason = None
                if resp.status_code != 206:
                    reason = (
                        f"源站未返回 206（实际 {resp.status_code}），"
                        "If-Range 已失效，补段被拒绝"
                    )
                elif not info["etag_verified"]:
                    reason = "补段 ETag 不是主下载的强 ETag"
                elif not info["range_verified"]:
                    reason = f"补段字节区间不符：{content_range!r}"
                elif not sha_verified:
                    reason = "补段内容摘要与主下载版本不符"
                if reason:
                    info["status"] = "rejected"
                    info["rejected_reason"] = reason
                    self.log("backfill", reason)
                    return

                received = 0
                try:
                    async for chunk in resp.aiter_bytes(self.params.chunk_size):
                        if not chunk:
                            continue
                        room = affordable - received
                        if room <= 0:
                            break
                        take = chunk[:room]
                        self.slow.feed(take)
                        received += len(take)
                        info["received_bytes"] = received
                        info["used_bytes"] += len(take)
                        if self.params.chunk_delay_ms:
                            await asyncio.sleep(self.params.chunk_delay_ms / 1000.0)
                        else:
                            await asyncio.sleep(0)
                finally:
                    if received >= affordable:
                        info["status"] = "accepted"
                        self.log(
                            "backfill",
                            f"补段校验通过，慢端收到 {received} 字节",
                        )
                    else:
                        info["status"] = "short"
                        info["rejected_reason"] = (
                            f"补段正文只收到 {received}/{affordable} 字节"
                        )
                        self.log("backfill", info["rejected_reason"])

    # ---------------------------------------------------------------- results
    def missing_segments(self) -> list[dict[str, int]]:
        return describe_segments(self.slow.received_bytes, self.total_bytes - 1)

    def _verification(self, sink: HashingSink) -> dict[str, Any]:
        actual = sink.digest()
        if not self.expected_sha256:
            result = "pending"
        elif sink.received_bytes < self.total_bytes:
            result = "incomplete"
        elif actual == self.expected_sha256:
            result = "match"
        else:
            result = "mismatch"
        return {
            "algorithm": "sha256",
            "expected": self.expected_sha256,
            "actual": actual if self.expected_sha256 else None,
            "result": result,
        }

    def _outcome(self) -> str:
        if self.total_bytes <= 0:
            return "pending"
        if (
            self.fast.received_bytes == self.total_bytes
            and self.slow.received_bytes == self.total_bytes
        ):
            return "both_complete"
        return "slow_missing"

    def snapshot(self) -> dict[str, Any]:
        fast_ok = (
            self.fast.received_bytes == self.total_bytes and self.total_bytes > 0
        )
        missing = self.missing_segments()
        missing_bytes = sum(s["length"] for s in missing)
        elapsed_ms = int(
            ((self.finished_monotonic or time.monotonic()) - self.started_monotonic)
            * 1000
        )
        return {
            "run_id": self.run_id,
            "status": self.status,
            "phase": self.phase,
            "error": self.error,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "elapsed_ms": elapsed_ms,
            "params": self.params.public(),
            "total_bytes": self.total_bytes,
            "outcome": self._outcome()
            if self.status == STATUS_DONE
            else "pending",
            "main": {
                "request_count": 1 if self.main_etag else 0,
                "etag": self.main_etag,
                "etag_strong": self.main_etag_strong,
                "expected_sha256": self.expected_sha256,
                "bytes_streamed": self.main_bytes_streamed,
                "complete": self.main_complete,
            },
            "fast": {
                "received_bytes": self.fast.received_bytes,
                "complete": fast_ok,
                "verification": self._verification(self.fast),
            },
            "slow": {
                "received_bytes": self.slow.received_bytes,
                "paused": self.slow_paused,
                "paused_at_bytes": self.slow_paused_at,
                "resumed": self.slow_resumed,
                "pause_threshold_bytes": self.pause_threshold,
                "verification": self._verification(self.slow),
                "missing_segments": missing,
                "missing_bytes": missing_bytes,
                "backfill": dict(self.backfill),
            },
            "retention": self.retention.stats(),
            "quota_check": self._check_quota()
            if self.slow_paused_at is not None
            else None,
            "origin_switch": dict(self.origin_switch),
            "events": list(self.events),
        }


class DrillManager:
    def __init__(self) -> None:
        self.current: Optional[DrillRun] = None
        self._start_lock = asyncio.Lock()

    async def start(self, params: DrillParams, origin_base: str) -> DrillRun:
        async with self._start_lock:
            if self.current is not None and self.current.status == STATUS_RUNNING:
                await self.current.cancel()
            run = DrillRun(params, origin_base)
            self.current = run
            run.start()
            return run

    async def reset(self) -> None:
        async with self._start_lock:
            if self.current is not None and self.current.status == STATUS_RUNNING:
                await self.current.cancel()
            self.current = None

    def snapshot(self) -> dict[str, Any]:
        if self.current is None:
            return {
                "status": STATUS_IDLE,
                "phase": PHASE_IDLE,
                "outcome": "pending",
                "params": None,
            }
        return self.current.snapshot()
