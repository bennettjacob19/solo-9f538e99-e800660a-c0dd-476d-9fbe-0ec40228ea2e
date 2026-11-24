#!/usr/bin/env python3
"""Self-test: drive the public HTTP API through the key drill scenarios.

This starts the origin and gateway services as two separate local processes
(the same split as docker compose), then exercises the web API:

1. pause at 3/4, combined quota covers the tail          -> both complete
2. quota too small                                       -> slow reports missing
3. origin swapped to an equal-length new version on pause -> slow reports missing
4. origin swapped but full retention covered the tail     -> both complete
5. manual switch while paused                            -> handled

Exit code is non-zero when any expectation fails.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Services:
    def __init__(self) -> None:
        self.origin_port = free_port()
        self.gateway_port = free_port()
        self.procs: list[subprocess.Popen] = []

    def __enter__(self) -> "Services":
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        env["DRILL_LAYER_BYTES"] = str(1024 * 1024)
        log_dir = Path("/tmp/drill-selftest")
        log_dir.mkdir(exist_ok=True)
        self._origin_log = (log_dir / "origin.log").open("w")
        self._gateway_log = (log_dir / "gateway.log").open("w")
        self.procs.append(
            subprocess.Popen(
                [
                    PYTHON, "-m", "uvicorn", "app.origin_app:app",
                    "--host", "127.0.0.1", "--port", str(self.origin_port),
                    "--log-level", "warning",
                ],
                cwd=ROOT, env=env, stdout=self._origin_log, stderr=subprocess.STDOUT,
            )
        )
        genv = dict(env)
        genv["DRILL_ORIGIN_BASE"] = f"http://127.0.0.1:{self.origin_port}"
        self.procs.append(
            subprocess.Popen(
                [
                    PYTHON, "-m", "uvicorn", "app.gateway_app:app",
                    "--host", "127.0.0.1", "--port", str(self.gateway_port),
                    "--log-level", "warning",
                ],
                cwd=ROOT, env=genv, stdout=self._gateway_log, stderr=subprocess.STDOUT,
            )
        )
        self.origin = f"http://127.0.0.1:{self.origin_port}"
        self.gateway = f"http://127.0.0.1:{self.gateway_port}"
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                if httpx.get(self.origin + "/healthz", timeout=1).status_code == 200 and (
                    httpx.get(self.gateway + "/api/health", timeout=1).status_code == 200
                ):
                    return self
            except httpx.HTTPError:
                pass
            time.sleep(0.15)
        raise RuntimeError("services did not become healthy; see /tmp/drill-selftest")

    def __exit__(self, *exc) -> None:
        for proc in self.procs:
            proc.terminate()
        for proc in self.procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        self._origin_log.close()
        self._gateway_log.close()


def run_drill(base: str, **overrides) -> dict:
    manual_switch = bool(overrides.pop("_manual_switch", False))
    payload = {
        "size_bytes": 1024 * 1024,
        "chunk_size": 64 * 1024,
        "chunk_delay_ms": 4,
        "retention_bytes": 128 * 1024,
        "retention_memory_bytes": 64 * 1024,
        "backfill_bytes": 512 * 1024,
        "pause_fraction": 0.75,
        "auto_switch_on_pause": False,
        "switch_version": "v2",
    }
    payload.update(overrides)
    with httpx.Client(base_url=base, timeout=60) as client:
        resp = client.post("/api/drill", json=payload)
        resp.raise_for_status()
        deadline = time.time() + 90
        while time.time() < deadline:
            state = client.get("/api/drill").json()
            if state["status"] != "running":
                return state
            if manual_switch and state["phase"] == "slow_paused":
                client.post("/api/drill/switch", params={"version": "v2"})
                manual_switch = False
            time.sleep(0.01)
    raise RuntimeError("drill timed out")


def summarize(name: str, state: dict) -> dict:
    return {
        "scenario": name,
        "outcome": state["outcome"],
        "fast": state["fast"]["received_bytes"],
        "fast_sha": state["fast"]["verification"]["result"],
        "slow": state["slow"]["received_bytes"],
        "slow_sha": state["slow"]["verification"]["result"],
        "missing": state["slow"]["missing_bytes"],
        "backfill": state["slow"]["backfill"]["status"],
        "etag_ok": state["slow"]["backfill"]["etag_verified"],
        "range_ok": state["slow"]["backfill"]["range_verified"],
        "retention_peak": state["retention"]["peak_bytes"],
        "retention_mem_peak": state["retention"]["peak_memory_bytes"],
        "retention_disk_peak": state["retention"]["peak_disk_bytes"],
        "origin_sent": state["origin"]["bytes_sent"],
        "origin_version": state["origin"]["current_version"],
        "switched": state["origin_switch"]["triggered"],
        "main_requests": state["origin"]["main_requests"],
        "range_requests": state["origin"]["range_requests"],
    }


def main() -> int:
    failures: list[str] = []
    results: list[dict] = []

    with Services() as svc:
        total = 1024 * 1024

        # 1. Combined quota covers a 256 KiB tail: 128 KiB retained + 512 KiB
        #    backfill is more than enough.
        state = run_drill(svc.gateway)
        results.append(summarize("quota covers tail", state))
        ok = (
            state["outcome"] == "both_complete"
            and state["fast"]["received_bytes"] == total
            and state["slow"]["received_bytes"] == total
            and state["fast"]["verification"]["result"] == "match"
            and state["slow"]["verification"]["result"] == "match"
            and state["slow"]["backfill"]["etag_verified"]
            and state["slow"]["backfill"]["range_verified"]
            and state["origin"]["main_requests"] == 1
        )
        if not ok:
            failures.append("scenario 1 (quota covers tail)")

        # 2. Retention 8 KiB + backfill 8 KiB cannot cover a 256 KiB tail.
        state = run_drill(
            svc.gateway,
            retention_bytes=8 * 1024,
            retention_memory_bytes=4 * 1024,
            backfill_bytes=8 * 1024,
        )
        results.append(summarize("quota too small", state))
        ok = (
            state["outcome"] == "slow_missing"
            and state["fast"]["received_bytes"] == total
            and state["fast"]["verification"]["result"] == "match"
            and state["slow"]["verification"]["result"] == "incomplete"
            and state["slow"]["missing_bytes"] > 0
            and state["origin"]["main_requests"] == 1
        )
        if not ok:
            failures.append("scenario 2 (quota too small)")

        # 3. Auto-swap on pause; the tail is not fully retained, so the If-Range
        #    backfill sees v2 -> must be rejected and reported as missing.
        state = run_drill(
            svc.gateway,
            retention_bytes=8 * 1024,
            retention_memory_bytes=4 * 1024,
            backfill_bytes=1024 * 1024,
            auto_switch_on_pause=True,
        )
        results.append(summarize("auto swap, tail not retained", state))
        ok = (
            state["origin_switch"]["triggered"]
            and state["origin"]["current_version"] == "v2"
            and state["slow"]["backfill"]["status"] == "rejected"
            and state["slow"]["backfill"]["etag_verified"] is False
            and state["outcome"] == "slow_missing"
            and state["fast"]["received_bytes"] == total
            and state["fast"]["verification"]["result"] == "match"
            and state["origin"]["main_requests"] == 1
        )
        if not ok:
            failures.append("scenario 3 (auto swap, tail not retained)")

        # 4. Retain the whole tail in memory, swap on pause, no backfill needed.
        state = run_drill(
            svc.gateway,
            retention_bytes=total,
            retention_memory_bytes=total,
            backfill_bytes=0,
            auto_switch_on_pause=True,
        )
        results.append(summarize("auto swap, full retention", state))
        ok = (
            state["origin_switch"]["triggered"]
            and state["slow"]["backfill"]["attempted"] is False
            and state["outcome"] == "both_complete"
            and state["slow"]["verification"]["result"] == "match"
            and state["fast"]["verification"]["result"] == "match"
            and state["retention"]["peak_bytes"] == total - 3 * total // 4
        )
        if not ok:
            failures.append("scenario 4 (auto swap, full retention)")

        # 5. Manual swap while the slow route is paused.
        state = run_drill(
            svc.gateway,
            retention_bytes=8 * 1024,
            retention_memory_bytes=4 * 1024,
            backfill_bytes=1024 * 1024,
            chunk_delay_ms=40,
            _manual_switch=True,
        )
        results.append(summarize("manual swap during pause", state))
        ok = (
            state["origin_switch"]["triggered"]
            and state["origin_switch"]["source"] == "manual"
            and state["slow"]["paused"]
            and state["origin"]["current_version"] == "v2"
            and state["fast"]["received_bytes"] == total
        )
        if not ok:
            failures.append("scenario 5 (manual swap during pause)")

    print(json.dumps(results, indent=2, ensure_ascii=False))
    print()
    print(f"{'scenario':32} {'outcome':14} {'fast':>9} {'slow':>9} {'missing':>9} "
          f"{'backfill':>11} {'ret peak':>10} {'origin sent':>12}")
    for r in results:
        print(
            f"{r['scenario']:32} {r['outcome']:14} {r['fast']:>9} {r['slow']:>9} "
            f"{r['missing']:>9} {r['backfill']:>11} {r['retention_peak']:>10} "
            f"{r['origin_sent']:>12}"
        )
    print()
    if failures:
        print("FAILED:", ", ".join(failures))
        return 1
    print("ALL SCENARIOS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
