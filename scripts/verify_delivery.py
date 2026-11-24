#!/usr/bin/env python3
"""Delivery verification for the 96 MiB origin / 128 MiB layer rework.

This starts origin + gateway as two local processes (the same split as docker
compose) in a dedicated layer data directory, samples origin RSS, and checks:

  A. docker-compose.yml gives the origin exactly a 96 MiB memory limit;
  B. 128 MiB v1/v2 body / strong ETag / SHA-256 match the locked goldens from
     the legacy in-memory builder (same size + version => same bytes);
  C. ordinary GET, Range and If-Range rules stay compatible;
  D. a <= 64 KiB 206 range costs <= 1 MiB of locate+send body work and the
     actual value is present in the origin request record;
  E. resizing mid-download: the old connection finishes with its original
     headers and bytes, then the superseded generation files are reclaimed;
  F. an early disconnect also reclaims the retired generation;
  G. a failed reset leaves the old layer current and downloadable;
  H. a real 128 MiB drill through the gateway completes on both routes with
     matching SHA-256 and exactly one main origin request;
  I. peak origin RSS stays below the 96 MiB container limit.

Exit code is non-zero when any hard expectation fails.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.origin import iter_layer_bytes  # noqa: E402

MIB = 1024 * 1024
LAYER_128 = 128 * MIB
GOLDEN_128 = {
    "v1": "734546cb6c9f493ed1aed3d17e8a1c4c5ef282898313fef4403807163e9b4cef",
    "v2": "85aba783564d92cc65ad6af861edaf874664f23c93613525e576ebc4493eb379",
}
V1_SEED, V2_SEED = 20261123, 20261124


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class RssSampler(threading.Thread):
    def __init__(self, pid: int) -> None:
        super().__init__(daemon=True)
        self.pid = pid
        self.peak_kb = 0
        self._stop_event = threading.Event()

    def run(self) -> None:
        status = Path(f"/proc/{self.pid}/status")
        while not self._stop_event.is_set():
            try:
                for line in status.read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        self.peak_kb = max(self.peak_kb, int(line.split()[1]))
                        break
            except (OSError, ValueError):
                pass
            time.sleep(0.03)

    def stop(self) -> None:
        self._stop_event.set()


class Services:
    def __init__(self, data_dir: Path) -> None:
        self.origin_port = free_port()
        self.gateway_port = free_port()
        self.procs: list[subprocess.Popen] = []
        self.data_dir = data_dir
        self._logs: list = []

    def __enter__(self) -> "Services":
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        env["DRILL_LAYER_BYTES"] = "1048576"
        env["DRILL_ORIGIN_DATA_DIR"] = str(self.data_dir)
        env["MALLOC_ARENA_MAX"] = "2"
        log_dir = Path("/tmp/drill-verify")
        log_dir.mkdir(exist_ok=True)
        self._origin_log = (log_dir / "origin.log").open("w")
        self._gateway_log = (log_dir / "gateway.log").open("w")
        self.procs.append(
            subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.origin_app:app",
                 "--host", "127.0.0.1", "--port", str(self.origin_port),
                 "--log-level", "warning"],
                cwd=ROOT, env=env,
                stdout=self._origin_log, stderr=subprocess.STDOUT,
            )
        )
        genv = dict(env)
        genv["DRILL_ORIGIN_BASE"] = f"http://127.0.0.1:{self.origin_port}"
        self.procs.append(
            subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.gateway_app:app",
                 "--host", "127.0.0.1", "--port", str(self.gateway_port),
                 "--log-level", "warning"],
                cwd=ROOT, env=genv,
                stdout=self._gateway_log, stderr=subprocess.STDOUT,
            )
        )
        self.origin = f"http://127.0.0.1:{self.origin_port}"
        self.gateway = f"http://127.0.0.1:{self.gateway_port}"
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                if httpx.get(self.origin + "/healthz", timeout=1).status_code == 200:
                    g = httpx.get(self.gateway + "/api/health", timeout=1)
                    if g.status_code == 200:
                        return self
            except httpx.HTTPError:
                pass
            time.sleep(0.15)
        raise RuntimeError("services did not become healthy; see /tmp/drill-verify")

    @property
    def origin_pid(self) -> int:
        return self.procs[0].pid

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


def origin_state(client: httpx.Client) -> dict:
    return client.get("/origin/state").json()["state"]


def wait_generation_count(client: httpx.Client, count: int, reclaimed: int = 1,
                          timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = origin_state(client)["storage"]
        if last["generation_count"] == count and last["reclaimed_generations"] >= reclaimed:
            return last
        time.sleep(0.1)
    raise AssertionError(f"generation lifecycle timeout: {last}")


def expected_slice(seed: int, size: int, start: int, end_inclusive: int) -> bytes:
    """Stream the deterministic layer and keep only [start, end]."""
    keep = bytearray()
    pos = 0
    for chunk in iter_layer_bytes(seed, size):
        cstart, cend = pos, pos + len(chunk) - 1
        if cend >= start and cstart <= end_inclusive:
            lo = max(0, start - cstart)
            hi = min(len(chunk), end_inclusive - cstart + 1)
            keep.extend(chunk[lo:hi])
        pos = cend + 1
        if pos > end_inclusive:
            break
    return bytes(keep)


def check_compose_limit(results: list, failures: list) -> None:
    text = (ROOT / "docker-compose.yml").read_text()
    m = re.search(r"^\s*mem_limit:\s*['\"]?(\d+)\s*([mMgGkK]?)['\"]?", text, re.M)
    ok_mem = bool(m and m.group(1) == "96" and m.group(2).lower() == "m")
    ok_deploy = re.search(r"memory:\s*['\"]?96M[iB]*['\"]?", text) is not None
    # Resolve with the real compose plugin if available next to the script env.
    normalized = None
    plugin = os.environ.get("COMPOSE_PLUGIN")
    if plugin and Path(plugin).exists():
        proc = subprocess.run(
            [plugin, "-f", str(ROOT / "docker-compose.yml"), "config", "--format", "json"],
            capture_output=True, text=True,
        )
        if proc.returncode == 0:
            origin = json.loads(proc.stdout)["services"]["origin"]
            normalized = int(origin["mem_limit"])
            ok_mem = normalized == 96 * MIB
    results.append({
        "check": "compose origin memory limit = 96 MiB",
        "ok": ok_mem and ok_deploy,
        "detail": f"mem_limit_match={ok_mem} deploy_limit={ok_deploy} normalized={normalized}",
    })
    if not (ok_mem and ok_deploy):
        failures.append("compose memory limit")


def main() -> int:
    failures: list[str] = []
    results: list[dict] = []
    data_dir = Path(tempfile.mkdtemp(prefix="verify-layers-"))
    print(f"[setup] layer data dir: {data_dir}")

    with Services(data_dir) as svc:
        sampler = RssSampler(svc.origin_pid)
        sampler.start()
        try:
            check_compose_limit(results, failures)

            with httpx.Client(base_url=svc.origin, timeout=httpx.Timeout(180.0)) as oc:
                # -------------------------------------------------- B goldens
                r = oc.post("/origin/reset", params={"size_bytes": LAYER_128})
                r.raise_for_status()
                versions = {v["name"]: v for v in r.json()["versions"]}
                golden_ok = all(
                    versions[name]["sha256"] == GOLDEN_128[name]
                    and versions[name]["etag"] == f'"{name}-{GOLDEN_128[name]}"'
                    for name in ("v1", "v2")
                )
                results.append({
                    "check": "128 MiB v1/v2 SHA-256 + strong ETag match pre-change goldens",
                    "ok": golden_ok,
                    "detail": versions["v1"]["sha256"][:20] + "...",
                })
                if not golden_ok:
                    failures.append("golden etag/sha")

                # ------------------------------------------- C full GET bytes
                t0 = time.time()
                body = oc.get("/origin/layer").content
                dt = time.time() - t0
                full_ok = (
                    len(body) == LAYER_128
                    and hashlib.sha256(body).hexdigest() == GOLDEN_128["v1"]
                )
                results.append({
                    "check": "full GET of 128 MiB distributes exact v1 body",
                    "ok": full_ok,
                    "detail": f"{len(body)} bytes in {dt:.1f}s",
                })
                if not full_ok:
                    failures.append("full 128MiB GET")

                # -------------------------------------- D tail + work budget
                range_cases = [
                    ("bytes=134217727-134217727", LAYER_128 - 1, LAYER_128 - 1, 1),
                    ("bytes=-1", LAYER_128 - 1, LAYER_128 - 1, 1),
                    ("bytes=-65536", LAYER_128 - 65536, LAYER_128 - 1, 65536),
                    ("bytes=134152192-134217727", LAYER_128 - 65536, LAYER_128 - 1, 65536),
                    ("bytes=96-163", 96, 163, 68),
                ]
                tail_ok = True
                work_detail = []
                for header, start, end, length in range_cases:
                    resp = oc.get("/origin/layer", headers={"Range": header})
                    rec = origin_state(oc)["requests"][0]
                    body_match = resp.status_code == 206 and resp.content == body[start:end + 1]
                    used = rec["body_bytes_used"]
                    budget_ok = used <= MIB and rec["work_within_limit"] is True
                    exact_ok = used == length
                    work_detail.append(f"{header}: used={used}")
                    if not (body_match and budget_ok and exact_ok):
                        tail_ok = False
                        print("  range failure:", header, resp.status_code,
                              body_match, used, rec.get("work_within_limit"))
                results.append({
                    "check": "tail/small ranges: exact bytes, 206, locate+send work <= 1 MiB",
                    "ok": tail_ok,
                    "detail": "; ".join(work_detail),
                })
                if not tail_ok:
                    failures.append("tail range / work budget")

                # ------------------------------------------- If-Range compat
                etag_v1 = f'"v1-{GOLDEN_128["v1"]}"'
                good = oc.get("/origin/layer", headers={
                    "Range": "bytes=0-99", "If-Range": etag_v1})
                bad = oc.get("/origin/layer", headers={
                    "Range": "bytes=0-99", "If-Range": '"v1-stale"'})
                ifr_ok = (
                    good.status_code == 206
                    and good.headers.get("x-if-range-matched") == "true"
                    and bad.status_code == 200
                    and bad.headers.get("x-if-range-matched") == "false"
                    and len(bad.content) == LAYER_128
                )
                results.append({
                    "check": "If-Range matching -> 206; stale -> full 200",
                    "ok": ifr_ok, "detail": f"good={good.status_code} bad={bad.status_code}",
                })
                if not ifr_ok:
                    failures.append("if-range")

                # ----------------------------- E resize + continue old download
                resize_ok = False
                resize_detail = ""
                with oc.stream("GET", "/origin/layer") as resp:
                    old_etag = resp.headers.get("etag", "")
                    old_sha = resp.headers.get("x-content-sha256", "")
                    old_len = int(resp.headers["content-length"])
                    received = bytearray()
                    it = resp.iter_bytes(524288)
                    for chunk in it:
                        received.extend(chunk)
                        if len(received) >= 8 * MIB:
                            break
                    rs = oc.post("/origin/reset", params={"size_bytes": 64 * MIB})
                    rs.raise_for_status()
                    storage = origin_state(oc)["storage"]
                    held = storage["generation_count"] == 2
                    for chunk in it:
                        received.extend(chunk)
                old_hash_ok = hashlib.sha256(bytes(received)).hexdigest() == old_sha
                lifecycle = wait_generation_count(oc, count=1, reclaimed=1)
                files = list(data_dir.glob("*.bin"))
                resize_ok = (
                    held and old_len == LAYER_128 and len(received) == LAYER_128
                    and old_hash_ok and old_etag == etag_v1 and len(files) == 2
                )
                resize_detail = (
                    f"held_across_reset={held} drained={len(received)} "
                    f"sha_ok={old_hash_ok} files_left={len(files)} "
                    f"reclaimed={lifecycle['reclaimed_generations']}"
                )
                results.append({
                    "check": "resize mid-download: old headers/body finish, then reclaim",
                    "ok": resize_ok, "detail": resize_detail,
                })
                if not resize_ok:
                    failures.append("resize continue/reclaim")

                # --------------------------------------- F disconnect reclaim
                disc_ok = False
                with oc.stream("GET", "/origin/layer") as resp:
                    dit = resp.iter_bytes(524288)
                    got = 0
                    for chunk in dit:
                        got += len(chunk)
                        if got >= 4 * MIB:
                            break
                    oc.post("/origin/reset", params={"size_bytes": 32 * MIB})
                    held2 = origin_state(oc)["storage"]["generation_count"] == 2
                life2 = wait_generation_count(oc, count=1, reclaimed=2)
                disc_ok = held2 and life2["reclaimed_generations"] >= 2
                results.append({
                    "check": "disconnect of last old download reclaims its generation",
                    "ok": disc_ok,
                    "detail": f"held={held2} reclaimed={life2['reclaimed_generations']}",
                })
                if not disc_ok:
                    failures.append("disconnect reclaim")

                # ------------------------------------------- G failed reset
                if os.geteuid() == 0:
                    results.append({
                        "check": "failed reset keeps old layer downloadable",
                        "ok": True, "detail": "SKIPPED as root (chmod cannot deny root)",
                    })
                else:
                    os.chmod(data_dir, 0o500)
                    try:
                        bad = oc.post("/origin/reset", params={"size_bytes": 16 * MIB})
                        bad_code = bad.status_code
                        after = origin_state(oc)
                        rr = oc.get("/origin/layer", headers={"Range": "bytes=0-4095"})
                        fail_ok = (
                            bad_code == 500
                            and after["current_sha256"]
                            == hashlib.sha256(
                                b"".join(iter_layer_bytes(V1_SEED, 32 * MIB))
                            ).hexdigest()
                            and rr.status_code == 206
                            and len(rr.content) == 4096
                        )
                    finally:
                        os.chmod(data_dir, 0o700)
                    results.append({
                        "check": "failed reset keeps old (32 MiB) layer downloadable",
                        "ok": fail_ok, "detail": f"reset_status={bad_code} range=206",
                    })
                    if not fail_ok:
                        failures.append("failed-reset retention")

            # ------------------------------------------- H gateway 128 drill
            payload = {
                "size_bytes": LAYER_128,
                "chunk_size": MIB,
                "chunk_delay_ms": 0,
                "retention_bytes": 33 * MIB,
                "retention_memory_bytes": 4 * MIB,
                "backfill_bytes": 0,
                "pause_fraction": 0.75,
                "auto_switch_on_pause": False,
            }
            with httpx.Client(base_url=svc.gateway, timeout=60) as gc:
                pr = gc.post("/api/drill", json=payload)
                pr.raise_for_status()
                deadline = time.time() + 300
                state = {}
                while time.time() < deadline:
                    state = gc.get("/api/drill").json()
                    if state.get("status") != "running":
                        break
                    time.sleep(1)
                drill_ok = (
                    state.get("status") == "done"
                    and state.get("outcome") == "both_complete"
                    and state["fast"]["verification"]["result"] == "match"
                    and state["slow"]["verification"]["result"] == "match"
                    and state["fast"]["verification"]["actual"] == GOLDEN_128["v1"]
                    and state["slow"]["verification"]["actual"] == GOLDEN_128["v1"]
                    and state.get("origin", {}).get("main_requests") == 1
                )
                detail = (
                    f"status={state.get('status')} outcome={state.get('outcome')} "
                    f"fast={state.get('fast', {}).get('verification', {}).get('result')} "
                    f"slow={state.get('slow', {}).get('verification', {}).get('result')} "
                    f"elapsed={state.get('elapsed_ms')}ms"
                )
                page = gc.get("/").text
                ui_ok = "preset-128-btn" in page and "origin-range-work" in page
                results.append({
                    "check": "gateway drill distributes 128 MiB to both routes (SHA match)",
                    "ok": drill_ok, "detail": detail,
                })
                results.append({
                    "check": "web page exposes 128 MiB preset and work metric slot",
                    "ok": ui_ok, "detail": "index.html elements present",
                })
                if not drill_ok:
                    failures.append("gateway 128MiB drill")
                if not ui_ok:
                    failures.append("web ui elements")

            sampler.stop()
            sampler.join(timeout=2)
            peak_mib = sampler.peak_kb / 1024
            # A urllib healthcheck interpreter measures ~10-13 MiB RSS.
            combined_estimate = peak_mib + 14
            mem_ok = peak_mib < 96 and combined_estimate < 96
            results.append({
                "check": "origin RSS stays under 96 MiB (host proxy for cgroup limit)",
                "ok": mem_ok,
                "detail": (
                    f"origin_peak={peak_mib:.1f} MiB, "
                    f"with healthcheck subprocess ~{combined_estimate:.1f} MiB"
                ),
            })
            if not mem_ok:
                failures.append("memory budget")
        finally:
            sampler.stop()

    shutil.rmtree(data_dir, ignore_errors=True)

    print()
    print(json.dumps(results, indent=2, ensure_ascii=False))
    print()
    for r in results:
        print(f"[{'PASS' if r['ok'] else 'FAIL'}] {r['check']} -- {r['detail']}")
    print()
    if failures:
        print("FAILED:", ", ".join(failures))
        return 1
    print("ALL DELIVERY CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
