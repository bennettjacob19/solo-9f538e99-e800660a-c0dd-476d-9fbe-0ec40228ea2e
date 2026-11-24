"""End-to-end drill tests.

The tests run the real FastAPI application over a live server and let the
drill talk to its own origin endpoint via HTTP, so the whole path (main
download, retention, conditional backfill, SHA-256 verification) is covered.

Run with:  python -m pytest -q
"""

from __future__ import annotations

import socket
import threading
import time

import pytest
import uvicorn

from app.drill import DrillParams, DrillRun
from app.main import create_app
from app.origin import OriginStore
from app.retention import RetentionBuffer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def server():
    port = _free_port()
    app = create_app(origin_base=f"http://127.0.0.1:{port}")
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.02)
    assert srv.started, "server did not start"
    try:
        yield app, f"http://127.0.0.1:{port}"
    finally:
        srv.should_exit = True
        thread.join(timeout=10)


def _run_drill(server_base: str, origin_store: OriginStore, **overrides) -> dict:
    import asyncio

    payload = {
        "size_bytes": 512 * 1024,
        "chunk_size": 32 * 1024,
        "chunk_delay_ms": 1,
        "retention_bytes": 32 * 1024,
        "retention_memory_bytes": 16 * 1024,
        "backfill_bytes": 256 * 1024,
        "pause_fraction": 0.75,
    }
    payload.update(overrides)
    params = DrillParams.from_payload(payload)

    async def go():
        run = DrillRun(params, server_base)
        run.start()
        deadline = time.time() + 30
        while run.status == "running" and time.time() < deadline:
            await asyncio.sleep(0.02)
        assert run.status != "running", "drill timed out"
        snap = run.snapshot()
        snap["origin"] = origin_store.stats()
        return snap

    return asyncio.run(go())


# ------------------------------------------------------------------ unit tests
def test_retention_buffer_spills_and_caps():
    buf = RetentionBuffer(capacity_bytes=10, memory_limit_bytes=4)
    assert buf.append(b"abcdef") == 6
    assert buf.stats()["used_memory_bytes"] == 4
    assert buf.stats()["used_disk_bytes"] == 2
    assert buf.append(b"ghijklmn") == 4  # only 4 bytes of room left
    assert buf.dropped_bytes == 4
    assert buf.used_bytes == 10
    assert buf.read(6) == b"abcdef"
    assert buf.read(100) == b"ghij"
    assert buf.used_bytes == 0
    assert buf.stats()["peak_bytes"] == 10
    buf.close()


def test_retention_buffer_memory_only():
    buf = RetentionBuffer(capacity_bytes=8, memory_limit_bytes=8)
    buf.append(b"12345678")
    assert buf.stats()["spilled"] is False
    assert buf.stats()["peak_disk_bytes"] == 0
    buf.close()


def test_origin_versions_are_equal_length_but_different():
    store = OriginStore(size=4096, chunk_size=1024)
    v1, v2 = store.get("v1"), store.get("v2")
    assert v1.size == v2.size == 4096
    assert v1.sha256 != v2.sha256
    assert v1.etag.startswith('"') and not v1.etag.startswith("W/")


# --------------------------------------------------------------- drill tests
def test_both_routes_complete_when_combined_quota_covers_tail(server):
    app, base = server
    snap = _run_drill(
        base,
        app.state.origin_store,
        retention_bytes=32 * 1024,
        backfill_bytes=256 * 1024,
    )
    assert snap["outcome"] == "both_complete"
    assert snap["fast"]["complete"] is True
    assert snap["slow"]["received_bytes"] == snap["total_bytes"]
    assert snap["slow"]["missing_segments"] == []
    assert snap["fast"]["verification"]["result"] == "match"
    assert snap["slow"]["verification"]["result"] == "match"
    assert snap["slow"]["backfill"]["etag_verified"] is True
    assert snap["slow"]["backfill"]["range_verified"] is True
    assert snap["main"]["request_count"] == 1
    assert snap["origin"]["main_requests"] == 1


def test_missing_segment_reported_when_quota_too_small(server):
    app, base = server
    snap = _run_drill(
        base,
        app.state.origin_store,
        retention_bytes=8 * 1024,
        retention_memory_bytes=4 * 1024,
        backfill_bytes=8 * 1024,
    )
    assert snap["outcome"] == "slow_missing"
    assert snap["fast"]["complete"] is True
    assert snap["fast"]["verification"]["result"] == "match"
    assert snap["slow"]["missing_segments"]
    assert snap["slow"]["verification"]["result"] == "incomplete"


def test_version_swap_during_pause_still_serves_old_version(server):
    app, base = server
    snap = _run_drill(
        base,
        app.state.origin_store,
        retention_bytes=8 * 1024,
        retention_memory_bytes=4 * 1024,
        backfill_bytes=8 * 1024,
        auto_switch_on_pause=True,
    )
    # Origin flipped to v2, but the main download and the If-Range backfill both
    # answer for v1 -- so the backfill fails the ETag check and the slow route
    # honestly reports a missing segment rather than mixing versions.
    assert snap["origin_switch"]["triggered"] is True
    assert snap["origin"]["current_version"] == "v2"
    assert snap["slow"]["backfill"]["attempted"] is True
    assert snap["slow"]["backfill"]["status"] == "rejected"
    assert snap["slow"]["backfill"]["etag_verified"] is False
    assert snap["outcome"] == "slow_missing"
    assert snap["fast"]["verification"]["result"] == "match"


def test_version_swap_with_full_retention_still_completes(server):
    app, base = server
    size = 512 * 1024
    snap = _run_drill(
        base,
        app.state.origin_store,
        size_bytes=size,
        retention_bytes=size,
        retention_memory_bytes=size,
        backfill_bytes=0,
        auto_switch_on_pause=True,
    )
    assert snap["origin_switch"]["triggered"] is True
    assert snap["slow"]["backfill"]["attempted"] is False
    assert snap["outcome"] == "both_complete"
    assert snap["slow"]["verification"]["result"] == "match"
    assert snap["fast"]["verification"]["result"] == "match"


def test_origin_serves_200_when_if_range_stale(server):
    import httpx

    app, base = server
    store: OriginStore = app.state.origin_store
    old = store.get("v1")
    with httpx.Client(base_url=base, timeout=10) as client:
        store.switch("v2")
        resp = client.get(
            "/origin/layer",
            headers={"Range": "bytes=0-99", "If-Range": old.etag},
        )
        assert resp.status_code == 200
        assert resp.headers["etag"] == store.get("v2").etag
        assert len(resp.content) == store.get("v2").size
        assert resp.headers["x-if-range-matched"] == "false"
