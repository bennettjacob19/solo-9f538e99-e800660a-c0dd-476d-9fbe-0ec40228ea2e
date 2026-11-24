"""Tests for the memory-bounded file origin: compatibility, work metering,
generation leases and reclamation after resize / disconnect.
"""

from __future__ import annotations

import hashlib
import os
import socket
import time
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from app.origin import (
    SMALL_RANGE_WORK_LIMIT,
    OriginStore,
    build_version,
    iter_layer_bytes,
    make_origin_router,
)


# ----------------------------------------------------------- golden constants
GOLDEN = {
    1024: {
        "v1": "9d990d823f62ce90ed3a4832aa69eff123019437ff5352f9136970b575afa2d4",
        "v2": "1b05066ada6ebcb6701e4a1d32dab8975e41c95820d180aea746a1baeb874056",
    },
    524288: {
        "v1": "2e3a614ec4b65aa3fca17c4cc727ecfba448afa4e6b0e035e489b6f66a6c7dc6",
        "v2": "417bdfc95667d8ab4ace82b313a850821325b2a1c06ab2d1b94d7e041ccf7a58",
    },
    8 * 1024 * 1024: {
        "v1": "8ba3b2941869e5c1c6f93f23e8c7ff75a14b1a8353d9f585805f5371e9727b6d",
        "v2": "f81f74d8573dd6dfd8f543981a80b6eec2f3adae51db706a3b5ae4f62c44fe83",
    },
}


# ------------------------------------------------------------------ fixtures
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def origin_server(tmp_path):
    data_dir = tmp_path / "layers"
    store = OriginStore(size=64 * 1024, chunk_size=16 * 1024, data_dir=data_dir)
    app = FastAPI()
    app.include_router(make_origin_router(store))
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    srv = uvicorn.Server(config)
    thread = __import__("threading").Thread(target=srv.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not srv.started and time.time() < deadline:
        time.sleep(0.02)
    assert srv.started
    try:
        yield store, f"http://127.0.0.1:{port}", data_dir
    finally:
        srv.should_exit = True
        thread.join(timeout=10)
        store.close()


def _wait_reclaimed(store: OriginStore, minimum: int = 1, timeout: float = 6.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = store.stats()["storage"]
        if snap["reclaimed_generations"] >= minimum and snap["generation_count"] == 1:
            return snap
        time.sleep(0.05)
    raise AssertionError(
        "old generation was not reclaimed: " + str(store.stats()["storage"])
    )


# ------------------------------------------------------- byte/etag/sha parity
@pytest.mark.parametrize("size", [0, 1, 2, 3, 4, 5, 7, 63, 64, 65, 4095, 4096, 4097, 200001])
def test_streaming_generator_is_identical_to_legacy_randbytes(size):
    for name, seed in (("v1", 20261123), ("v2", 20261124)):
        legacy = build_version(name, size, seed)
        streamed = b"".join(iter_layer_bytes(seed, size))
        assert streamed == legacy.data
        assert hashlib.sha256(streamed).hexdigest() == legacy.sha256
        assert legacy.etag == f'"{name}-{legacy.sha256}"'


@pytest.mark.parametrize("size", [1024, 524288, 8 * 1024 * 1024])
def test_store_versions_match_locked_goldens(tmp_path, size):
    store = OriginStore(size=size, chunk_size=65536, data_dir=tmp_path)
    try:
        for name in ("v1", "v2"):
            version = store.get(name)
            assert version.sha256 == GOLDEN[size][name]
            assert version.etag == f'"{name}-{GOLDEN[size][name]}"'
            assert version.size == size
            # And the file on disk really is that body.
            assert hashlib.sha256(Path(version.path).read_bytes()).hexdigest() == GOLDEN[size][name]
    finally:
        store.close()


# ------------------------------------------------------------ lease lifecycle
def test_resize_retires_and_reclaims_generation_after_lease(tmp_path):
    store = OriginStore(size=4096, chunk_size=1024, data_dir=tmp_path)
    old, lease = store.lease_current()  # simulates an in-flight streamed response
    old_path = Path(old.path)
    assert old_path.exists()

    summary = store.reset(8192)
    assert summary["resized"] is True
    assert store.current().size == 8192
    storage = store.stats()["storage"]
    assert storage["generation_count"] == 2  # current + retired-with-user

    # Old generation cannot be deleted while leased; bytes stay readable.
    assert old_path.exists()
    assert hashlib.sha256(old_path.read_bytes()).hexdigest() == old.sha256

    # Releasing twice must be harmless (generator + response safety net).
    lease.release()
    lease.release()
    deadline = time.time() + 2
    while old_path.exists() and time.time() < deadline:
        time.sleep(0.02)
    assert not old_path.exists()
    snap = store.stats()["storage"]
    assert snap["reclaimed_generations"] == 1
    assert snap["generation_count"] == 1
    store.close()


def test_response_releases_lease_even_if_iteration_never_starts(tmp_path):
    import asyncio

    from app.origin import LeasedStreamingResponse, _stream_body

    store = OriginStore(size=4096, chunk_size=1024, data_dir=tmp_path)
    version, lease = store.lease_current()
    record = store.begin_request(
        kind="main", range_header=None, if_range=None,
        version=version, status=200, range_length=version.size,
    )
    body = _stream_body(store, version, 0, version.size - 1, record, lease)
    response = LeasedStreamingResponse(body, lease=lease, status_code=200)

    # Resize retires the leased generation while the response has not run.
    store.reset(8192)
    assert store.stats()["storage"]["generation_count"] == 2

    async def go():
        async def receive():
            return {"type": "http.disconnect"}

        async def send(_message):
            # Fail on the very first send (response.start), before the body
            # iterator is touched by Starlette.
            raise OSError("client gone")

        scope = {"type": "http", "asgi": {"spec_version": "2.4"}}
        with pytest.raises(Exception):
            await response(scope, receive, send)

    asyncio.run(go())

    assert lease.released is True
    snap = store.stats()["storage"]
    assert snap["generation_count"] == 1
    assert snap["reclaimed_generations"] == 1
    store.close()


def test_failed_reset_keeps_current_generation(tmp_path, monkeypatch):
    store = OriginStore(size=4096, chunk_size=1024, data_dir=tmp_path)
    before = store.current()

    def boom(*a, **k):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(store, "_build_generation", boom)
    with pytest.raises(OSError):
        store.reset(8192)

    after = store.current()
    assert after.etag == before.etag
    assert after.size == before.size
    assert Path(after.path).read_bytes() == Path(before.path).read_bytes()
    assert store.stats()["storage"]["generation_count"] == 1
    store.close()


# ------------------------------------------------------------- HTTP metering
def test_small_range_records_actual_work_within_1mib(origin_server):
    store, base, _ = origin_server
    with httpx.Client(base_url=base, timeout=10) as client:
        client.post("/origin/reset", params={"size_bytes": 1024 * 1024})
        full = client.get("/origin/layer").content
        assert len(full) == 1024 * 1024

        resp = client.get("/origin/layer", headers={"Range": "bytes=100-199"})
        assert resp.status_code == 206
        assert resp.content == full[100:200]
        state = client.get("/origin/state").json()["state"]
        rec = state["requests"][0]
        assert rec["range_length"] == 100
        assert rec["bytes_sent"] == 100
        assert rec["body_bytes_used"] == 100
        assert rec["positioning_bytes"] == 0
        assert rec["work_limit_bytes"] == SMALL_RANGE_WORK_LIMIT
        assert rec["work_within_limit"] is True

        # Exactly 64 KiB tail-aligned window: still far below 1 MiB.
        resp = client.get("/origin/layer", headers={"Range": "bytes=0-65535"})
        assert resp.status_code == 206 and len(resp.content) == 65536
        rec = client.get("/origin/state").json()["state"]["requests"][0]
        assert rec["body_bytes_used"] == 65536
        assert rec["work_within_limit"] is True

        # Suffix range near the very end (the expensive case for naive RNG).
        resp = client.get("/origin/layer", headers={"Range": "bytes=-65536"})
        assert resp.status_code == 206
        assert resp.headers["content-range"] == (
            f"bytes {1024*1024-65536}-{1024*1024-1}/{1024*1024}"
        )
        assert resp.content == full[-65536:]
        rec = client.get("/origin/state").json()["state"]["requests"][0]
        assert rec["body_bytes_used"] == 65536
        assert rec["work_within_limit"] is True

        # Larger than the small-range class: budget flag is not applicable,
        # actual usage is still reported.
        resp = client.get("/origin/layer", headers={"Range": "bytes=0-131071"})
        assert resp.status_code == 206 and len(resp.content) == 131072
        rec = client.get("/origin/state").json()["state"]["requests"][0]
        assert rec["body_bytes_used"] == 131072
        assert rec["work_within_limit"] is None


def test_range_and_if_range_rules_remain_compatible(origin_server):
    _, base, _ = origin_server
    with httpx.Client(base_url=base, timeout=10) as client:
        client.post("/origin/reset", params={"size_bytes": 65536})
        etag = client.get("/origin/state").json()["versions"][0]["etag"]

        r = client.get("/origin/layer", headers={"Range": "bytes=10-19"})
        assert r.status_code == 206
        assert r.headers["etag"] == etag
        assert r.headers["content-range"] == "bytes 10-19/65536"
        assert r.headers["x-if-range-matched"] == "not_sent"

        r = client.get(
            "/origin/layer",
            headers={"Range": "bytes=10-19", "If-Range": etag},
        )
        assert r.status_code == 206
        assert r.headers["x-if-range-matched"] == "true"

        r = client.get(
            "/origin/layer",
            headers={"Range": "bytes=0-99", "If-Range": '"v2-does-not-match"'},
        )
        assert r.status_code == 200
        assert r.headers["x-if-range-matched"] == "false"
        assert len(r.content) == 65536

        r = client.get("/origin/layer", headers={"Range": "bytes=999999-"})
        assert r.status_code == 416
        assert r.headers["content-range"] == "bytes */65536"


# --------------------------------------------- resize keeps old download whole
def test_inflight_download_finishes_with_old_headers_after_resize(origin_server):
    store, base, data_dir = origin_server
    # Large enough that loopback buffers cannot drain the whole body while the
    # client is not reading, so the server-side lease is genuinely held across
    # the reset.
    old_size = 16 * 1024 * 1024
    new_size = 8 * 1024 * 1024
    with httpx.Client(base_url=base, timeout=60) as client:
        client.post("/origin/reset", params={"size_bytes": old_size})

        with client.stream("GET", "/origin/layer") as resp:
            assert resp.status_code == 200
            old_etag = resp.headers["etag"]
            old_sha = resp.headers["x-content-sha256"]
            assert resp.headers["content-length"] == str(old_size)

            received = bytearray()
            iterator = resp.iter_bytes(256 * 1024)
            for chunk in iterator:
                received.extend(chunk)
                if len(received) >= 2 * 1024 * 1024:
                    break  # leave the download well and truly in flight

            client.post("/origin/reset", params={"size_bytes": new_size})
            assert store.current().size == new_size

            for chunk in iterator:
                received.extend(chunk)

        assert len(received) == old_size
        assert hashlib.sha256(bytes(received)).hexdigest() == old_sha
        # The old ETag must be a strong v1 validator.
        assert old_etag.startswith('"v1-') and not old_etag.startswith("W/")
        assert store.current().etag != old_etag

        snap = _wait_reclaimed(store)
        assert snap["generations"][0]["size"] == new_size

    # No layer file for the old generation remains on disk.
    leftovers = [p.name for p in Path(data_dir).glob("*.bin")]
    assert len(leftovers) == 2


def test_disconnect_reclaims_retired_generation(origin_server):
    store, base, _ = origin_server
    big_size = 64 * 1024 * 1024
    with httpx.Client(base_url=base, timeout=60) as client:
        client.post("/origin/reset", params={"size_bytes": big_size})
        with client.stream("GET", "/origin/layer") as resp:
            assert resp.status_code == 200
            iterator = resp.iter_bytes(256 * 1024)
            got = 0
            for chunk in iterator:
                got += len(chunk)
                if got >= 1024 * 1024:
                    break
            # Resize retires the big generation while we hold the stream.
            client.post("/origin/reset", params={"size_bytes": 512 * 1024})
            assert store.stats()["storage"]["generation_count"] == 2
            # Exiting the `with` block closes the connection -> generator
            # gets cancelled and releases the lease.
        snap = _wait_reclaimed(store)
        assert snap["reclaimed_generations"] >= 1
        assert snap["generation_count"] == 1


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses directory permissions")
def test_failed_reset_over_http_keeps_layer_downloadable(origin_server):
    store, base, data_dir = origin_server
    with httpx.Client(base_url=base, timeout=30) as client:
        client.post("/origin/reset", params={"size_bytes": 1024 * 1024})
        before = client.get("/origin/state").json()["state"]
        old_etag = before["current_etag"]

        os.chmod(data_dir, 0o500)
        try:
            bad = client.post("/origin/reset", params={"size_bytes": 2 * 1024 * 1024})
            assert bad.status_code == 500
        finally:
            os.chmod(data_dir, 0o700)

        state = client.get("/origin/state").json()["state"]
        assert state["current_etag"] == old_etag
        assert store.current().size == 1024 * 1024

        r = client.get("/origin/layer", headers={"Range": "bytes=0-4095"})
        assert r.status_code == 206
        assert r.headers["etag"] == old_etag
        assert len(r.content) == 4096
