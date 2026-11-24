"""FastAPI application: distribution drill UI, API and local origin."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .drill import DrillManager, DrillParams
from .origin import OriginStore, make_origin_router

STATIC_DIR = Path(__file__).resolve().parent / "static"

DEFAULT_LAYER_BYTES = int(os.environ.get("DRILL_LAYER_BYTES", 8 * 1024 * 1024))
DEFAULT_CHUNK_BYTES = int(os.environ.get("DRILL_CHUNK_BYTES", 64 * 1024))
DEFAULT_ORIGIN_CHUNK_BYTES = int(os.environ.get("DRILL_ORIGIN_CHUNK_BYTES", 64 * 1024))


class DrillRequest(BaseModel):
    size_bytes: int = Field(
        default=DEFAULT_LAYER_BYTES, ge=1024, le=128 * 1024 * 1024
    )
    chunk_size: int = Field(default=DEFAULT_CHUNK_BYTES, ge=1024, le=4 * 1024 * 1024)
    chunk_delay_ms: int = Field(default=15, ge=0, le=2000)
    retention_bytes: int = Field(default=1024 * 1024, ge=0, le=128 * 1024 * 1024)
    retention_memory_bytes: int = Field(
        default=512 * 1024, ge=0, le=128 * 1024 * 1024
    )
    backfill_bytes: int = Field(default=2 * 1024 * 1024, ge=0, le=128 * 1024 * 1024)
    auto_switch_on_pause: bool = False
    switch_version: str = Field(default="v2", pattern=r"^[A-Za-z0-9_-]{1,32}$")
    pause_fraction: float = Field(default=0.75, gt=0, lt=1)


def create_app(
    origin_base: Optional[str] = None,
    *,
    serve_origin: bool = True,
    origin_remote: bool = False,
) -> FastAPI:
    """Build the gateway app.

    ``serve_origin`` mounts the origin endpoints in this process; the combined
    single-process mode uses that.  ``origin_remote`` means the origin runs
    elsewhere, so origin counters are read over HTTP instead of from a local
    store (docker compose runs the origin as its own service).
    """

    if not serve_origin and not origin_remote:
        raise ValueError("either serve the origin locally or point at a remote one")

    app = FastAPI(
        title="跨机房镜像层分发演练",
        description=(
            "一个 HTTP 主下载，快慢两路接收；慢端 3/4 暂停后由网关留存或"
            "条件补段，最终两路按原版 SHA-256 校验。"
        ),
        version="1.0.0",
    )

    origin_store = (
        OriginStore(size=DEFAULT_LAYER_BYTES, chunk_size=DEFAULT_ORIGIN_CHUNK_BYTES)
        if serve_origin
        else None
    )
    manager = DrillManager()
    app.state.origin_store = origin_store
    app.state.manager = manager
    app.state.origin_remote = origin_remote

    if origin_base is None:
        origin_base = os.environ.get("DRILL_ORIGIN_BASE", "http://127.0.0.1:8000")
    app.state.origin_base = origin_base.rstrip("/")

    async def origin_snapshot() -> dict[str, Any]:
        if origin_store is not None:
            return {
                "state": origin_store.stats(),
                "versions": origin_store.describe_versions(),
                "remote": False,
            }
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                resp = await client.get(app.state.origin_base + "/origin/state")
                resp.raise_for_status()
                data = resp.json()
            return {
                "state": data.get("state", {}),
                "versions": data.get("versions", []),
                "remote": True,
            }
        except Exception as exc:  # pragma: no cover - depends on network
            return {
                "state": {},
                "versions": [],
                "remote": True,
                "error": f"{type(exc).__name__}: {exc}",
            }

    if serve_origin:
        assert origin_store is not None
        app.include_router(make_origin_router(origin_store))

    async def full_state() -> dict[str, Any]:
        state = manager.snapshot()
        origin = await origin_snapshot()
        state["origin"] = origin.get("state", {})
        state["versions"] = origin.get("versions", [])
        state["origin_remote"] = origin.get("remote", False)
        if "error" in origin:
            state["origin_error"] = origin["error"]
        return state

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/health")
    async def health() -> dict:
        origin = await origin_snapshot()
        return {
            "ok": True,
            "origin_base": app.state.origin_base,
            "origin": origin.get("state", {}),
            "versions": origin.get("versions", []),
        }

    @app.get("/api/drill")
    async def drill_state() -> dict:
        return await full_state()

    @app.post("/api/drill")
    async def drill_start(payload: DrillRequest) -> dict:
        try:
            params = DrillParams.from_payload(payload.model_dump())
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        run = await manager.start(params, app.state.origin_base)
        return {"ok": True, "run_id": run.run_id, "state": await full_state()}

    async def reset_origin(size_bytes: Optional[int]) -> None:
        if origin_store is not None:
            origin_store.reset(size_bytes)
            return
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            params = {"size_bytes": size_bytes} if size_bytes else None
            resp = await client.post(
                app.state.origin_base + "/origin/reset", params=params
            )
            resp.raise_for_status()

    @app.post("/api/drill/reset")
    async def drill_reset(payload: DrillRequest | None = None) -> dict:
        await manager.reset()
        try:
            size_bytes = None
            if payload is not None:
                size_bytes = DrillParams.from_payload(payload.model_dump()).size_bytes
            await reset_origin(size_bytes)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"ok": True, "state": await full_state()}

    @app.post("/api/drill/switch")
    async def drill_switch(version: str = "v2") -> dict:
        """Manual version swap, e.g. while the slow route is paused."""
        if origin_store is not None and version not in origin_store.version_names():
            raise HTTPException(status_code=400, detail="unknown version")
        run = manager.current
        if run is not None and run.status == "running":
            try:
                result = await run.switch_origin(version, source="manual")
            except Exception as exc:  # pragma: no cover - network defensive
                raise HTTPException(status_code=502, detail=str(exc)) from exc
        elif origin_store is not None:
            try:
                result = origin_store.switch(version)
            except KeyError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        else:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(10.0)
                ) as client:
                    resp = await client.post(
                        app.state.origin_base + "/origin/switch",
                        params={"version": version},
                    )
                    resp.raise_for_status()
                    result = resp.json()
            except httpx.HTTPError as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {"ok": True, "origin": result, "state": await full_state()}

    @app.get("/api/origin")
    async def origin_state() -> dict:
        origin = await origin_snapshot()
        return {
            "state": origin.get("state", {}),
            "versions": origin.get("versions", []),
            "remote": origin.get("remote", False),
        }

    return app


app = create_app()
