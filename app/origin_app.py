"""Standalone local origin service (used by docker compose and manual drills)."""

from __future__ import annotations

import os

from fastapi import FastAPI

from .origin import OriginStore, make_origin_router

DEFAULT_LAYER_BYTES = int(os.environ.get("DRILL_LAYER_BYTES", 8 * 1024 * 1024))
DEFAULT_ORIGIN_CHUNK_BYTES = int(os.environ.get("DRILL_ORIGIN_CHUNK_BYTES", 64 * 1024))


def create_origin_app() -> FastAPI:
    app = FastAPI(title="镜像层源站", version="1.0.0")
    store = OriginStore(size=DEFAULT_LAYER_BYTES, chunk_size=DEFAULT_ORIGIN_CHUNK_BYTES)
    app.state.origin_store = store
    app.include_router(make_origin_router(store))

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    return app


app = create_origin_app()
