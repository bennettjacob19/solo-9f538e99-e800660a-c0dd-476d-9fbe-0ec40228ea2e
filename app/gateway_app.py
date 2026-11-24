"""Gateway service: web UI + drill API, talking to the separate origin service."""

from __future__ import annotations

import os

from fastapi import FastAPI

from .main import create_app

ORIGIN_BASE = os.environ.get("DRILL_ORIGIN_BASE", "http://origin:8001")

app: FastAPI = create_app(
    origin_base=ORIGIN_BASE, serve_origin=False, origin_remote=True
)
