"""FastAPI application for Channel Relevance UI."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from db import init_pool, close_pool, get_filtered_channels, set_channel_relevance, get_distinct_tags, get_summary_stats


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    yield
    await close_pool()


app = FastAPI(title="Channel Relevance UI", lifespan=lifespan)

# Mount static files and templates
_dir = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(_dir, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(_dir, "templates"))


# ─── Pages ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ─── API ────────────────────────────────────────────────────────────────────

@app.get("/api/channels")
async def api_channels(
    lang: str = Query("es", pattern="^(es|en)$"),
    min_views_individual: int = Query(0, ge=0),
    max_views_individual: int | None = Query(None, ge=0),
    min_videos_total: int = Query(0, ge=0),
    max_videos_total: int | None = Query(None, ge=0),
    min_hits_count: int = Query(0, ge=0),
    min_avg_views: int = Query(0, ge=0),
    min_subscribers: int | None = Query(None, ge=0),
    max_subscribers: int | None = Query(None, ge=0),
    is_verified: bool | None = Query(None),
    channel_name_search: str | None = Query(None),
    relevance_filter: str = Query("all", pattern="^(all|unmarked|relevant|not_relevant)$"),
    tag_filter: str | None = Query(None),
    last_uploaded_after: str | None = Query(None),
    last_uploaded_before: str | None = Query(None),
    first_uploaded_after: str | None = Query(None),
    first_uploaded_before: str | None = Query(None),
    sort_by: str = Query("hit_videos_count"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    result = await get_filtered_channels(
        lang,
        min_views_individual=min_views_individual,
        max_views_individual=max_views_individual,
        min_videos_total=min_videos_total,
        max_videos_total=max_videos_total,
        min_hits_count=min_hits_count,
        min_avg_views=min_avg_views,
        min_subscribers=min_subscribers,
        max_subscribers=max_subscribers,
        is_verified=is_verified,
        channel_name_search=channel_name_search,
        relevance_filter=relevance_filter,
        tag_filter=tag_filter,
        last_uploaded_after=last_uploaded_after,
        last_uploaded_before=last_uploaded_before,
        first_uploaded_after=first_uploaded_after,
        first_uploaded_before=first_uploaded_before,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        page_size=page_size,
    )
    return JSONResponse(result)


class RelevanceBody(BaseModel):
    lang: str
    is_relevant: bool | None = None
    notes: str | None = None
    tags: list[str] | None = None


@app.patch("/api/channels/{channel_url:path}/relevance")
async def api_set_relevance(channel_url: str, body: RelevanceBody):
    await set_channel_relevance(
        body.lang,
        channel_url,
        is_relevant=body.is_relevant,
        notes=body.notes,
        tags=body.tags,
    )
    return {"ok": True}


@app.get("/api/tags/{lang}")
async def api_tags(lang: str):
    tags = await get_distinct_tags(lang)
    return {"tags": tags}


@app.get("/api/stats/{lang}")
async def api_stats(lang: str):
    stats = await get_summary_stats(lang)
    return stats
