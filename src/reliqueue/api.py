from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response

from .config import Settings
from .db import create_pool, init_db
from .metrics import metrics_response
from .models import EnqueueRequest, EnqueueResult
from .queue import ReliQueue

settings = Settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool = await create_pool(settings.database_url)
    await init_db(pool)
    app.state.pool = pool
    app.state.queue = ReliQueue(pool)
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(title="ReliQueue API", version="0.1.0", lifespan=lifespan)


@app.post("/jobs", response_model=EnqueueResult)
async def enqueue_job(request: EnqueueRequest) -> EnqueueResult:
    try:
        return await app.state.queue.enqueue(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: UUID) -> dict[str, bool]:
    cancelled = await app.state.queue.request_cancel(job_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Job not found or already terminal")
    return {"cancel_requested": True}


@app.get("/metrics")
async def metrics() -> Response:
    await app.state.queue.queue_depth()
    content, content_type = metrics_response()
    return Response(content=content, media_type=content_type)
