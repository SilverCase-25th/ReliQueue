from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from .metrics import (
    job_latency_seconds,
    jobs_completed_total,
    jobs_enqueued_total,
    jobs_failed_total,
    jobs_retried_total,
    leased_jobs,
    queue_depth,
)
from .models import EnqueueRequest, EnqueueResult, JobStatus, LeasedJob, validate_payload

logger = logging.getLogger(__name__)


class ReliQueue:
    def __init__(self, pool: asyncpg.Pool, *, jitter_seed: int = 42) -> None:
        self.pool = pool
        self._random = random.Random(jitter_seed)

    async def enqueue(self, request: EnqueueRequest) -> EnqueueResult:
        validate_payload(request.job_type, request.payload)
        job_id = uuid4()
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    INSERT INTO jobs (
                        id, job_type, payload, status, idempotency_key, max_retries, run_at, timeout_seconds
                    ) VALUES ($1, $2, $3::jsonb, 'queued', $4, $5, $6, $7)
                    RETURNING id, status
                    """,
                    job_id,
                    request.job_type,
                    json.dumps(request.payload),
                    request.idempotency_key,
                    request.max_retries,
                    request.run_at,
                    request.timeout_seconds,
                )
                jobs_enqueued_total.labels(request.job_type).inc()
                return EnqueueResult(job_id=row["id"], status=row["status"], deduplicated=False)
            except asyncpg.UniqueViolationError:
                existing = await conn.fetchrow(
                    """
                    SELECT id, status
                    FROM jobs
                    WHERE job_type = $1 AND idempotency_key = $2
                    """,
                    request.job_type,
                    request.idempotency_key,
                )
                if existing is None:
                    raise
                return EnqueueResult(job_id=existing["id"], status=existing["status"], deduplicated=True)

    async def lease_jobs(self, *, worker_id: str, lease_seconds: int, limit: int = 1) -> list[LeasedJob]:
        correlation_id = uuid4()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH candidate AS (
                    SELECT id
                    FROM jobs
                    WHERE status IN ('queued', 'leased')
                      AND run_at <= NOW()
                      AND cancel_requested = FALSE
                      AND (status = 'queued' OR leased_until <= NOW())
                    ORDER BY run_at ASC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                )
                UPDATE jobs j
                SET status = 'leased',
                    lease_owner = $2,
                    leased_until = NOW() + make_interval(secs => $3::int),
                    last_heartbeat_at = NOW(),
                    attempt = j.attempt + 1,
                    correlation_id = $4,
                    updated_at = NOW()
                FROM candidate
                WHERE j.id = candidate.id
                RETURNING j.id, j.job_type, j.payload, j.attempt, j.max_retries, j.timeout_seconds, j.correlation_id, j.created_at
                """,
                limit,
                worker_id,
                lease_seconds,
                correlation_id,
            )
            leased_jobs.set(len(rows))
            return [
                LeasedJob(
                    id=row["id"],
                    job_type=row["job_type"],
                    payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else dict(row["payload"]),
                    attempt=row["attempt"],
                    max_retries=row["max_retries"],
                    timeout_seconds=row["timeout_seconds"],
                    correlation_id=row["correlation_id"],
                    created_at=row["created_at"],
                )
                for row in rows
            ]

    async def heartbeat(self, job_id: UUID, *, worker_id: str, lease_seconds: int) -> bool:
        async with self.pool.acquire() as conn:
            updated = await conn.execute(
                """
                UPDATE jobs
                SET leased_until = NOW() + make_interval(secs => $1::int),
                    last_heartbeat_at = NOW(),
                    updated_at = NOW()
                WHERE id = $2
                  AND status = 'leased'
                  AND lease_owner = $3
                """,
                lease_seconds,
                job_id,
                worker_id,
            )
        return updated.endswith("1")

    async def ack_success(self, job: LeasedJob, *, worker_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE jobs
                SET status = 'completed',
                    lease_owner = NULL,
                    leased_until = NULL,
                    updated_at = NOW(),
                    finished_at = NOW()
                WHERE id = $1
                  AND status = 'leased'
                  AND lease_owner = $2
                """,
                job.id,
                worker_id,
            )
        jobs_completed_total.labels(job.job_type).inc()
        job_latency_seconds.labels(job.job_type).observe((datetime.now(UTC) - job.created_at).total_seconds())

    async def request_cancel(self, job_id: UUID) -> bool:
        async with self.pool.acquire() as conn:
            updated = await conn.execute(
                """
                UPDATE jobs
                SET cancel_requested = TRUE, updated_at = NOW()
                WHERE id = $1
                  AND status IN ('queued', 'leased')
                """,
                job_id,
            )
        return updated.endswith("1")

    async def ack_failure(self, job: LeasedJob, *, worker_id: str, error: str) -> JobStatus:
        if job.attempt > job.max_retries:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """
                        UPDATE jobs
                        SET status = 'dead',
                            lease_owner = NULL,
                            leased_until = NULL,
                            last_error = $3,
                            updated_at = NOW(),
                            finished_at = NOW()
                        WHERE id = $1
                          AND status = 'leased'
                          AND lease_owner = $2
                        """,
                        job.id,
                        worker_id,
                        error,
                    )
                    await conn.execute(
                        """
                        INSERT INTO dead_letter_jobs (id, original_job_id, job_type, payload, attempts, last_error)
                        VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        uuid4(),
                        job.id,
                        job.job_type,
                        json.dumps(job.payload),
                        job.attempt,
                        error,
                    )
            jobs_failed_total.labels(job.job_type).inc()
            return JobStatus.DEAD

        delay = self.retry_delay_seconds(job.attempt)
        async with self.pool.acquire() as conn:
            updated = await conn.fetchval(
                """
                UPDATE jobs
                SET status = CASE WHEN cancel_requested THEN 'cancelled' ELSE 'queued' END,
                    lease_owner = NULL,
                    leased_until = NULL,
                    run_at = CASE WHEN cancel_requested THEN run_at ELSE NOW() + make_interval(secs => $4::int) END,
                    last_error = $3,
                    updated_at = NOW(),
                    finished_at = CASE WHEN cancel_requested THEN NOW() ELSE NULL END
                WHERE id = $1
                  AND status = 'leased'
                  AND lease_owner = $2
                RETURNING status
                """,
                job.id,
                worker_id,
                error,
                delay,
            )
        if updated == JobStatus.CANCELLED:
            return JobStatus.CANCELLED
        jobs_retried_total.labels(job.job_type).inc()
        return JobStatus.QUEUED

    def retry_delay_seconds(self, attempt: int, *, base_seconds: int = 2, max_seconds: int = 300) -> int:
        exp = min(max_seconds, base_seconds * (2 ** max(attempt - 1, 0)))
        jitter = self._random.uniform(0, exp * 0.5)
        return int(exp + jitter)

    async def queue_depth(self) -> int:
        async with self.pool.acquire() as conn:
            depth = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM jobs
                WHERE status = 'queued'
                  AND run_at <= NOW()
                """
            )
        queue_depth.set(depth)
        return int(depth)


async def run_with_timeout(coro: Any, timeout_seconds: int) -> Any:
    async with asyncio.timeout(timeout_seconds):
        return await coro
