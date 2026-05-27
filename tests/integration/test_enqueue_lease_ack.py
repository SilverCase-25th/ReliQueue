from __future__ import annotations

import pytest

from reliqueue.models import EnqueueRequest, JobStatus
from reliqueue.queue import ReliQueue


@pytest.mark.integration
async def test_enqueue_lease_execute_ack(db_pool) -> None:
    queue = ReliQueue(db_pool)

    created = await queue.enqueue(
        EnqueueRequest(
            job_type="email.send",
            payload={"to": "a@company.com", "subject": "hi", "body": "hello"},
            idempotency_key="email-1",
        )
    )
    assert created.status == JobStatus.QUEUED

    duplicate = await queue.enqueue(
        EnqueueRequest(
            job_type="email.send",
            payload={"to": "a@company.com", "subject": "hi", "body": "hello"},
            idempotency_key="email-1",
        )
    )
    assert duplicate.deduplicated is True
    assert duplicate.job_id == created.job_id

    leased = await queue.lease_jobs(worker_id="worker-a", lease_seconds=10, limit=1)
    assert len(leased) == 1
    job = leased[0]

    await queue.ack_success(job, worker_id="worker-a")

    async with db_pool.acquire() as conn:
        status = await conn.fetchval("SELECT status FROM jobs WHERE id = $1", job.id)
    assert status == JobStatus.COMPLETED
