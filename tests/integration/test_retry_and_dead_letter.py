from __future__ import annotations

import pytest

from reliqueue.models import EnqueueRequest, JobStatus
from reliqueue.queue import ReliQueue


@pytest.mark.integration
async def test_retry_then_dead_letter(db_pool) -> None:
    queue = ReliQueue(db_pool)

    await queue.enqueue(
        EnqueueRequest(
            job_type="report.generate",
            payload={"report_name": "finance", "account_id": "acc-1"},
            max_retries=0,
            idempotency_key="report-1",
        )
    )
    leased = await queue.lease_jobs(worker_id="worker-a", lease_seconds=5)
    status = await queue.ack_failure(leased[0], worker_id="worker-a", error="boom")
    assert status == JobStatus.DEAD

    async with db_pool.acquire() as conn:
        dead_count = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_jobs")
    assert dead_count == 1
