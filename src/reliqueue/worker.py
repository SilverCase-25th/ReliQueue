from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable
from contextlib import suppress
from uuid import UUID

from .config import Settings
from .db import create_pool, init_db
from .handlers import DEFAULT_HANDLERS, Handler
from .logging import configure_logging
from .models import JobStatus, LeasedJob
from .queue import ReliQueue, run_with_timeout

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        queue: ReliQueue,
        *,
        worker_id: str,
        handlers: dict[str, Handler],
        lease_seconds: int,
        batch_size: int,
        poll_interval_seconds: float,
    ) -> None:
        self.queue = queue
        self.worker_id = worker_id
        self.handlers = handlers
        self.lease_seconds = lease_seconds
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self._shutdown = asyncio.Event()
        self._running_tasks: set[asyncio.Task[None]] = set()

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def run(self) -> None:
        while not self._shutdown.is_set():
            jobs = await self.queue.lease_jobs(
                worker_id=self.worker_id,
                lease_seconds=self.lease_seconds,
                limit=self.batch_size,
            )
            if not jobs:
                await asyncio.sleep(self.poll_interval_seconds)
                continue

            for job in jobs:
                task = asyncio.create_task(self._process(job), name=f"job-{job.id}")
                self._running_tasks.add(task)
                task.add_done_callback(self._running_tasks.discard)

        if self._running_tasks:
            await asyncio.wait(self._running_tasks)

    async def _process(self, job: LeasedJob) -> None:
        handler = self.handlers.get(job.job_type)
        if handler is None:
            await self.queue.ack_failure(job, worker_id=self.worker_id, error=f"no handler for {job.job_type}")
            return

        heartbeat_task = asyncio.create_task(self._heartbeat(job.id))
        try:
            await run_with_timeout(handler(job.payload), job.timeout_seconds)
            await self.queue.ack_success(job, worker_id=self.worker_id)
            logger.info(
                "job completed",
                extra={
                    "job_id": str(job.id),
                    "job_type": job.job_type,
                    "attempt": job.attempt,
                    "correlation_id": str(job.correlation_id),
                },
            )
        except TimeoutError:
            status = await self.queue.ack_failure(job, worker_id=self.worker_id, error="job timed out")
            logger.warning("job timed out", extra={"job_id": str(job.id), "status": status})
        except Exception as exc:  # noqa: BLE001
            status = await self.queue.ack_failure(job, worker_id=self.worker_id, error=str(exc))
            logger.warning("job failed", extra={"job_id": str(job.id), "status": status})
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

    async def _heartbeat(self, job_id: UUID) -> None:
        interval = max(self.lease_seconds / 2, 1)
        while True:
            await asyncio.sleep(interval)
            renewed = await self.queue.heartbeat(job_id, worker_id=self.worker_id, lease_seconds=self.lease_seconds)
            if not renewed:
                return


async def run_worker(settings: Settings, handlers: dict[str, Handler] | None = None) -> None:
    configure_logging()
    pool = await create_pool(settings.database_url)
    await init_db(pool)
    queue = ReliQueue(pool)
    worker = Worker(
        queue,
        worker_id=settings.worker_id,
        handlers=handlers or DEFAULT_HANDLERS,
        lease_seconds=settings.lease_seconds,
        batch_size=settings.batch_size,
        poll_interval_seconds=settings.poll_interval_seconds,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.request_shutdown)

    try:
        await worker.run()
    finally:
        await pool.close()


def main() -> None:
    asyncio.run(run_worker(Settings()))


if __name__ == "__main__":
    main()
