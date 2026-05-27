from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql://reliqueue:reliqueue@localhost:5432/reliqueue")
    worker_id: str = os.getenv("WORKER_ID", "worker-1")
    lease_seconds: int = int(os.getenv("LEASE_SECONDS", "30"))
    poll_interval_seconds: float = float(os.getenv("POLL_INTERVAL_SECONDS", "0.5"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "10"))
