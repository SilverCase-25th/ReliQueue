from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

jobs_enqueued_total = Counter("reliqueue_jobs_enqueued_total", "Jobs successfully enqueued", ["job_type"])
jobs_completed_total = Counter("reliqueue_jobs_completed_total", "Jobs completed", ["job_type"])
jobs_failed_total = Counter("reliqueue_jobs_failed_total", "Jobs failed", ["job_type"])
jobs_retried_total = Counter("reliqueue_jobs_retried_total", "Jobs retried", ["job_type"])
queue_depth = Gauge("reliqueue_queue_depth", "Runnable queued jobs")
leased_jobs = Gauge("reliqueue_leased_jobs", "Jobs currently leased")
job_latency_seconds = Histogram("reliqueue_job_latency_seconds", "End-to-end job latency", ["job_type"])


def metrics_response() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
