"""Cooperative execution deadlines for application jobs."""

from datetime import datetime, timedelta, timezone
import time


class JobDeadlineExceeded(TimeoutError):
    """Raised when an application exceeds its execution deadline."""


class JobLeaseLost(RuntimeError):
    """Raised when the worker no longer owns the job lease."""


def create_execution_context(
    job_id,
    attempt,
    idempotency_key,
    timeout_seconds,
    lease_lost,
):
    started = time.monotonic()
    deadline = started + timeout_seconds

    deadline_at = (
        datetime.now(timezone.utc)
        + timedelta(seconds=timeout_seconds)
    ).isoformat()

    def remaining_seconds():
        return max(0.0, deadline - time.monotonic())

    def is_cancelled():
        return lease_lost.is_set() or remaining_seconds() <= 0

    def check_cancelled():
        if lease_lost.is_set():
            raise JobLeaseLost(
                f"Lease lost while executing job {job_id}"
            )

        if remaining_seconds() <= 0:
            raise JobDeadlineExceeded(
                f"Job {job_id} exceeded its "
                f"{timeout_seconds}-second execution deadline"
            )

    return {
        "job_id": job_id,
        "attempt": attempt,
        "idempotency_key": idempotency_key,
        "deadline_at": deadline_at,
        "remaining_seconds": remaining_seconds,
        "is_cancelled": is_cancelled,
        "check_cancelled": check_cancelled,
    }
