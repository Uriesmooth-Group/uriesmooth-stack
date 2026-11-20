import threading
from datetime import datetime

import pytest

from alphauriel_pascal import execution


def test_execution_context_contains_job_metadata_and_deadline(
    monkeypatch,
):
    monkeypatch.setattr(execution.time, "monotonic", lambda: 100.0)
    lease_lost = threading.Event()

    context = execution.create_execution_context(
        job_id="job-123",
        attempt=2,
        idempotency_key="key-123",
        timeout_seconds=30,
        lease_lost=lease_lost,
    )

    assert context["job_id"] == "job-123"
    assert context["attempt"] == 2
    assert context["idempotency_key"] == "key-123"
    assert context["remaining_seconds"]() == 30
    assert context["is_cancelled"]() is False
    assert isinstance(
        datetime.fromisoformat(context["deadline_at"]),
        datetime,
    )


def test_execution_context_detects_deadline_expiry(monkeypatch):
    clock = iter([100.0, 102.0])
    monkeypatch.setattr(
        execution.time,
        "monotonic",
        lambda: next(clock),
    )
    lease_lost = threading.Event()

    context = execution.create_execution_context(
        job_id="job-expired",
        attempt=1,
        idempotency_key="key-expired",
        timeout_seconds=1,
        lease_lost=lease_lost,
    )

    with pytest.raises(execution.JobDeadlineExceeded):
        context["check_cancelled"]()


def test_execution_context_detects_lost_lease(monkeypatch):
    monkeypatch.setattr(execution.time, "monotonic", lambda: 100.0)
    lease_lost = threading.Event()

    context = execution.create_execution_context(
        job_id="job-lost",
        attempt=1,
        idempotency_key="key-lost",
        timeout_seconds=30,
        lease_lost=lease_lost,
    )

    lease_lost.set()

    assert context["is_cancelled"]() is True

    with pytest.raises(execution.JobLeaseLost):
        context["check_cancelled"]()


def test_execution_context_reports_remaining_time(monkeypatch):
    clock = iter([100.0, 104.0])
    monkeypatch.setattr(
        execution.time,
        "monotonic",
        lambda: next(clock),
    )
    lease_lost = threading.Event()

    context = execution.create_execution_context(
        job_id="job-remaining",
        attempt=1,
        idempotency_key="key-remaining",
        timeout_seconds=10,
        lease_lost=lease_lost,
    )

    assert context["remaining_seconds"]() == 6
