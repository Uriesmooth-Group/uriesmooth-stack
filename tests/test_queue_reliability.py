import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from alphauriel_pascal import queue


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(queue, "DB_PATH", tmp_path / "jobs.sqlite3")
    yield tmp_path / "jobs.sqlite3"


def read_job(job_id):
    with queue.managed_connection() as db:
        return db.execute(
            "SELECT * FROM jobs WHERE id=?", (job_id,)
        ).fetchone()


def test_idempotent_enqueue_returns_existing_job(isolated_db):
    first = queue.enqueue(
        "demo",
        {"b": 2, "a": 1},
        idempotency_key="request-001",
    )
    second = queue.enqueue(
        "demo",
        {"a": 1, "b": 2},
        idempotency_key="request-001",
    )
    assert first == second

    with queue.managed_connection() as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1


def test_idempotency_conflict_is_rejected(isolated_db):
    queue.enqueue("demo", {"value": 1}, idempotency_key="same-key")

    with pytest.raises(ValueError, match="idempotency"):
        queue.enqueue("demo", {"value": 2}, idempotency_key="same-key")


def test_concurrent_idempotent_enqueue_creates_one_job(isolated_db):
    def submit(_):
        return queue.enqueue(
            "demo",
            {"task": "shared"},
            idempotency_key="concurrent-key",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(submit, range(16)))

    assert len(set(ids)) == 1
    with queue.managed_connection() as db:
        assert db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 1


@pytest.mark.parametrize(
    "app_name,payload,max_attempts,key",
    [
        ("", {}, 3, None),
        ("   ", {}, 3, None),
        (None, {}, 3, None),
        ("demo", {}, True, None),
        ("demo", {}, 1.5, None),
        ("demo", {}, 0, None),
        ("demo", {"bad": float("nan")}, 3, None),
        ("demo", {"bad": float("inf")}, 3, None),
        ("demo", {"bad": object()}, 3, None),
        ("demo", {}, 3, " " * 2),
        ("demo", {}, 3, 123),
    ],
)
def test_enqueue_rejects_invalid_input(
    isolated_db, app_name, payload, max_attempts, key
):
    with pytest.raises((ValueError, TypeError)):
        queue.enqueue(
            app_name,
            payload,
            max_attempts=max_attempts,
            idempotency_key=key,
        )


def test_terminal_failure_creates_dead_letter(isolated_db):
    job_id = queue.enqueue("demo", {"task": "fail"}, max_attempts=1)
    job = queue.claim()
    assert job["id"] == job_id

    assert queue.fail(job, "permanent failure") is True

    events = queue.list_dead_letters()
    assert len(events) == 1
    assert events[0]["job_id"] == job_id
    assert events[0]["reason"] == "attempts_exhausted"
    assert events[0]["resolved_at"] is None
    assert queue.queue_stats()["dead_lettered"] == 1


def test_requeue_preserves_attempt_history_and_allows_new_failure(
    isolated_db,
):
    job_id = queue.enqueue("demo", {}, max_attempts=1)
    first_attempt = queue.claim()
    assert queue.fail(first_attempt, "first failure") is True

    assert queue.requeue_dead_letter(job_id) is True
    requeued = read_job(job_id)
    assert requeued["status"] == "pending"
    assert requeued["attempts"] == 1
    assert requeued["max_attempts"] == 2

    second_attempt = queue.claim()
    assert second_attempt["id"] == job_id
    assert second_attempt["attempts"] == 2
    assert queue.fail(second_attempt, "second failure") is True

    all_events = queue.list_dead_letters(include_resolved=True)
    assert len(all_events) == 2
    assert sum(event["resolved_at"] is not None for event in all_events) == 1
    assert sum(event["resolved_at"] is None for event in all_events) == 1


def test_requeue_rejects_limit_not_above_cumulative_attempts(isolated_db):
    job_id = queue.enqueue("demo", {}, max_attempts=1)
    job = queue.claim()
    assert queue.fail(job, "failed") is True

    with pytest.raises(ValueError, match="cumulative attempts"):
        queue.requeue_dead_letter(job_id, max_attempts=1)

    assert queue.requeue_dead_letter(job_id, max_attempts=3) is True


def test_requeue_requires_unresolved_dead_letter(isolated_db):
    job_id = queue.enqueue("demo", {}, max_attempts=1)
    job = queue.claim()
    assert queue.fail(job, "failed") is True

    assert queue.requeue_dead_letter(job_id) is True
    assert queue.requeue_dead_letter(job_id) is False


def test_exhausted_stale_job_creates_dead_letter(isolated_db):
    job_id = queue.enqueue("demo", {}, max_attempts=1)
    job = queue.claim()

    old_stamp = (
        datetime.now(timezone.utc) - timedelta(seconds=10)
    ).isoformat()

    with queue.managed_connection() as db:
        db.execute(
            "UPDATE jobs SET updated_at=? WHERE id=?",
            (old_stamp, job_id),
        )

    assert queue.recover_stale_jobs(lease_seconds=1) == 1

    events = queue.list_dead_letters()
    assert len(events) == 1
    assert events[0]["reason"] == "lease_exhausted"
    assert read_job(job_id)["status"] == "failed"


def test_stale_worker_cannot_fail_reclaimed_job(isolated_db, monkeypatch):
    monkeypatch.setattr(queue, "retry_delay", lambda attempts: 0)

    job_id = queue.enqueue("demo", {}, max_attempts=3)
    old_worker_job = queue.claim()

    old_stamp = (
        datetime.now(timezone.utc) - timedelta(seconds=10)
    ).isoformat()

    with queue.managed_connection() as db:
        db.execute(
            "UPDATE jobs SET updated_at=?, available_at=? WHERE id=?",
            (old_stamp, old_stamp, job_id),
        )

    new_worker_job = queue.claim(lease_seconds=1)
    assert new_worker_job is not None
    assert new_worker_job["id"] == job_id
    assert new_worker_job["lease_token"] != old_worker_job["lease_token"]

    assert queue.fail(old_worker_job, "late failure") is False
    assert read_job(job_id)["status"] == "running"


def test_version_one_migration_backfills_failed_jobs(isolated_db):
    db = sqlite3.connect(isolated_db)
    db.execute("""
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            result TEXT,
            error TEXT,
            lease_token TEXT,
            available_at TEXT
        )
    """)
    db.execute("""
        CREATE TABLE worker_instances (
            worker_id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            status TEXT NOT NULL
        )
    """)
    db.execute("""
        INSERT INTO jobs (
            id, app_name, payload, status, attempts,
            max_attempts, created_at, updated_at, error, available_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "legacy-failed",
        "demo",
        '{"legacy":true}',
        "failed",
        3,
        3,
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:00:00+00:00",
        "legacy error",
        "2026-01-01T00:00:00+00:00",
    ))
    db.execute("PRAGMA user_version=1")
    db.commit()
    db.close()

    migrated = queue.connect()
    try:
        assert migrated.execute("PRAGMA user_version").fetchone()[0] == 2
        event = migrated.execute("""
            SELECT job_id, reason, error
            FROM dead_letters
            WHERE job_id='legacy-failed'
        """).fetchone()
        assert event["job_id"] == "legacy-failed"
        assert event["reason"] == "migration_backfill"
        assert event["error"] == "legacy error"
        assert migrated.execute(
            "SELECT COUNT(*) FROM jobs"
        ).fetchone()[0] == 1
    finally:
        migrated.close()
