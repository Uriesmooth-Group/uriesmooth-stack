
import json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import alphauriel_pascal.queue as queue

from alphauriel_pascal.registry import list_apps, run_app


def test_hello_app_is_registered():
    assert any(
        app["name"] == "hello"
        for app in list_apps()
    )


def test_hello_app_runs():
    result = run_app(
        "hello",
        {"message": "Termux test"},
    )

    assert result["message"] == "Termux test"
    assert "timestamp" in result


def test_job_queue_round_trip(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        queue, "DB_PATH", tmp_path / "jobs.sqlite3"
    )

    job_id = queue.enqueue(
        "hello", {"message": "queued test"}, max_attempts=2
    )

    job = queue.claim()

    assert job is not None
    assert job["id"] == job_id
    assert json.loads(job["payload"])["message"] == "queued test"

    assert queue.finish(
        job_id, {"ok": True}, job["lease_token"]
    ) is True

    matching = [
        item for item in queue.list_jobs()
        if item["id"] == job_id
    ]

    assert matching[0]["status"] == "succeeded"


def test_expired_job_is_recovered_and_reclaimed(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        queue, "DB_PATH", tmp_path / "jobs.sqlite3"
    )

    job_id = queue.enqueue(
        "hello", {"message": "recovery test"}, 2
    )

    first = queue.claim(lease_seconds=30)
    assert first["id"] == job_id

    old_token = first["lease_token"]

    stale_time = (
        datetime.now(timezone.utc) - timedelta(minutes=5)
    ).isoformat()

    with queue.connect() as db:
        db.execute(
            "UPDATE jobs SET updated_at=? WHERE id=?",
            (stale_time, job_id),
        )

    assert queue.recover_stale_jobs(30) == 1

    # A recovered job must wait for its retry delay.
    assert queue.claim(lease_seconds=30) is None

    # Move its availability into the past for a deterministic test.
    with queue.connect() as db:
        db.execute(
            "UPDATE jobs SET available_at=? WHERE id=?",
            (
                (
                    datetime.now(timezone.utc)
                    - timedelta(seconds=1)
                ).isoformat(),
                job_id,
            ),
        )

    second = queue.claim(lease_seconds=30)

    assert second["id"] == job_id
    assert second["attempts"] == 2
    assert second["lease_token"] != old_token

    assert queue.finish(
        job_id, {"wrong": True}, old_token
    ) is False

    assert queue.finish(
        job_id, {"ok": True}, second["lease_token"]
    ) is True



def test_retry_delay_uses_exponential_backoff_and_cap(monkeypatch):
    monkeypatch.setattr(queue, "RETRY_BASE_SECONDS", 5)
    monkeypatch.setattr(queue, "RETRY_MAX_SECONDS", 12)

    assert queue.retry_delay(1) == 5
    assert queue.retry_delay(2) == 10
    assert queue.retry_delay(3) == 12
    assert queue.retry_delay(4) == 12


def test_claim_respects_available_at(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        queue, "DB_PATH", tmp_path / "jobs.sqlite3"
    )

    job_id = queue.enqueue("hello", {}, 2)

    with queue.connect() as db:
        row = db.execute(
            "SELECT created_at, available_at FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()

        assert row["available_at"] == row["created_at"]

        future = (
            datetime.now(timezone.utc) + timedelta(minutes=1)
        ).isoformat()

        db.execute(
            "UPDATE jobs SET available_at=? WHERE id=?",
            (future, job_id),
        )

    assert queue.claim() is None

    with queue.connect() as db:
        past = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat()

        db.execute(
            "UPDATE jobs SET available_at=? WHERE id=?",
            (past, job_id),
        )

    job = queue.claim()

    assert job is not None
    assert job["id"] == job_id


def test_expired_job_exhausts_attempts(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        queue, "DB_PATH", tmp_path / "jobs.sqlite3"
    )

    job_id = queue.enqueue("hello", {}, 1)
    job = queue.claim(lease_seconds=30)

    assert job["id"] == job_id

    stale_time = (
        datetime.now(timezone.utc) - timedelta(minutes=5)
    ).isoformat()

    with queue.connect() as db:
        db.execute(
            "UPDATE jobs SET updated_at=? WHERE id=?",
            (stale_time, job_id),
        )

    assert queue.recover_stale_jobs(30) == 1

    matching = [
        item for item in queue.list_jobs()
        if item["id"] == job_id
    ]

    assert matching[0]["status"] == "failed"


def test_concurrent_workers_claim_unique_jobs(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        queue, "DB_PATH", tmp_path / "jobs.sqlite3"
    )

    job_count = 12

    for number in range(job_count):
        queue.enqueue(
            "hello",
            {"message": f"concurrent job {number}"},
            2,
        )

    with ThreadPoolExecutor(max_workers=6) as executor:
        claimed = list(
            executor.map(
                lambda _: queue.claim(),
                range(job_count),
            )
        )

    jobs = [job for job in claimed if job is not None]
    job_ids = [job["id"] for job in jobs]

    assert len(jobs) == job_count
    assert len(set(job_ids)) == job_count

    for job in jobs:
        assert queue.finish(
            job["id"],
            {"ok": True},
            job["lease_token"],
        ) is True

    stats = queue.queue_stats()

    assert stats["succeeded"] == job_count
    assert stats["running"] == 0
    assert stats["pending"] == 0


def test_worker_loop_does_not_claim_after_shutdown(monkeypatch):
    import threading
    import alphauriel_pascal.worker as worker

    stop_event = threading.Event()
    stop_event.set()

    def unexpected_job():
        raise AssertionError(
            "Worker must not claim a job after shutdown"
        )

    monkeypatch.setattr(
        worker,
        "process_one",
        unexpected_job,
    )

    worker.worker_loop(
        stop_event,
        poll_seconds=1,
    )


def test_worker_loop_stops_after_current_iteration(monkeypatch):
    import threading
    import alphauriel_pascal.worker as worker

    stop_event = threading.Event()
    calls = []

    def complete_current_iteration():
        calls.append(True)
        stop_event.set()
        return True

    monkeypatch.setattr(
        worker,
        "process_one",
        complete_current_iteration,
    )

    worker.worker_loop(
        stop_event,
        poll_seconds=1,
    )

    assert len(calls) == 1



def test_maintain_lease_marks_lease_lost_when_heartbeat_returns_false(monkeypatch):
    import threading
    import alphauriel_pascal.worker as worker

    job = {"id": "test-job", "lease_token": "test-token"}
    stop_event = threading.Event()
    lease_lost = threading.Event()

    monkeypatch.setattr(worker, "heartbeat", lambda job_id, token: False)

    class ImmediateEvent:
        def wait(self, interval):
            return False

        def set(self):
            stop_event.set()

    worker._maintain_lease(
        job,
        ImmediateEvent(),
        lease_lost,
        1,
    )

    assert lease_lost.is_set()


def test_maintain_lease_marks_lease_lost_when_heartbeat_raises(monkeypatch):
    import threading
    import alphauriel_pascal.worker as worker

    job = {"id": "test-job", "lease_token": "test-token"}
    lease_lost = threading.Event()

    def failed_heartbeat(job_id, token):
        raise RuntimeError("Temporary database failure")

    monkeypatch.setattr(worker, "heartbeat", failed_heartbeat)

    class ImmediateEvent:
        def wait(self, interval):
            return False

    worker._maintain_lease(
        job,
        ImmediateEvent(),
        lease_lost,
        1,
    )

    assert lease_lost.is_set()


def test_shutdown_signal_sets_stop_event():
    import threading
    import signal
    from alphauriel_pascal.worker import _request_shutdown

    stop_event = threading.Event()

    _request_shutdown(
        signal.SIGTERM,
        None,
        stop_event,
    )

    assert stop_event.is_set()
