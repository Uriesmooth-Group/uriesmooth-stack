import threading

import alphauriel_pascal.worker as worker


def test_failed_worker_heartbeat_stops_worker(monkeypatch):
    health_stop_event = threading.Event()
    worker_stop_event = threading.Event()

    monkeypatch.setattr(worker, "WORKER_HEARTBEAT_SECONDS", 0)
    monkeypatch.setattr(
        worker,
        "heartbeat_worker",
        lambda worker_id: False,
    )

    worker._maintain_worker_health(
        "test-worker",
        health_stop_event,
        worker_stop_event,
    )

    assert worker_stop_event.is_set()


def test_worker_heartbeat_exception_stops_worker(monkeypatch):
    health_stop_event = threading.Event()
    worker_stop_event = threading.Event()

    monkeypatch.setattr(worker, "WORKER_HEARTBEAT_SECONDS", 0)

    def failing_heartbeat(worker_id):
        raise RuntimeError("Simulated heartbeat failure")

    monkeypatch.setattr(
        worker,
        "heartbeat_worker",
        failing_heartbeat,
    )

    worker._maintain_worker_health(
        "test-worker",
        health_stop_event,
        worker_stop_event,
    )

    assert worker_stop_event.is_set()


def test_stopped_health_monitor_does_not_stop_worker(monkeypatch):
    health_stop_event = threading.Event()
    worker_stop_event = threading.Event()

    health_stop_event.set()

    def unexpected_heartbeat(worker_id):
        raise AssertionError("Heartbeat should not run")

    monkeypatch.setattr(
        worker,
        "heartbeat_worker",
        unexpected_heartbeat,
    )

    worker._maintain_worker_health(
        "test-worker",
        health_stop_event,
        worker_stop_event,
    )

    assert not worker_stop_event.is_set()


def test_process_one_does_not_finish_job_after_lease_loss(monkeypatch):
    import threading
    import alphauriel_pascal.worker as worker

    heartbeat_called = threading.Event()
    finish_calls = []

    job = {
        "id": "lease-lost-job",
        "lease_token": "lease-token",
        "payload": "{}",
        "attempts": 1,
        "max_attempts": 3,
        "app_name": "test-app",
    }

    monkeypatch.setattr(worker, "LEASE_SECONDS", 1)
    monkeypatch.setattr(worker, "claim", lambda lease: job)

    def lose_lease(job_id, token):
        heartbeat_called.set()
        return False

    from alphauriel_pascal.execution import JobLeaseLost

    def execute(job, payload, timeout, lease_lost):
        assert heartbeat_called.wait(timeout=3)
        raise JobLeaseLost("Lease lost during execution")

    monkeypatch.setattr(worker, "heartbeat", lose_lease)
    monkeypatch.setattr(worker, "execute_app", execute)
    monkeypatch.setattr(
        worker,
        "finish",
        lambda *args: finish_calls.append(args) or True,
    )

    assert worker.process_one() is True
    assert finish_calls == []


def test_process_one_does_not_fail_job_after_lease_loss(monkeypatch):
    import threading
    import alphauriel_pascal.worker as worker

    heartbeat_called = threading.Event()
    fail_calls = []

    job = {
        "id": "lease-lost-error-job",
        "lease_token": "lease-token",
        "payload": "{}",
        "attempts": 1,
        "max_attempts": 3,
        "app_name": "test-app",
    }

    monkeypatch.setattr(worker, "LEASE_SECONDS", 1)
    monkeypatch.setattr(worker, "claim", lambda lease: job)

    def lose_lease(job_id, token):
        heartbeat_called.set()
        return False

    from alphauriel_pascal.execution import JobLeaseLost

    def execute(job, payload, timeout, lease_lost):
        assert heartbeat_called.wait(timeout=3)
        raise JobLeaseLost("Lease lost during application failure")

    monkeypatch.setattr(worker, "heartbeat", lose_lease)
    monkeypatch.setattr(worker, "execute_app", execute)
    monkeypatch.setattr(
        worker,
        "fail",
        lambda *args: fail_calls.append(args) or True,
    )

    assert worker.process_one() is True
    assert fail_calls == []


def test_process_one_fails_job_when_execution_deadline_expires(
    monkeypatch,
):
    import time
    import alphauriel_pascal.worker as worker
    from alphauriel_pascal.execution import JobDeadlineExceeded

    job = {
        "id": "deadline-expired-job",
        "lease_token": "deadline-token",
        "payload": "{}",
        "attempts": 1,
        "max_attempts": 3,
        "app_name": "test-app",
    }

    fail_calls = []
    finish_calls = []

    monkeypatch.setattr(worker, "LEASE_SECONDS", 30)
    monkeypatch.setattr(worker, "JOB_TIMEOUT_SECONDS", 1)
    monkeypatch.setattr(worker, "claim", lambda lease: job)
    monkeypatch.setattr(worker, "heartbeat", lambda *args: True)
    monkeypatch.setattr(
        worker,
        "fail",
        lambda *args: fail_calls.append(args) or True,
    )
    monkeypatch.setattr(
        worker,
        "finish",
        lambda *args: finish_calls.append(args) or True,
    )

    def execute(job, payload, timeout, lease_lost):
        raise JobDeadlineExceeded("Execution deadline exceeded")

    monkeypatch.setattr(worker, "execute_app", execute)

    assert worker.process_one() is True

    assert len(fail_calls) == 1
    assert isinstance(fail_calls[0][1], JobDeadlineExceeded)
    assert finish_calls == []
