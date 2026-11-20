import threading

import pytest

from alphauriel_pascal import worker


class RecordingStopEvent:
    def __init__(self, stop_after_waits):
        self.stopped = False
        self.stop_after_waits = stop_after_waits
        self.waits = []

    def is_set(self):
        return self.stopped

    def wait(self, seconds):
        self.waits.append(seconds)
        if len(self.waits) >= self.stop_after_waits:
            self.stopped = True
            return True
        return False


@pytest.mark.parametrize(
    "failure_count, expected",
    [
        (1, 1),
        (2, 2),
        (3, 4),
        (4, 8),
        (5, 16),
        (6, 30),
        (7, 30),
        (20, 30),
    ],
)
def test_worker_error_backoff_is_exponential_and_capped(
    failure_count,
    expected,
):
    assert worker._worker_error_backoff(failure_count, 1) == expected


@pytest.mark.parametrize(
    "failure_count, base_seconds",
    [
        (0, 1),
        (-1, 1),
        (True, 1),
        (1, 0),
        (1, -1),
        (1, True),
    ],
)
def test_worker_error_backoff_rejects_invalid_arguments(
    failure_count,
    base_seconds,
):
    with pytest.raises(ValueError):
        worker._worker_error_backoff(failure_count, base_seconds)


def test_worker_loop_recovers_from_transient_process_one_error(monkeypatch):
    calls = []

    def fake_process_one():
        calls.append(True)
        if len(calls) == 1:
            raise OSError("temporary database interruption")
        return False

    monkeypatch.setattr(worker, "process_one", fake_process_one)

    stop_event = RecordingStopEvent(stop_after_waits=2)
    worker.worker_loop(stop_event, poll_seconds=1)

    assert len(calls) == 2
    assert stop_event.waits == [1, 1]
    assert stop_event.is_set()


def test_worker_loop_resets_backoff_after_success(monkeypatch):
    calls = []

    def fake_process_one():
        calls.append(True)
        if len(calls) == 1:
            raise OSError("temporary failure")
        if len(calls) == 2:
            return True
        return False

    monkeypatch.setattr(worker, "process_one", fake_process_one)

    stop_event = RecordingStopEvent(stop_after_waits=2)
    worker.worker_loop(stop_event, poll_seconds=2)

    assert len(calls) == 3
    assert stop_event.waits == [2, 2]
    assert stop_event.is_set()


def test_one_shot_worker_does_not_retry_after_error(monkeypatch):
    calls = []

    def fake_process_one():
        calls.append(True)
        raise OSError("database unavailable")

    monkeypatch.setattr(worker, "process_one", fake_process_one)

    with pytest.raises(OSError, match="database unavailable"):
        worker.worker_loop(threading.Event(), poll_seconds=1, once=True)

    assert len(calls) == 1


@pytest.mark.parametrize("workers", [0, -1, True, 1.5, "2"])
def test_run_worker_rejects_invalid_worker_count(monkeypatch, workers):
    monkeypatch.setattr(worker, "configure_logging", lambda: None)

    with pytest.raises(ValueError, match="workers"):
        worker.run_worker(workers=workers)


@pytest.mark.parametrize("poll_seconds", [0, -1, True, 1.5, "2"])
def test_run_worker_rejects_invalid_poll_interval(
    monkeypatch,
    poll_seconds,
):
    monkeypatch.setattr(worker, "configure_logging", lambda: None)

    with pytest.raises(ValueError, match="poll_seconds"):
        worker.run_worker(poll_seconds=poll_seconds)
