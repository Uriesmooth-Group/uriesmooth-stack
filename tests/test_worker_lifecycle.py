import pytest
import alphauriel_pascal.worker as worker


class FakeFuture:
    def __init__(self, result=None, error=None):
        self.result_value = result
        self.error = error

    def result(self):
        if self.error:
            raise self.error
        return self.result_value


class FakeExecutor:
    def __init__(self, max_workers, thread_name_prefix):
        self.max_workers = max_workers
        self.thread_name_prefix = thread_name_prefix
        self.shutdown_calls = []

    def submit(self, function, *args):
        try:
            return FakeFuture(function(*args))
        except Exception as exc:
            return FakeFuture(error=exc)

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_calls.append((wait, cancel_futures))


def test_run_worker_starts_and_cleans_up_successfully(monkeypatch):
    calls = {}
    executor_instances = []

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(
        worker,
        "register_worker",
        lambda worker_id, workers: calls.update(
            registered=(worker_id, workers)
        ),
    )
    monkeypatch.setattr(
        worker,
        "_maintain_worker_health",
        lambda *args: None,
    )
    monkeypatch.setattr(
        worker,
        "_install_signal_handlers",
        lambda event: {"test": "previous-handler"},
    )
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id) or True,
    )

    def fake_executor(**kwargs):
        instance = FakeExecutor(**kwargs)
        executor_instances.append(instance)
        return instance

    monkeypatch.setattr(worker, "ThreadPoolExecutor", fake_executor)

    def fake_worker_loop(stop_event, delay, once):
        calls["loop"] = (delay, once)
        stop_event.set()

    monkeypatch.setattr(worker, "worker_loop", fake_worker_loop)

    worker.run_worker(once=True, poll_seconds=2, workers=4)

    assert calls["registered"][1] == 1
    assert calls["loop"] == (2, True)
    assert calls["stopped"] == calls["registered"][0]
    assert calls["restored"] == {"test": "previous-handler"}
    assert executor_instances[0].max_workers == 1
    assert executor_instances[0].shutdown_calls == [(True, True)]


def test_run_worker_registration_failure_skips_worker_cleanup(
    monkeypatch,
):
    calls = {}

    monkeypatch.setattr(worker, "configure_logging", lambda: None)

    def failed_registration(worker_id, workers):
        raise RuntimeError("Registration failed")

    monkeypatch.setattr(worker, "register_worker", failed_registration)
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id),
    )

    with pytest.raises(RuntimeError, match="Registration failed"):
        worker.run_worker()

    assert "stopped" not in calls
    assert calls["restored"] == {}


def test_run_worker_cleans_up_after_worker_task_exception(monkeypatch):
    calls = {}
    executor_instances = []

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(
        worker,
        "register_worker",
        lambda worker_id, workers: calls.update(
            registered=(worker_id, workers)
        ),
    )
    monkeypatch.setattr(
        worker,
        "_maintain_worker_health",
        lambda *args: None,
    )
    monkeypatch.setattr(
        worker,
        "_install_signal_handlers",
        lambda event: {"test": "previous-handler"},
    )
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id) or True,
    )

    def fake_executor(**kwargs):
        instance = FakeExecutor(**kwargs)
        executor_instances.append(instance)
        return instance

    monkeypatch.setattr(worker, "ThreadPoolExecutor", fake_executor)

    def failed_worker_loop(*args):
        raise RuntimeError("Worker task failed")

    monkeypatch.setattr(worker, "worker_loop", failed_worker_loop)

    with pytest.raises(RuntimeError, match="Worker task failed"):
        worker.run_worker(workers=2)

    assert calls["stopped"] == calls["registered"][0]
    assert calls["restored"] == {"test": "previous-handler"}
    assert executor_instances[0].shutdown_calls == [(True, True)]


def test_run_worker_cleans_up_when_executor_creation_fails(monkeypatch):
    calls = {}

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(
        worker,
        "register_worker",
        lambda worker_id, workers: calls.update(
            registered=(worker_id, workers)
        ),
    )
    monkeypatch.setattr(
        worker,
        "_maintain_worker_health",
        lambda *args: None,
    )
    monkeypatch.setattr(
        worker,
        "_install_signal_handlers",
        lambda event: {"test": "previous-handler"},
    )
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id) or True,
    )

    def failed_executor(**kwargs):
        raise RuntimeError("Executor creation failed")

    monkeypatch.setattr(worker, "ThreadPoolExecutor", failed_executor)

    with pytest.raises(RuntimeError, match="Executor creation failed"):
        worker.run_worker()

    assert calls["stopped"] == calls["registered"][0]
    assert calls["restored"] == {"test": "previous-handler"}


def test_run_worker_continues_cleanup_when_executor_shutdown_fails(
    monkeypatch,
):
    calls = {}

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(
        worker,
        "register_worker",
        lambda worker_id, workers: calls.update(
            registered=(worker_id, workers)
        ),
    )
    monkeypatch.setattr(
        worker,
        "_maintain_worker_health",
        lambda *args: None,
    )
    monkeypatch.setattr(
        worker,
        "_install_signal_handlers",
        lambda event: {"test": "previous-handler"},
    )
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id) or True,
    )
    monkeypatch.setattr(worker, "worker_loop", lambda *args: None)

    class BrokenShutdownExecutor(FakeExecutor):
        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))
            raise RuntimeError("Executor shutdown failed")

    executor_instances = []

    def fake_executor(**kwargs):
        instance = BrokenShutdownExecutor(**kwargs)
        executor_instances.append(instance)
        return instance

    monkeypatch.setattr(worker, "ThreadPoolExecutor", fake_executor)

    with pytest.raises(RuntimeError, match="Executor shutdown failed"):
        worker.run_worker(once=True)

    assert calls["stopped"] == calls["registered"][0]
    assert calls["restored"] == {"test": "previous-handler"}
    assert executor_instances[0].shutdown_calls == [(True, True)]


def test_run_worker_cleans_up_when_signal_installation_fails(monkeypatch):
    calls = {}

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(
        worker,
        "register_worker",
        lambda worker_id, workers: calls.update(
            registered=(worker_id, workers)
        ),
    )
    monkeypatch.setattr(
        worker,
        "_install_signal_handlers",
        lambda event: (_ for _ in ()).throw(
            RuntimeError("Signal installation failed")
        ),
    )
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id) or True,
    )

    with pytest.raises(RuntimeError, match="Signal installation failed"):
        worker.run_worker()

    assert calls["stopped"] == calls["registered"][0]
    assert calls["restored"] == {}


def test_run_worker_cleans_up_when_health_thread_start_fails(monkeypatch):
    calls = {}

    monkeypatch.setattr(worker, "configure_logging", lambda: None)
    monkeypatch.setattr(
        worker,
        "register_worker",
        lambda worker_id, workers: calls.update(
            registered=(worker_id, workers)
        ),
    )
    monkeypatch.setattr(
        worker,
        "_install_signal_handlers",
        lambda event: {"test": "previous-handler"},
    )
    monkeypatch.setattr(
        worker,
        "_restore_signal_handlers",
        lambda handlers: calls.update(restored=handlers),
    )
    monkeypatch.setattr(
        worker,
        "stop_worker",
        lambda worker_id: calls.update(stopped=worker_id) or True,
    )

    class FailedStartThread:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def start(self):
            raise RuntimeError("Health thread start failed")

        def join(self):
            calls["joined"] = True

    monkeypatch.setattr(worker.threading, "Thread", FailedStartThread)

    with pytest.raises(RuntimeError, match="Health thread start failed"):
        worker.run_worker()

    assert calls["stopped"] == calls["registered"][0]
    assert calls["restored"] == {"test": "previous-handler"}
    assert "joined" not in calls
