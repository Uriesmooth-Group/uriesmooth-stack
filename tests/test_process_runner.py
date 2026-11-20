import json
import threading
import time
import uuid
from pathlib import Path

import pytest

from alphauriel_pascal import process_runner
from alphauriel_pascal.execution import (
    JobDeadlineExceeded,
    JobLeaseLost,
)
from alphauriel_pascal.config import APPS_DIR


@pytest.fixture
def create_test_app():
    created = []

    def create(source):
        name = "process_test_" + uuid.uuid4().hex[:12]
        folder = APPS_DIR / name
        folder.mkdir(parents=True)

        (folder / "app.json").write_text(
            json.dumps({
                "name": name,
                "description": "Temporary process supervisor test app",
            }),
            encoding="utf-8",
        )

        (folder / "app.py").write_text(
            source,
            encoding="utf-8",
        )

        created.append(folder)
        return name

    yield create

    for folder in created:
        import shutil
        shutil.rmtree(folder, ignore_errors=True)


def make_job(app_name):
    return {
        "id": uuid.uuid4().hex,
        "app_name": app_name,
        "attempts": 1,
    }


def test_execute_app_returns_json_result(create_test_app):
    app_name = create_test_app(
        """
def run(payload):
    return {
        "message": payload["message"],
        "count": 3,
    }
"""
    )

    result = process_runner.execute_app(
        make_job(app_name),
        {"message": "process execution passed"},
        timeout_seconds=15,
        lease_lost=threading.Event(),
    )

    assert result == {
        "message": "process execution passed",
        "count": 3,
    }


def test_execute_app_preserves_application_failure(create_test_app):
    app_name = create_test_app(
        """
def run(payload):
    raise ValueError("intentional application failure")
"""
    )

    with pytest.raises(RuntimeError, match="ValueError"):
        process_runner.execute_app(
            make_job(app_name),
            {},
            timeout_seconds=15,
            lease_lost=threading.Event(),
        )


def test_execute_app_rejects_non_serializable_result(create_test_app):
    app_name = create_test_app(
        """
def run(payload):
    return {"not_json": object()}
"""
    )

    with pytest.raises(RuntimeError, match="TypeError"):
        process_runner.execute_app(
            make_job(app_name),
            {},
            timeout_seconds=15,
            lease_lost=threading.Event(),
        )


def test_execute_app_rejects_oversized_result(create_test_app):
    app_name = create_test_app(
        """
def run(payload):
    return {"data": "x" * (2 * 1024 * 1024)}
"""
    )

    with pytest.raises(RuntimeError, match="exceeds"):
        process_runner.execute_app(
            make_job(app_name),
            {},
            timeout_seconds=15,
            lease_lost=threading.Event(),
        )


def test_execute_app_enforces_hard_timeout(create_test_app):
    app_name = create_test_app(
        """
import time

def run(payload):
    time.sleep(20)
    return {"unexpected": "completion"}
"""
    )

    started = time.monotonic()

    with pytest.raises(JobDeadlineExceeded):
        process_runner.execute_app(
            make_job(app_name),
            {},
            timeout_seconds=2,
            lease_lost=threading.Event(),
        )

    assert time.monotonic() - started < 10


def test_execute_app_terminates_child_after_lease_loss(create_test_app):
    app_name = create_test_app(
        """
import time

def run(payload):
    time.sleep(20)
    return {"unexpected": "completion"}
"""
    )

    lease_lost = threading.Event()

    def lose_lease():
        time.sleep(1)
        lease_lost.set()

    monitor = threading.Thread(
        target=lose_lease,
        daemon=True,
    )
    monitor.start()

    started = time.monotonic()

    with pytest.raises(JobLeaseLost):
        process_runner.execute_app(
            make_job(app_name),
            {},
            timeout_seconds=15,
            lease_lost=lease_lost,
        )

    monitor.join(timeout=2)

    assert time.monotonic() - started < 10


@pytest.mark.parametrize(
    "timeout",
    [0, -1, True, "10", None],
)
def test_execute_app_rejects_invalid_timeout(timeout):
    with pytest.raises(ValueError):
        process_runner.execute_app(
            {
                "id": "invalid-job",
                "app_name": "unused",
                "attempts": 1,
            },
            {},
            timeout_seconds=timeout,
            lease_lost=threading.Event(),
        )


def test_execute_app_rejects_invalid_poll_interval():
    with pytest.raises(ValueError):
        process_runner.execute_app(
            {
                "id": "invalid-job",
                "app_name": "unused",
                "attempts": 1,
            },
            {},
            timeout_seconds=10,
            lease_lost=threading.Event(),
            poll_interval=0,
        )


def test_execute_app_rejects_missing_job_fields():
    with pytest.raises(ValueError, match="app_name"):
        process_runner.execute_app(
            {"id": "incomplete-job", "attempts": 1},
            {},
            timeout_seconds=10,
            lease_lost=threading.Event(),
        )
