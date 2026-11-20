import sqlite3
from datetime import datetime, timedelta, timezone

import alphauriel_pascal.queue as queue


def setup_test_database(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(
        queue, "DB_PATH", tmp_path / "health.sqlite3"
    )


def test_health_reports_degraded_without_workers(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)

    result = queue.system_health()

    assert result["overall"] == "degraded"
    assert result["database"]["status"] == "healthy"
    assert result["workers"]["status"] == "unavailable"


def test_health_reports_fresh_worker_as_healthy(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)

    queue.register_worker("fresh-worker", 2)

    result = queue.system_health()

    assert result["overall"] == "healthy"
    assert result["workers"]["healthy"] == 1
    assert result["workers"]["instances"][0]["health"] == "healthy"


def test_health_reports_stale_worker_as_degraded(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)

    queue.register_worker("stale-worker", 1)

    stale_time = (
        datetime.now(timezone.utc)
        - timedelta(seconds=queue.WORKER_STALE_SECONDS + 60)
    ).isoformat()

    with queue.connect() as db:
        db.execute(
            "UPDATE worker_instances SET heartbeat_at=? "
            "WHERE worker_id=?",
            (stale_time, "stale-worker"),
        )

    result = queue.system_health()

    assert result["overall"] == "degraded"
    assert result["workers"]["stale"] == 1
    assert result["workers"]["instances"][0]["health"] == "stale"


def test_health_reports_stopped_worker(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)

    queue.register_worker("stopped-worker", 1)
    assert queue.stop_worker("stopped-worker")

    result = queue.system_health()

    assert result["overall"] == "degraded"
    assert result["workers"]["instances"][0]["health"] == "stopped"


def test_health_reports_database_failure(monkeypatch):
    def failed_connection():
        raise sqlite3.OperationalError("Simulated database failure")

    monkeypatch.setattr(queue, "connect", failed_connection)

    result = queue.system_health()

    assert result["overall"] == "unhealthy"
    assert result["database"]["status"] == "unhealthy"
    assert result["database"]["error"] == "Database health check failed"
    assert "Simulated database failure" not in result["database"]["error"]


def test_healthy_worker_does_not_hide_stale_worker(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)

    queue.register_worker("healthy-worker", 1)
    queue.register_worker("stale-worker", 1)

    stale_time = (
        datetime.now(timezone.utc)
        - timedelta(seconds=queue.WORKER_STALE_SECONDS + 60)
    ).isoformat()

    with queue.connect() as db:
        db.execute(
            "UPDATE worker_instances SET heartbeat_at=? "
            "WHERE worker_id=?",
            (stale_time, "stale-worker"),
        )

    result = queue.system_health()

    assert result["overall"] == "degraded"
    assert result["workers"]["status"] == "stale"
    assert result["workers"]["healthy"] == 1
    assert result["workers"]["stale"] == 1

    health_by_id = {
        item["worker_id"]: item["health"]
        for item in result["workers"]["instances"]
    }

    assert health_by_id["healthy-worker"] == "healthy"
    assert health_by_id["stale-worker"] == "stale"
