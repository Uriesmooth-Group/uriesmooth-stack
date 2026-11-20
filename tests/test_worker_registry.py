import alphauriel_pascal.queue as queue


def setup_test_database(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, "DATA_DIR", tmp_path)
    monkeypatch.setattr(queue, "DB_PATH", tmp_path / "workers.sqlite3")


def test_register_worker_creates_running_instance(tmp_path, monkeypatch):
    setup_test_database(tmp_path, monkeypatch)

    queue.register_worker("registry-worker", 3)

    with queue.connect() as db:
        row = db.execute(
            "SELECT * FROM worker_instances WHERE worker_id=?",
            ("registry-worker",),
        ).fetchone()

    assert row is not None
    assert row["status"] == "running"
    assert row["workers"] == 3
    assert row["pid"] > 0
    assert row["started_at"] == row["heartbeat_at"]
    assert row["stopped_at"] is None


def test_worker_heartbeat_updates_only_running_instance(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)
    queue.register_worker("heartbeat-worker", 1)

    assert queue.heartbeat_worker("heartbeat-worker") is True
    assert queue.heartbeat_worker("missing-worker") is False

    assert queue.stop_worker("heartbeat-worker") is True
    assert queue.heartbeat_worker("heartbeat-worker") is False


def test_stop_worker_is_idempotently_rejected_after_first_stop(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)
    queue.register_worker("shutdown-worker", 2)

    assert queue.stop_worker("shutdown-worker") is True
    assert queue.stop_worker("shutdown-worker") is False

    with queue.connect() as db:
        row = db.execute(
            "SELECT status, stopped_at FROM worker_instances "
            "WHERE worker_id=?",
            ("shutdown-worker",),
        ).fetchone()

    assert row["status"] == "stopped"
    assert row["stopped_at"] is not None


def test_worker_registration_normalizes_worker_count(
    tmp_path, monkeypatch
):
    setup_test_database(tmp_path, monkeypatch)
    queue.register_worker("normalized-worker", 0)

    with queue.connect() as db:
        row = db.execute(
            "SELECT workers FROM worker_instances WHERE worker_id=?",
            ("normalized-worker",),
        ).fetchone()

    assert row["workers"] == 1
