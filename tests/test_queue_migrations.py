import sqlite3

import pytest

import alphauriel_pascal.queue as queue


def make_database(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    return db


def test_legacy_database_migration_preserves_existing_jobs(tmp_path):
    db_path = tmp_path / "legacy.sqlite3"
    db = make_database(db_path)

    db.execute(
        """
        CREATE TABLE jobs (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            result TEXT,
            error TEXT
        )
        """
    )

    db.execute(
        """
        INSERT INTO jobs (
            id, app_name, payload, status, attempts,
            max_attempts, created_at, updated_at, result, error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-job-1",
            "hello",
            '{"message": "preserve me"}',
            "pending",
            0,
            3,
            "2026-01-01T10:00:00+00:00",
            "2026-01-01T10:00:00+00:00",
            None,
            None,
        ),
    )
    db.commit()

    queue._initialize_schema(db)

    job = db.execute(
        "SELECT * FROM jobs WHERE id=?",
        ("legacy-job-1",),
    ).fetchone()

    columns = {
        row["name"]
        for row in db.execute("PRAGMA table_info(jobs)")
    }

    indexes = {
        row["name"]
        for row in db.execute("PRAGMA index_list(jobs)")
    }

    worker_table = db.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='worker_instances'
        """
    ).fetchone()

    version = db.execute("PRAGMA user_version").fetchone()[0]

    assert job is not None
    assert job["app_name"] == "hello"
    assert job["payload"] == '{"message": "preserve me"}'
    assert job["status"] == "pending"
    assert job["available_at"] == job["created_at"]
    assert job["lease_token"] is None

    assert "lease_token" in columns
    assert "available_at" in columns
    assert "idx_jobs_pending_available" in indexes
    assert "idx_jobs_status_updated" in indexes
    assert worker_table is not None
    assert version == queue.SCHEMA_VERSION

    db.close()


def test_new_database_receives_complete_schema(tmp_path):
    db = make_database(tmp_path / "new.sqlite3")

    queue._initialize_schema(db)

    tables = {
        row["name"]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }

    assert "jobs" in tables
    assert "worker_instances" in tables
    assert db.execute("PRAGMA user_version").fetchone()[0] == (
        queue.SCHEMA_VERSION
    )

    db.close()


def test_database_newer_than_supported_schema_is_rejected(tmp_path):
    db = make_database(tmp_path / "future.sqlite3")
    future_version = queue.SCHEMA_VERSION + 1

    db.execute(f"PRAGMA user_version = {future_version}")

    with pytest.raises(RuntimeError, match="newer than supported"):
        queue._initialize_schema(db)

    assert db.execute("PRAGMA user_version").fetchone()[0] == future_version

    db.close()


def test_failed_migration_rolls_back_schema_changes(tmp_path):
    db = make_database(tmp_path / "broken.sqlite3")
    db.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY)")
    db.commit()

    with pytest.raises(sqlite3.OperationalError):
        queue._initialize_schema(db)

    columns = {
        row["name"]
        for row in db.execute("PRAGMA table_info(jobs)")
    }
    worker_table = db.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='worker_instances'
        """
    ).fetchone()

    assert columns == {"id"}
    assert worker_table is None
    assert db.execute("PRAGMA user_version").fetchone()[0] == 0

    db.close()
