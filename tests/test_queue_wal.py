import sqlite3

import alphauriel_pascal.queue as queue


class FakeCursor:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return (self.value,)


class FakeDatabase:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.calls = 0

    def execute(self, statement):
        self.calls += 1
        outcome = next(self.outcomes)

        if isinstance(outcome, Exception):
            raise outcome

        return FakeCursor(outcome)


def test_enable_wal_returns_when_already_enabled(tmp_path):
    db = FakeDatabase(["wal"])

    queue._enable_wal(db, tmp_path / "test.sqlite3")

    assert db.calls == 1


def test_enable_wal_retries_lock_and_succeeds(
    tmp_path, monkeypatch
):
    db = FakeDatabase([
        sqlite3.OperationalError("database is locked"),
        "wal",
    ])

    sleeps = []
    monkeypatch.setattr(queue.time, "sleep", sleeps.append)
    monkeypatch.setattr(queue.time, "monotonic", lambda: 0)

    queue._enable_wal(db, tmp_path / "test.sqlite3")

    assert db.calls == 2
    assert sleeps == [0.1]


def test_enable_wal_stops_after_retry_deadline(
    tmp_path, monkeypatch
):
    db = FakeDatabase([
        sqlite3.OperationalError("database is locked"),
    ])

    times = iter([0, 31])
    monkeypatch.setattr(
        queue.time, "monotonic", lambda: next(times)
    )

    try:
        queue._enable_wal(db, tmp_path / "test.sqlite3")
    except sqlite3.OperationalError as exc:
        assert "locked" in str(exc).lower()
    else:
        raise AssertionError("Expected WAL lock error")

    assert db.calls == 1


def test_enable_wal_propagates_non_lock_error(tmp_path):
    db = FakeDatabase([
        sqlite3.OperationalError("disk I/O error"),
    ])

    try:
        queue._enable_wal(db, tmp_path / "test.sqlite3")
    except sqlite3.OperationalError as exc:
        assert "disk I/O error" in str(exc)
    else:
        raise AssertionError("Expected database error")

    assert db.calls == 1
