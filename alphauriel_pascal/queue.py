
import json
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta

from .config import (
    DATA_DIR,
    DB_PATH,
    RETRY_BASE_SECONDS,
    RETRY_MAX_SECONDS,
)


def now():
    return datetime.now(timezone.utc).isoformat()


def retry_delay(attempts):
    delay = RETRY_BASE_SECONDS
    maximum = RETRY_MAX_SECONDS

    for _ in range(max(0, int(attempts) - 1)):
        if delay >= maximum / 2:
            return maximum
        delay *= 2

    return min(delay, maximum)


def connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row

    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            app_name TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            result TEXT,
            error TEXT,
            lease_token TEXT,
            available_at TEXT
        )
        """
    )

    columns = {
        row["name"]
        for row in db.execute("PRAGMA table_info(jobs)")
    }

    if "lease_token" not in columns:
        db.execute(
            "ALTER TABLE jobs ADD COLUMN lease_token TEXT"
        )

    if "available_at" not in columns:
        db.execute(
            "ALTER TABLE jobs ADD COLUMN available_at TEXT"
        )

    db.execute(
        "UPDATE jobs SET available_at=created_at "
        "WHERE available_at IS NULL"
    )

    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_pending_available "
        "ON jobs(status, available_at, created_at)"
    )

    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_status_updated "
        "ON jobs(status, updated_at)"
    )

    db.commit()
    return db


def enqueue(app_name, payload=None, max_attempts=3):
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    job_id = uuid.uuid4().hex
    stamp = now()

    with connect() as db:
        db.execute(
            """
            INSERT INTO jobs (
                id, app_name, payload, status,
                attempts, max_attempts,
                created_at, updated_at, available_at
            )
            VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?)
            """,
            (
                job_id,
                app_name,
                json.dumps(payload or {}),
                max_attempts,
                stamp,
                stamp,
                stamp,
            ),
        )

    return job_id


def recover_stale_jobs(lease_seconds=600):
    if lease_seconds < 1:
        raise ValueError("lease_seconds must be positive")

    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(seconds=lease_seconds)
    ).isoformat()

    recovered = 0

    with connect() as db:
        db.execute("BEGIN IMMEDIATE")

        rows = db.execute(
            """
            SELECT id, attempts, max_attempts
            FROM jobs
            WHERE status='running'
              AND updated_at < ?
            """,
            (cutoff,),
        ).fetchall()

        for row in rows:
            stamp = now()
            status = (
                "pending"
                if row["attempts"] < row["max_attempts"]
                else "failed"
            )

            available_at = (
                (
                    datetime.fromisoformat(stamp)
                    + timedelta(seconds=retry_delay(row["attempts"]))
                ).isoformat()
                if status == "pending"
                else stamp
            )

            result = db.execute(
                """
                UPDATE jobs
                SET status=?,
                    error=?,
                    updated_at=?,
                    available_at=?,
                    lease_token=NULL
                WHERE id=?
                  AND status='running'
                  AND updated_at < ?
                """,
                (
                    status,
                    "Worker lease expired; job recovered.",
                    stamp,
                    available_at,
                    row["id"],
                    cutoff,
                ),
            )

            recovered += result.rowcount

    return recovered


def claim(lease_seconds=600):
    recover_stale_jobs(lease_seconds)

    with connect() as db:
        db.execute("BEGIN IMMEDIATE")

        stamp = now()

        row = db.execute(
            """
            SELECT *
            FROM jobs
            WHERE status='pending'
              AND available_at <= ?
            ORDER BY available_at, created_at, id
            LIMIT 1
            """,
            (stamp,),
        ).fetchone()

        if row is None:
            db.commit()
            return None

        token = uuid.uuid4().hex

        result = db.execute(
            """
            UPDATE jobs
            SET status='running',
                attempts=attempts+1,
                updated_at=?,
                lease_token=?
            WHERE id=?
              AND status='pending'
              AND available_at <= ?
            """,
            (stamp, token, row["id"], stamp),
        )

        if result.rowcount != 1:
            db.rollback()
            return None

        db.commit()

        job = dict(row)
        job["attempts"] += 1
        job["updated_at"] = stamp
        job["lease_token"] = token
        job["status"] = "running"

        return job


def heartbeat(job_id, lease_token):
    with connect() as db:
        result = db.execute(
            """
            UPDATE jobs
            SET updated_at=?
            WHERE id=?
              AND status='running'
              AND lease_token=?
            """,
            (now(), job_id, lease_token),
        )

        return result.rowcount == 1


def finish(job_id, result, lease_token=None):
    with connect() as db:
        if lease_token is None:
            cursor = db.execute(
                """
                UPDATE jobs
                SET status='succeeded',
                    result=?,
                    error=NULL,
                    updated_at=?,
                    lease_token=NULL
                WHERE id=?
                  AND status='running'
                """,
                (
                    json.dumps(result, default=str),
                    now(),
                    job_id,
                ),
            )
        else:
            cursor = db.execute(
                """
                UPDATE jobs
                SET status='succeeded',
                    result=?,
                    error=NULL,
                    updated_at=?,
                    lease_token=NULL
                WHERE id=?
                  AND status='running'
                  AND lease_token=?
                """,
                (
                    json.dumps(result, default=str),
                    now(),
                    job_id,
                    lease_token,
                ),
            )

        return cursor.rowcount == 1


def fail(job, error):
    status = (
        "pending"
        if job["attempts"] < job["max_attempts"]
        else "failed"
    )

    stamp = now()

    available_at = (
        (
            datetime.fromisoformat(stamp)
            + timedelta(seconds=retry_delay(job["attempts"]))
        ).isoformat()
        if status == "pending"
        else stamp
    )

    with connect() as db:
        if job.get("lease_token"):
            cursor = db.execute(
                """
                UPDATE jobs
                SET status=?,
                    error=?,
                    updated_at=?,
                    available_at=?,
                    lease_token=NULL
                WHERE id=?
                  AND status='running'
                  AND lease_token=?
                """,
                (
                    status,
                    str(error)[:4000],
                    stamp,
                    available_at,
                    job["id"],
                    job["lease_token"],
                ),
            )
        else:
            cursor = db.execute(
                """
                UPDATE jobs
                SET status=?,
                    error=?,
                    updated_at=?,
                    available_at=?,
                    lease_token=NULL
                WHERE id=?
                  AND status='running'
                """,
                (
                    status,
                    str(error)[:4000],
                    stamp,
                    available_at,
                    job["id"],
                ),
            )

        return cursor.rowcount == 1


def list_jobs(limit=20):
    limit = max(1, min(int(limit), 1000))

    with connect() as db:
        rows = db.execute(
            """
            SELECT id, app_name, status, attempts,
                   created_at, error
            FROM jobs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [dict(row) for row in rows]


def queue_stats():
    statuses = (
        "pending",
        "running",
        "succeeded",
        "failed",
    )

    with connect() as db:
        rows = db.execute(
            """
            SELECT status, COUNT(*) AS total
            FROM jobs
            GROUP BY status
            """
        ).fetchall()

    counts = {
        status: 0
        for status in statuses
    }

    for row in rows:
        if row["status"] in counts:
            counts[row["status"]] = row["total"]

    counts["total"] = sum(counts.values())

    return counts
