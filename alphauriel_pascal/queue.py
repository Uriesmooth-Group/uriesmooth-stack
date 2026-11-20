
import json
import logging
import os
import sqlite3
import time
import threading
from contextlib import contextmanager
import uuid
from datetime import datetime, timezone, timedelta

SCHEMA_VERSION = 2
_WAL_LOCK = threading.Lock()


from .config import (
    DATA_DIR,
    DB_PATH,
    RETRY_BASE_SECONDS,
    RETRY_MAX_SECONDS,
    WORKER_STALE_SECONDS,
)



def _enable_wal(db, database_path):
    with _WAL_LOCK:
        deadline = time.monotonic() + 30

        while True:
            try:
                mode = db.execute(
                    "PRAGMA journal_mode"
                ).fetchone()[0]

                if mode.lower() == "wal":
                    return

                mode = db.execute(
                    "PRAGMA journal_mode=WAL"
                ).fetchone()[0]

                if mode.lower() != "wal":
                    raise RuntimeError(
                        f"Could not enable WAL mode: {mode}"
                    )

                return

            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise

                if time.monotonic() >= deadline:
                    raise

                time.sleep(0.1)



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


def _initialize_schema(db):
    version = db.execute("PRAGMA user_version").fetchone()[0]

    if version > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {version} is newer "
            f"than supported version {SCHEMA_VERSION}"
        )

    if version == SCHEMA_VERSION:
        return

    db.execute("BEGIN IMMEDIATE")
    try:
        version = db.execute("PRAGMA user_version").fetchone()[0]

        if version > SCHEMA_VERSION:
            raise RuntimeError("Unsupported future database schema")

        if version < 1:
            db.execute("""
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
            """)

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

            db.execute("""
                UPDATE jobs SET available_at=created_at
                WHERE available_at IS NULL
            """)

            db.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_pending_available
                ON jobs(status, available_at, created_at)
            """)

            db.execute("""
                CREATE INDEX IF NOT EXISTS idx_jobs_status_updated
                ON jobs(status, updated_at)
            """)

            db.execute("""
                CREATE TABLE IF NOT EXISTS worker_instances (
                    worker_id TEXT PRIMARY KEY,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    stopped_at TEXT,
                    status TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    workers INTEGER NOT NULL
                )
            """)

            db.execute("PRAGMA user_version = 1")
            version = 1

        if version < 2:
            columns = {
                row["name"]
                for row in db.execute("PRAGMA table_info(jobs)")
            }

            if "idempotency_key" not in columns:
                db.execute(
                    "ALTER TABLE jobs ADD COLUMN idempotency_key TEXT"
                )

            db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
                ON jobs(idempotency_key)
                WHERE idempotency_key IS NOT NULL
            """)

            db.execute("""
                CREATE TABLE IF NOT EXISTS dead_letters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    app_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    error TEXT NOT NULL,
                    failed_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    resolved_at TEXT
                )
            """)

            db.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS
                idx_dead_letters_unresolved_job
                ON dead_letters(job_id)
                WHERE resolved_at IS NULL
            """)

            db.execute("""
                INSERT INTO dead_letters (
                    job_id, app_name, payload, attempts,
                    max_attempts, error, failed_at, reason
                )
                SELECT
                    j.id, j.app_name, j.payload, j.attempts,
                    j.max_attempts,
                    COALESCE(j.error, 'Job failed before migration.'),
                    j.updated_at, 'migration_backfill'
                FROM jobs AS j
                WHERE j.status='failed'
                  AND NOT EXISTS (
                      SELECT 1 FROM dead_letters AS d
                      WHERE d.job_id=j.id
                        AND d.resolved_at IS NULL
                  )
            """)

            db.execute("PRAGMA user_version = 2")

        db.commit()
    except Exception:
        if db.in_transaction:
            db.rollback()
        raise


def connect():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = sqlite3.Row

    try:
        db.execute("PRAGMA busy_timeout=30000")
        _enable_wal(db, DB_PATH)
        _initialize_schema(db)
        return db

    except Exception:
        db.close()
        raise


@contextmanager
def managed_connection():
    db = connect()
    try:
        with db:
            yield db
    finally:
        db.close()


def _record_dead_letter(db, job_row, error, stamp, reason):
    db.execute("""
        INSERT INTO dead_letters (
            job_id, app_name, payload, attempts,
            max_attempts, error, failed_at, reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_row["id"], job_row["app_name"], job_row["payload"],
        job_row["attempts"], job_row["max_attempts"],
        str(error)[:4000], stamp, reason,
    ))


def enqueue(app_name, payload=None, max_attempts=3, idempotency_key=None):
    if (
        not isinstance(app_name, str)
        or not app_name.strip()
        or len(app_name.strip()) > 128
    ):
        raise ValueError("app_name must be a non-empty string up to 128 characters")
    app_name = app_name.strip()

    if (
        isinstance(max_attempts, bool)
        or not isinstance(max_attempts, int)
        or max_attempts < 1
    ):
        raise ValueError("max_attempts must be a positive integer")

    if idempotency_key is not None:
        if not isinstance(idempotency_key, str):
            raise ValueError("idempotency_key must be a string")
        idempotency_key = idempotency_key.strip()
        if not 1 <= len(idempotency_key) <= 255:
            raise ValueError("idempotency_key must contain 1 to 255 characters")

    try:
        payload_json = json.dumps(
            payload if payload is not None else {},
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"payload must be valid JSON data: {exc}") from exc

    job_id = uuid.uuid4().hex
    stamp = now()

    with managed_connection() as db:
        db.execute("BEGIN IMMEDIATE")

        if idempotency_key is not None:
            existing = db.execute("""
                SELECT id, app_name, payload, max_attempts
                FROM jobs WHERE idempotency_key=?
            """, (idempotency_key,)).fetchone()

            if existing is not None:
                if (
                    existing["app_name"] != app_name
                    or existing["payload"] != payload_json
                    or existing["max_attempts"] != max_attempts
                ):
                    raise ValueError(
                        "idempotency_key already exists with different job data"
                    )
                return existing["id"]

        db.execute("""
            INSERT INTO jobs (
                id, app_name, payload, status, attempts,
                max_attempts, created_at, updated_at,
                idempotency_key, available_at
            )
            VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?, ?)
        """, (
            job_id, app_name, payload_json, max_attempts,
            stamp, stamp, idempotency_key, stamp,
        ))

    return job_id


def recover_stale_jobs(lease_seconds=600):
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, (int, float))
        or lease_seconds < 1
    ):
        raise ValueError("lease_seconds must be positive")

    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(seconds=lease_seconds)
    ).isoformat()
    recovered = 0

    with managed_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        rows = db.execute("""
            SELECT id, app_name, payload, attempts, max_attempts
            FROM jobs
            WHERE status='running' AND updated_at < ?
            ORDER BY updated_at, id
        """, (cutoff,)).fetchall()

        for row in rows:
            stamp = now()
            error = "Worker lease expired; job recovered."
            status = (
                "pending" if row["attempts"] < row["max_attempts"]
                else "failed"
            )
            available_at = (
                (
                    datetime.fromisoformat(stamp)
                    + timedelta(seconds=retry_delay(row["attempts"]))
                ).isoformat()
                if status == "pending" else stamp
            )

            updated = db.execute("""
                UPDATE jobs
                SET status=?, error=?, updated_at=?,
                    available_at=?, lease_token=NULL
                WHERE id=? AND status='running' AND updated_at < ?
            """, (status, error, stamp, available_at, row["id"], cutoff))

            if updated.rowcount == 1:
                recovered += 1
                if status == "failed":
                    _record_dead_letter(
                        db, row, error, stamp, "lease_exhausted"
                    )

    return recovered


def claim(lease_seconds=600):
    recover_stale_jobs(lease_seconds)

    with managed_connection() as db:
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
    with managed_connection() as db:
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
    if not lease_token:
        return False

    with managed_connection() as db:
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
    lease_token = job.get("lease_token")
    job_id = job.get("id")
    if not lease_token or not job_id:
        return False

    stamp = now()
    error_text = str(error)[:4000]

    with managed_connection() as db:
        db.execute("BEGIN IMMEDIATE")
        current = db.execute("""
            SELECT id, app_name, payload, attempts, max_attempts
            FROM jobs
            WHERE id=? AND status='running' AND lease_token=?
        """, (job_id, lease_token)).fetchone()

        if current is None:
            return False

        status = (
            "pending" if current["attempts"] < current["max_attempts"]
            else "failed"
        )
        available_at = (
            (
                datetime.fromisoformat(stamp)
                + timedelta(seconds=retry_delay(current["attempts"]))
            ).isoformat()
            if status == "pending" else stamp
        )

        updated = db.execute("""
            UPDATE jobs
            SET status=?, error=?, updated_at=?,
                available_at=?, lease_token=NULL
            WHERE id=? AND status='running' AND lease_token=?
        """, (
            status, error_text, stamp, available_at, job_id, lease_token,
        ))

        if updated.rowcount != 1:
            return False

        if status == "failed":
            _record_dead_letter(
                db, current, error_text, stamp, "attempts_exhausted"
            )
        return True


def list_jobs(limit=20):
    limit = max(1, min(int(limit), 1000))

    with managed_connection() as db:
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


def list_dead_letters(limit=100, include_resolved=False):
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise ValueError("limit must be an integer")
    limit = max(1, min(limit, 1000))

    with managed_connection() as db:
        condition = "" if include_resolved else "WHERE resolved_at IS NULL"
        rows = db.execute(f"""
            SELECT id, job_id, app_name, payload, attempts,
                   max_attempts, error, failed_at, reason, resolved_at
            FROM dead_letters
            {condition}
            ORDER BY failed_at DESC, id DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return [dict(row) for row in rows]


def requeue_dead_letter(job_id, max_attempts=None):
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("job_id must be a non-empty string")

    if max_attempts is not None and (
        isinstance(max_attempts, bool)
        or not isinstance(max_attempts, int)
        or max_attempts < 1
    ):
        raise ValueError("max_attempts must be a positive integer")

    stamp = now()
    with managed_connection() as db:
        db.execute("BEGIN IMMEDIATE")

        event = db.execute("""
            SELECT id FROM dead_letters
            WHERE job_id=? AND resolved_at IS NULL
            ORDER BY failed_at DESC, id DESC LIMIT 1
        """, (job_id,)).fetchone()

        job = db.execute("""
            SELECT attempts, max_attempts FROM jobs
            WHERE id=? AND status='failed'
        """, (job_id,)).fetchone()

        if event is None or job is None:
            return False

        new_limit = max_attempts
        if new_limit is None:
            new_limit = max(job["max_attempts"], job["attempts"] + 1)

        if new_limit <= job["attempts"]:
            raise ValueError(
                "max_attempts must exceed the job's cumulative attempts"
            )

        updated = db.execute("""
            UPDATE jobs
            SET status='pending', max_attempts=?, error=NULL,
                updated_at=?, available_at=?, lease_token=NULL
            WHERE id=? AND status='failed'
        """, (new_limit, stamp, stamp, job_id))

        if updated.rowcount != 1:
            return False

        resolved = db.execute("""
            UPDATE dead_letters SET resolved_at=?
            WHERE id=? AND resolved_at IS NULL
        """, (stamp, event["id"]))

        if resolved.rowcount != 1:
            raise RuntimeError("Could not resolve dead-letter event")

        return True



def queue_stats():
    statuses = ("pending", "running", "succeeded", "failed")

    with managed_connection() as db:
        rows = db.execute("""
            SELECT status, COUNT(*) AS total
            FROM jobs GROUP BY status
        """).fetchall()
        dead_lettered = db.execute("""
            SELECT COUNT(*) FROM dead_letters
            WHERE resolved_at IS NULL
        """).fetchone()[0]

    counts = {status: 0 for status in statuses}
    for row in rows:
        if row["status"] in counts:
            counts[row["status"]] = row["total"]

    counts["total"] = sum(counts.values())
    counts["dead_lettered"] = dead_lettered
    return counts


def register_worker(worker_id, workers):
    stamp = now()

    with managed_connection() as db:
        db.execute(
            """
            INSERT INTO worker_instances (
                worker_id,
                started_at,
                heartbeat_at,
                stopped_at,
                status,
                pid,
                workers
            )
            VALUES (?, ?, ?, NULL, 'running', ?, ?)
            """,
            (
                worker_id,
                stamp,
                stamp,
                os.getpid(),
                max(1, int(workers)),
            ),
        )


def heartbeat_worker(worker_id):
    stamp = now()

    with managed_connection() as db:
        cursor = db.execute(
            """
            UPDATE worker_instances
            SET heartbeat_at=?
            WHERE worker_id=?
              AND status='running'
            """,
            (stamp, worker_id),
        )

    return cursor.rowcount == 1


def stop_worker(worker_id):
    stamp = now()

    with managed_connection() as db:
        cursor = db.execute(
            """
            UPDATE worker_instances
            SET status='stopped',
                stopped_at=?,
                heartbeat_at=?
            WHERE worker_id=?
              AND status='running'
            """,
            (stamp, stamp, worker_id),
        )

    return cursor.rowcount == 1

def system_health():
    try:
        with managed_connection() as db:
            db.execute("SELECT 1").fetchone()
            rows = db.execute(
                """
                SELECT worker_id, started_at, heartbeat_at,
                       stopped_at, status, pid, workers
                FROM worker_instances
                ORDER BY started_at DESC
                """
            ).fetchall()
    except Exception:
        logging.exception("Database health check failed")
        return {
            "overall": "unhealthy",
            "database": {
                "status": "unhealthy",
                "error": "Database health check failed",
            },
            "workers": {
                "status": "unknown",
                "active": 0,
                "healthy": 0,
                "stale": 0,
                "instances": [],
            },
            "worker_stale_after_seconds": WORKER_STALE_SECONDS,
        }

    current_time = datetime.now(timezone.utc)
    instances = []
    healthy = 0
    stale = 0
    active = 0

    for row in rows:
        item = dict(row)
        age = None

        if item["status"] == "running":
            active += 1
            try:
                heartbeat_time = datetime.fromisoformat(
                    item["heartbeat_at"]
                )
                if heartbeat_time.tzinfo is None:
                    heartbeat_time = heartbeat_time.replace(
                        tzinfo=timezone.utc
                    )
                age = max(
                    0,
                    (current_time - heartbeat_time).total_seconds(),
                )
            except (TypeError, ValueError):
                age = None

            if age is not None and age <= WORKER_STALE_SECONDS:
                item["health"] = "healthy"
                healthy += 1
            else:
                item["health"] = "stale"
                stale += 1
        else:
            item["health"] = "stopped"

        item["heartbeat_age_seconds"] = (
            round(age, 2) if age is not None else None
        )
        instances.append(item)

    if stale:
        worker_status = "stale"
        overall = "degraded"
    elif healthy:
        worker_status = "healthy"
        overall = "healthy"
    else:
        worker_status = "unavailable"
        overall = "degraded"

    return {
        "overall": overall,
        "database": {"status": "healthy"},
        "workers": {
            "status": worker_status,
            "active": active,
            "healthy": healthy,
            "stale": stale,
            "instances": instances,
        },
        "worker_stale_after_seconds": WORKER_STALE_SECONDS,
    }
