import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
APPS_DIR = PROJECT_ROOT / "apps"


def _positive_int(variable, default):
    raw_value = os.environ.get(variable, str(default)).strip()

    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        raise ValueError(
            f"{variable} must be a positive integer; got {raw_value!r}"
        ) from None

    if value < 1:
        raise ValueError(
            f"{variable} must be a positive integer; got {raw_value!r}"
        )

    return value


DATA_DIR = Path(
    os.environ.get(
        "ALPHAURIEL_HOME",
        str(Path.home() / ".uriesmooth-stack"),
    )
).expanduser()

DB_PATH = DATA_DIR / "jobs.sqlite3"

POLL_SECONDS = _positive_int("ALPHAURIEL_POLL_SECONDS", 3)
MAX_ATTEMPTS = _positive_int("ALPHAURIEL_MAX_ATTEMPTS", 3)

JOB_TIMEOUT_SECONDS = _positive_int(
    "ALPHAURIEL_JOB_TIMEOUT_SECONDS", 300
)

LEASE_SECONDS = max(
    30,
    _positive_int("ALPHAURIEL_LEASE_SECONDS", 600),
)

WORKER_HEARTBEAT_SECONDS = _positive_int(
    "ALPHAURIEL_WORKER_HEARTBEAT_SECONDS",
    10,
)

WORKER_STALE_SECONDS = max(
    WORKER_HEARTBEAT_SECONDS * 2,
    _positive_int("ALPHAURIEL_WORKER_STALE_SECONDS", 30),
)

RETRY_BASE_SECONDS = _positive_int(
    "ALPHAURIEL_RETRY_BASE_SECONDS",
    5,
)

RETRY_MAX_SECONDS = max(
    RETRY_BASE_SECONDS,
    _positive_int("ALPHAURIEL_RETRY_MAX_SECONDS", 300),
)
