"""Container liveness check for the Alphauriel SQLite database."""

import sys

from alphauriel_pascal.queue import connect


def main():
    db = None

    try:
        db = connect()
        result = db.execute("SELECT 1").fetchone()

        if result is None or result[0] != 1:
            raise RuntimeError("SQLite health query returned an invalid result")

        return 0

    except Exception:
        print("Alphauriel database health check failed", file=sys.stderr)
        return 1

    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
