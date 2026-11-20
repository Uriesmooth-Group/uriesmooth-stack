import json
import math
import multiprocessing
import sys
import threading
import time

from .execution import JobDeadlineExceeded, JobLeaseLost


MAX_RESULT_BYTES = 1024 * 1024
DEFAULT_POLL_INTERVAL = 0.05
PROCESS_GRACE_SECONDS = 1.0


def _encode_message(message):
    """Encode a bounded JSON message for parent-child transport."""
    encoded = json.dumps(
        message,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")

    if len(encoded) > MAX_RESULT_BYTES:
        raise ValueError(
            f"Application result exceeds {MAX_RESULT_BYTES} bytes"
        )

    return encoded


def _execute_child(
    send_connection,
    app_name,
    payload,
    job_id,
    attempt,
    timeout_seconds,
):
    """Execute an application in a separate child process."""
    try:
        from .execution import create_execution_context
        from .registry import run_app

        lease_lost = threading.Event()

        context = create_execution_context(
            job_id=job_id,
            attempt=attempt,
            idempotency_key=job_id,
            timeout_seconds=timeout_seconds,
            lease_lost=lease_lost,
        )

        result = run_app(
            app_name,
            payload,
            context=context,
        )

        context["check_cancelled"]()

        encoded = _encode_message({
            "ok": True,
            "result": result,
        })

    except BaseException as exc:
        try:
            encoded = _encode_message({
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc)[:4000],
            })
        except BaseException:
            encoded = json.dumps({
                "ok": False,
                "error_type": "ChildExecutionError",
                "error": "Application failed and its error could not be encoded",
            }).encode("utf-8")

    try:
        send_connection.send_bytes(encoded)
    except (BrokenPipeError, EOFError, OSError):
        pass
    finally:
        send_connection.close()



def _stop_process(process, grace_seconds=PROCESS_GRACE_SECONDS):
    """Terminate, escalate and reap a child process safely."""
    if process is None:
        return True

    if (
        isinstance(grace_seconds, bool)
        or not isinstance(grace_seconds, (int, float))
        or not math.isfinite(float(grace_seconds))
        or grace_seconds < 0
    ):
        raise ValueError("grace_seconds must be finite and non-negative")

    grace_seconds = float(grace_seconds)

    if not process.is_alive():
        process.join(timeout=0)
        return True

    try:
        process.terminate()
    except OSError:
        # The process may have exited between is_alive() and terminate().
        if not process.is_alive():
            process.join(timeout=0)
            return True
        raise

    process.join(timeout=grace_seconds)

    if process.is_alive():
        try:
            process.kill()
        except OSError:
            # Handle the same exit race during kill.
            if not process.is_alive():
                process.join(timeout=0)
                return True
            raise

        process.join(timeout=grace_seconds)

    stopped = not process.is_alive()
    if stopped:
        process.join(timeout=0)

    return stopped



def _decode_message(receive_connection):
    """Receive and strictly validate one bounded child response."""
    try:
        raw = receive_connection.recv_bytes(maxlength=MAX_RESULT_BYTES)
        message = json.loads(raw.decode("utf-8"))
    except (EOFError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "Could not decode a valid application response"
        ) from exc

    if not isinstance(message, dict):
        raise RuntimeError("Invalid application response envelope")

    if not isinstance(message.get("ok"), bool):
        raise RuntimeError("Application response is missing its status")

    if message["ok"]:
        if "result" not in message:
            raise RuntimeError(
                "Application response is missing its result"
            )
    else:
        error_type = message.get("error_type")
        error = message.get("error")

        if (
            not isinstance(error_type, str)
            or not error_type
            or len(error_type) > 128
        ):
            raise RuntimeError("Application response has an invalid error type")

        if not isinstance(error, str) or len(error) > 4000:
            raise RuntimeError("Application response has an invalid error")

    return message


def _raise_child_error(message):
    """Restore known execution exceptions; safely wrap unknown errors."""
    error_type = message.get("error_type", "UnknownError")
    error = message.get("error", "Application execution failed")

    if not isinstance(error, str):
        error = "Application execution failed"

    if error_type == "JobDeadlineExceeded":
        raise JobDeadlineExceeded(error)

    if error_type == "JobLeaseLost":
        raise JobLeaseLost(error)

    raise RuntimeError(
        f"App execution failed ({error_type}): {error}"
    )


def execute_app(
    job,
    payload,
    timeout_seconds,
    lease_lost,
    poll_interval=DEFAULT_POLL_INTERVAL,
):
    """
    Execute an app in a spawned process.

    The parent retains queue and lease ownership.
    Results must be JSON-serializable and no larger than 1 MiB.
    This provides process-level termination, not a security sandbox.
    """
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
    ):
        raise ValueError("timeout_seconds must be a finite number of at least 1")

    try:
        timeout_seconds = float(timeout_seconds)
    except (OverflowError, ValueError):
        raise ValueError(
            "timeout_seconds must be a finite number of at least 1"
        ) from None

    if not math.isfinite(timeout_seconds) or timeout_seconds < 1:
        raise ValueError("timeout_seconds must be a finite number of at least 1")

    if (
        isinstance(poll_interval, bool)
        or not isinstance(poll_interval, (int, float))
    ):
        raise ValueError("poll_interval must be a finite positive number")

    try:
        poll_interval = float(poll_interval)
    except (OverflowError, ValueError):
        raise ValueError(
            "poll_interval must be a finite positive number"
        ) from None

    if not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("poll_interval must be a finite positive number")

    if not hasattr(lease_lost, "is_set"):
        raise TypeError("lease_lost must provide is_set()")

    for key in ("app_name", "id", "attempts"):
        if key not in job:
            raise ValueError(f"Job is missing required field: {key}")

    context = multiprocessing.get_context("spawn")
    receive_connection, send_connection = context.Pipe(
        duplex=False
    )

    process = context.Process(
        target=_execute_child,
        args=(
            send_connection,
            job["app_name"],
            payload,
            job["id"],
            job["attempts"],
            timeout_seconds,
        ),
        name=f"alphauriel-job-{job['id'][:8]}",
    )

    deadline = time.monotonic() + timeout_seconds
    process_started = False

    try:
        process.start()
        process_started = True

        # The parent must not retain the child's sending endpoint.
        send_connection.close()

        while True:
            if lease_lost.is_set():
                raise JobLeaseLost(
                    f"Lease lost while executing job {job['id']}"
                )

            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise JobDeadlineExceeded(
                    f"Job {job['id']} exceeded its "
                    f"{timeout_seconds}-second execution deadline"
                )

            if receive_connection.poll(
                min(poll_interval, remaining)
            ):
                message = _decode_message(receive_connection)
                break

            if not process.is_alive():
                process.join()

                if receive_connection.poll():
                    message = _decode_message(receive_connection)
                    break

                raise RuntimeError(
                    "Application process exited without returning "
                    f"a result (exit code {process.exitcode})"
                )

        # Do not accept a result from a process that failed to exit.
        process.join(timeout=PROCESS_GRACE_SECONDS)

        if process.is_alive():
            if not _stop_process(process):
                raise RuntimeError(
                    "Application process could not be stopped after "
                    "returning its result"
                )

        if lease_lost.is_set():
            raise JobLeaseLost(
                f"Lease lost while executing job {job['id']}"
            )

        if not message["ok"]:
            _raise_child_error(message)

        if "result" not in message:
            raise RuntimeError(
                "Application response is missing its result"
            )

        return message["result"]

    finally:
        active_error = sys.exception()
        cleanup_errors = []

        for connection in (send_connection, receive_connection):
            try:
                connection.close()
            except Exception as exc:
                cleanup_errors.append(exc)

        if process_started:
            try:
                stopped = _stop_process(process)
            except Exception as exc:
                cleanup_errors.append(exc)
            else:
                if stopped:
                    try:
                        process.close()
                    except Exception as exc:
                        cleanup_errors.append(exc)
                else:
                    cleanup_errors.append(
                        RuntimeError(
                            "Application process remains alive after cleanup"
                        )
                    )

        if cleanup_errors:
            if active_error is not None:
                for cleanup_error in cleanup_errors:
                    active_error.add_note(
                        "Additional process cleanup failure: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
            else:
                raise RuntimeError(
                    "Application process cleanup was incomplete"
                ) from cleanup_errors[0]
