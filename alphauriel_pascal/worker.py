import json
import logging
import signal
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from .config import (
    POLL_SECONDS,
    LEASE_SECONDS,
    WORKER_HEARTBEAT_SECONDS,
    JOB_TIMEOUT_SECONDS,
)
from .queue import (
    claim,
    fail,
    finish,
    heartbeat,
    register_worker,
    heartbeat_worker,
    stop_worker,
)
from .process_runner import execute_app
from .logging_config import configure_logging





def _maintain_worker_health(
    worker_id,
    health_stop_event,
    worker_stop_event,
):
    while not health_stop_event.wait(WORKER_HEARTBEAT_SECONDS):
        try:
            if not heartbeat_worker(worker_id):
                logging.error(
                    "Worker health registration lost: %s",
                    worker_id,
                )
                worker_stop_event.set()
                return

        except Exception:
            logging.exception(
                "Worker process heartbeat failed: %s",
                worker_id,
            )
            worker_stop_event.set()
            return


def _maintain_lease(job, stop_event, lease_lost, interval):
    while not stop_event.wait(interval):
        try:
            if not heartbeat(job["id"], job["lease_token"]):
                lease_lost.set()
                logging.error(
                    "Lease lost for job %s",
                    job["id"],
                )
                return

        except Exception:
            lease_lost.set()
            logging.exception(
                "Heartbeat failed; lease ownership is unconfirmed "
                "for job %s",
                job["id"],
            )
            return


def process_one():
    job = claim(LEASE_SECONDS)

    if job is None:
        return False

    stop_event = threading.Event()
    lease_lost = threading.Event()

    heartbeat_interval = max(1, LEASE_SECONDS // 3)

    heartbeat_thread = threading.Thread(
        target=_maintain_lease,
        args=(job, stop_event, lease_lost, heartbeat_interval),
        name=f"heartbeat-{job['id'][:8]}",
        daemon=True,
    )

    heartbeat_started = False

    try:
        heartbeat_thread.start()
        heartbeat_started = True

        payload = json.loads(job["payload"])
        result = execute_app(
            job,
            payload,
            JOB_TIMEOUT_SECONDS,
            lease_lost,
        )

        if lease_lost.is_set():
            logging.error(
                "Job %s finished after losing its lease; "
                "result not committed",
                job["id"],
            )

        elif finish(
            job["id"],
            result,
            job["lease_token"],
        ):
            logging.info(
                "Job %s completed (%s)",
                job["id"],
                job["app_name"],
            )

        else:
            logging.warning(
                "Job %s could not be marked complete",
                job["id"],
            )

    except Exception as exc:
        if not lease_lost.is_set():
            if not fail(job, exc):
                logging.warning(
                    "Job %s could not be marked failed",
                    job["id"],
                )

        logging.exception(
            "Job %s failed",
            job["id"],
        )

    finally:
        stop_event.set()
        if heartbeat_started:
            heartbeat_thread.join(timeout=max(1, min(5, heartbeat_interval)))
        if heartbeat_thread.is_alive():
            logging.warning(
                "Lease heartbeat thread remains blocked for job %s; "
                "it is a daemon thread",
                job["id"],
            )

    return True



def _worker_error_backoff(failure_count, base_seconds):
    """Return a capped exponential delay after a worker-loop error."""
    if (
        isinstance(failure_count, bool)
        or not isinstance(failure_count, int)
        or failure_count < 1
    ):
        raise ValueError("failure_count must be a positive integer")

    if (
        isinstance(base_seconds, bool)
        or not isinstance(base_seconds, int)
        or base_seconds < 1
    ):
        raise ValueError("base_seconds must be a positive integer")

    exponent = min(failure_count - 1, 5)
    return min(base_seconds * (2 ** exponent), 30)



def worker_loop(stop_event, poll_seconds, once=False):
    failure_count = 0

    while not stop_event.is_set():
        try:
            worked = process_one()
            failure_count = 0
        except Exception:
            if once:
                raise

            failure_count += 1
            retry_delay = _worker_error_backoff(
                failure_count,
                max(1, poll_seconds),
            )
            logging.exception(
                "Worker loop failed; retrying in %s second(s) "
                "(consecutive failures: %s)",
                retry_delay,
                failure_count,
            )

            if stop_event.wait(retry_delay):
                return
            continue

        if once:
            return

        if not worked:
            stop_event.wait(poll_seconds)


def _request_shutdown(signum, frame, stop_event):
    try:
        signal_name = signal.Signals(signum).name
    except (ValueError, TypeError):
        signal_name = str(signum)

    logging.info(
        "Shutdown requested by %s",
        signal_name,
    )

    stop_event.set()


def _install_signal_handlers(stop_event):
    if threading.current_thread() is not threading.main_thread():
        return {}

    previous_handlers = {}

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[sig] = signal.getsignal(sig)

            signal.signal(
                sig,
                lambda received_signal, frame: _request_shutdown(
                    received_signal,
                    frame,
                    stop_event,
                ),
            )
    except BaseException:
        for sig, handler in previous_handlers.items():
            try:
                signal.signal(sig, handler)
            except Exception:
                logging.exception(
                    "Failed to restore signal handler during startup"
                )
        raise

    return previous_handlers


def _restore_signal_handlers(previous_handlers):
    for sig, handler in previous_handlers.items():
        try:
            signal.signal(sig, handler)
        except Exception:
            logging.exception(
                "Failed to restore signal handler: %s",
                sig,
            )


def run_worker(once=False, poll_seconds=None, workers=1):
    configure_logging()
    if isinstance(workers, bool) or not isinstance(workers, int):
        raise ValueError("workers must be a positive integer")
    if workers < 1:
        raise ValueError("workers must be a positive integer")
    execution_workers = 1 if once else workers
    if poll_seconds is None:
        delay = POLL_SECONDS
    else:
        if isinstance(poll_seconds, bool) or not isinstance(poll_seconds, int):
            raise ValueError("poll_seconds must be a positive integer")
        if poll_seconds < 1:
            raise ValueError("poll_seconds must be a positive integer")
        delay = poll_seconds

    worker_id = uuid.uuid4().hex
    worker_registered = False

    stop_event = threading.Event()
    health_stop_event = threading.Event()

    health_thread = threading.Thread(
        target=_maintain_worker_health,
        args=(worker_id, health_stop_event, stop_event),
        name="alphauriel-health",
        daemon=True,
    )

    executor = None
    futures = []
    health_thread_started = False
    previous_handlers = {}

    try:
        register_worker(worker_id, execution_workers)
        worker_registered = True

        previous_handlers = _install_signal_handlers(stop_event)

        health_thread.start()
        health_thread_started = True

        logging.info(
            "Alphauriel worker started with %s worker(s), id=%s",
            execution_workers,
            worker_id,
        )

        executor = ThreadPoolExecutor(
            max_workers=execution_workers,
            thread_name_prefix="alphauriel",
        )

        for number in range(execution_workers):
            futures.append(
                executor.submit(
                    worker_loop,
                    stop_event,
                    delay,
                    once,
                )
            )

        for future in futures:
            future.result()

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received")
        stop_event.set()

    finally:
        stop_event.set()

        try:
            if executor is not None:
                executor.shutdown(
                    wait=True,
                    cancel_futures=True,
                )
        finally:
            health_stop_event.set()

            try:
                if health_thread_started:
                    health_thread.join(
                        timeout=max(1, min(5, WORKER_HEARTBEAT_SECONDS))
                    )
                    if health_thread.is_alive():
                        logging.warning(
                            "Worker health thread remains blocked; "
                            "it is a daemon thread"
                        )
            finally:
                try:
                    if worker_registered and not stop_worker(worker_id):
                        logging.warning(
                            "Worker registration was not marked stopped: %s",
                            worker_id,
                        )
                except Exception:
                    logging.exception(
                        "Failed to mark worker stopped: %s",
                        worker_id,
                    )
                finally:
                    _restore_signal_handlers(previous_handlers)
                    logging.info("Alphauriel worker stopped")
