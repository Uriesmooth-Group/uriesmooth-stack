import json
import math
import threading

import pytest

from alphauriel_pascal import process_runner as runner


class FakeConnection:
    def __init__(self, payload):
        self.payload = payload

    def recv_bytes(self, maxlength):
        if len(self.payload) > maxlength:
            raise OSError("message too large")
        return self.payload


class FakeProcess:
    def __init__(
        self,
        *,
        terminate_stops=False,
        kill_stops=True,
        terminate_race=False,
    ):
        self.alive = True
        self.terminate_stops = terminate_stops
        self.kill_stops = kill_stops
        self.terminate_race = terminate_race
        self.terminate_calls = 0
        self.kill_calls = 0
        self.join_calls = []
        self.close_calls = 0

    def is_alive(self):
        return self.alive

    def terminate(self):
        self.terminate_calls += 1
        if self.terminate_race:
            self.alive = False
            raise ProcessLookupError("already exited")
        if self.terminate_stops:
            self.alive = False

    def kill(self):
        self.kill_calls += 1
        if self.kill_stops:
            self.alive = False

    def join(self, timeout=None):
        self.join_calls.append(timeout)

    def close(self):
        self.close_calls += 1


def encoded(value):
    return json.dumps(value).encode("utf-8")


def test_stop_process_uses_terminate_and_reaps():
    process = FakeProcess(terminate_stops=True)

    assert runner._stop_process(process, 0.1) is True
    assert process.terminate_calls == 1
    assert process.kill_calls == 0
    assert process.alive is False


def test_stop_process_escalates_to_kill():
    process = FakeProcess(terminate_stops=False, kill_stops=True)

    assert runner._stop_process(process, 0.1) is True
    assert process.terminate_calls == 1
    assert process.kill_calls == 1
    assert process.alive is False


def test_stop_process_handles_terminate_exit_race():
    process = FakeProcess(terminate_race=True)

    assert runner._stop_process(process, 0.1) is True
    assert process.kill_calls == 0


def test_stop_process_reports_process_that_survives_kill():
    process = FakeProcess(kill_stops=False)

    assert runner._stop_process(process, 0) is False
    assert process.alive is True


@pytest.mark.parametrize("grace", [-1, float("nan"), float("inf"), True])
def test_stop_process_rejects_invalid_grace(grace):
    with pytest.raises(ValueError):
        runner._stop_process(FakeProcess(), grace)


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -float("inf"), 0])
def test_execute_app_rejects_nonfinite_or_invalid_timeout(timeout):
    with pytest.raises(ValueError):
        runner.execute_app(
            {"app_name": "demo", "id": "job-1", "attempts": 1},
            {},
            timeout,
            threading.Event(),
        )


@pytest.mark.parametrize(
    "poll",
    [float("nan"), float("inf"), -float("inf"), 0, -1],
)
def test_execute_app_rejects_nonfinite_or_invalid_poll_interval(poll):
    with pytest.raises(ValueError):
        runner.execute_app(
            {"app_name": "demo", "id": "job-1", "attempts": 1},
            {},
            5,
            threading.Event(),
            poll_interval=poll,
        )


def test_decode_message_accepts_success_result():
    message = runner._decode_message(
        FakeConnection(encoded({"ok": True, "result": {"done": True}}))
    )
    assert message["result"] == {"done": True}


def test_decode_message_rejects_success_without_result():
    with pytest.raises(RuntimeError, match="missing its result"):
        runner._decode_message(FakeConnection(encoded({"ok": True})))


def test_decode_message_rejects_invalid_error_fields():
    with pytest.raises(RuntimeError, match="invalid error type"):
        runner._decode_message(
            FakeConnection(encoded({"ok": False, "error_type": 7, "error": "bad"}))
        )

    with pytest.raises(RuntimeError, match="invalid error"):
        runner._decode_message(
            FakeConnection(
                encoded({"ok": False, "error_type": "ValueError", "error": 7})
            )
        )


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"{not-json}",
        b"\xff",
        encoded([]),
        encoded({"ok": "yes", "result": 1}),
    ],
)
def test_decode_message_rejects_malformed_envelopes(payload):
    with pytest.raises(RuntimeError):
        runner._decode_message(FakeConnection(payload))
