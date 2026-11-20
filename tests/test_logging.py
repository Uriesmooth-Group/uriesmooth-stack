import json
import logging

import pytest

from alphauriel_pascal.logging_config import (
    JsonFormatter,
    configure_logging,
)


def test_json_formatter_outputs_structured_log():
    record = logging.LogRecord(
        name="alphauriel.worker",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    output = JsonFormatter().format(record)
    data = json.loads(output)

    assert data["level"] == "INFO"
    assert data["component"] == "alphauriel.worker"
    assert data["message"] == "Test message"
    assert data["timestamp"].endswith("+00:00")


def test_configure_logging_rejects_invalid_format(monkeypatch):
    monkeypatch.setenv("ALPHAURIEL_LOG_FORMAT", "invalid")

    with pytest.raises(ValueError, match="Invalid ALPHAURIEL_LOG_FORMAT"):
        configure_logging()


def test_configure_logging_rejects_invalid_level(monkeypatch):
    monkeypatch.setenv("ALPHAURIEL_LOG_FORMAT", "text")
    monkeypatch.setenv("ALPHAURIEL_LOG_LEVEL", "INVALID")

    with pytest.raises(ValueError, match="Invalid ALPHAURIEL_LOG_LEVEL"):
        configure_logging()


def test_configure_logging_rejects_notset_level(monkeypatch):
    monkeypatch.setenv("ALPHAURIEL_LOG_FORMAT", "text")
    monkeypatch.setenv("ALPHAURIEL_LOG_LEVEL", "NOTSET")

    with pytest.raises(ValueError, match="Invalid ALPHAURIEL_LOG_LEVEL"):
        configure_logging()


def test_configure_logging_preserves_existing_third_party_handler(monkeypatch):
    import io

    root = logging.getLogger()
    third_party_handler = logging.StreamHandler(io.StringIO())
    original_formatter = logging.Formatter("THIRD-PARTY %(message)s")

    third_party_handler.setFormatter(original_formatter)

    monkeypatch.setattr(root, "handlers", [third_party_handler])
    monkeypatch.setattr(root, "level", root.level)
    monkeypatch.setenv("ALPHAURIEL_LOG_FORMAT", "json")
    monkeypatch.setenv("ALPHAURIEL_LOG_LEVEL", "INFO")

    configure_logging()

    assert root.handlers == [third_party_handler]
    assert third_party_handler.formatter is original_formatter


def test_configure_logging_does_not_duplicate_own_handler(monkeypatch):
    root = logging.getLogger()

    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(root, "level", root.level)
    monkeypatch.setenv("ALPHAURIEL_LOG_FORMAT", "json")
    monkeypatch.setenv("ALPHAURIEL_LOG_LEVEL", "INFO")

    configure_logging()

    first_handlers = [
        handler
        for handler in root.handlers
        if getattr(handler, "_alphauriel_handler", False)
    ]

    configure_logging()

    second_handlers = [
        handler
        for handler in root.handlers
        if getattr(handler, "_alphauriel_handler", False)
    ]

    assert len(first_handlers) == 1
    assert second_handlers == first_handlers
