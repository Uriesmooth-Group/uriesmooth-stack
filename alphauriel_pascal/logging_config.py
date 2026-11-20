import json
import logging
import os
import time
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            data["exception"] = self.formatException(
                record.exc_info
            )

        return json.dumps(data)


def configure_logging():
    level_name = os.environ.get(
        "ALPHAURIEL_LOG_LEVEL",
        "INFO",
    ).strip().upper()

    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    level = valid_levels.get(level_name)

    if level is None:
        raise ValueError(
            f"Invalid ALPHAURIEL_LOG_LEVEL: {level_name}"
        )

    log_format = os.environ.get(
        "ALPHAURIEL_LOG_FORMAT",
        "text",
    ).lower()

    if log_format not in {"text", "json"}:
        raise ValueError(
            f"Invalid ALPHAURIEL_LOG_FORMAT: {log_format}"
        )

    root = logging.getLogger()
    root.setLevel(level)

    handler = next(
        (
            item
            for item in root.handlers
            if getattr(item, "_alphauriel_handler", False)
        ),
        None,
    )

    if handler is None and not root.handlers:
        handler = logging.StreamHandler()
        handler._alphauriel_handler = True
        root.addHandler(handler)

    if handler is not None:
        if log_format == "json":
            handler.setFormatter(JsonFormatter())
        else:
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s "
                "%(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
            formatter.converter = time.gmtime
            handler.setFormatter(formatter)
