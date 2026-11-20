import importlib.util
import inspect
import json
import re
from pathlib import Path

from .config import APPS_DIR


_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


def _safe_component(value):
    return (
        isinstance(value, str)
        and value == value.strip()
        and _SAFE_COMPONENT.fullmatch(value) is not None
    )


def _contained_path(path, root):
    try:
        resolved = path.resolve(strict=True)
        return resolved if resolved.is_relative_to(root) else None
    except (OSError, RuntimeError):
        return None


def _inspect_app_folder(folder, root):
    if (
        folder.is_symlink()
        or not folder.is_dir()
        or not _safe_component(folder.name)
    ):
        return None

    resolved_folder = _contained_path(folder, root)
    if resolved_folder is None or not resolved_folder.is_dir():
        return None

    manifest = folder / "app.json"
    app_file = folder / "app.py"

    if manifest.is_symlink() or app_file.is_symlink():
        return None

    resolved_manifest = _contained_path(manifest, root)
    resolved_app_file = _contained_path(app_file, root)

    if (
        resolved_manifest is None
        or resolved_app_file is None
        or not resolved_manifest.is_file()
        or not resolved_app_file.is_file()
    ):
        return None

    try:
        data = json.loads(resolved_manifest.read_text(encoding="utf-8"))
    except (ValueError, OSError, UnicodeError):
        return None

    if not isinstance(data, dict):
        return None

    name = data.get("name")
    description = data.get("description", "")

    if not _safe_component(name) or not isinstance(description, str):
        return None

    return {
        "name": name,
        "description": description,
        "path": resolved_folder,
    }


def list_apps():
    try:
        root = Path(APPS_DIR).resolve(strict=True)
        if not root.is_dir():
            return []
        folders = sorted(root.iterdir(), key=lambda item: item.name)
    except (OSError, RuntimeError):
        return []

    candidates = []

    for folder in folders:
        if folder.name.startswith("_"):
            continue

        app = _inspect_app_folder(folder, root)
        if app is not None:
            candidates.append(app)

    # Exclude every case-insensitive duplicate rather than selecting
    # whichever directory happens to be encountered first.
    counts = {}
    for app in candidates:
        key = app["name"].casefold()
        counts[key] = counts.get(key, 0) + 1

    return [
        app for app in candidates
        if counts[app["name"].casefold()] == 1
    ]


def get_app(name):
    if not _safe_component(name):
        raise ValueError(f"Invalid app name: {name!r}")

    for app in list_apps():
        if app["name"] == name:
            return app

    raise ValueError(f"App not found: {name}")


def run_app(name, payload=None, context=None):
    app = get_app(name)

    # Revalidate the application immediately before loading it.
    try:
        root = Path(APPS_DIR).resolve(strict=True)
        folder = _contained_path(app["path"], root)
        if folder is None or folder.is_symlink():
            raise RuntimeError(f"Unsafe app directory: {name}")

        module_path = folder / "app.py"
        if module_path.is_symlink():
            raise RuntimeError(f"Unsafe app file: {name}")

        module_path = _contained_path(module_path, root)
        if module_path is None or not module_path.is_file():
            raise RuntimeError(f"Cannot load app: {name}")
    except (OSError, RuntimeError) as exc:
        raise RuntimeError(f"Cannot safely load app: {name}") from exc

    spec = importlib.util.spec_from_file_location(
        f"uriesmooth_app_{name}",
        module_path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load app: {name}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    run = getattr(module, "run", None)
    if not callable(run):
        raise RuntimeError(f"App {name} must define run(payload)")

    if context is not None:
        try:
            parameters = inspect.signature(run).parameters
        except (TypeError, ValueError):
            parameters = {}

        context_parameter = parameters.get("context")

        if context_parameter is not None:
            if (
                context_parameter.kind
                == inspect.Parameter.POSITIONAL_ONLY
            ):
                return run(payload or {}, context)

            return run(payload or {}, context=context)

        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )

        if accepts_kwargs:
            return run(payload or {}, context=context)

    return run(payload or {})
