import json

import pytest

import alphauriel_pascal.registry as registry


def create_app(root, folder_name, app_name, description="Test app"):
    folder = root / folder_name
    folder.mkdir(parents=True)

    (folder / "app.json").write_text(
        json.dumps({
            "name": app_name,
            "description": description,
        }),
        encoding="utf-8",
    )
    (folder / "app.py").write_text(
        "def run(payload): return {'ok': True}\n",
        encoding="utf-8",
    )
    return folder


def test_registry_rejects_invalid_folder_names(tmp_path, monkeypatch):
    create_app(tmp_path, "invalid folder", "valid-name")
    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    assert registry.list_apps() == []


@pytest.mark.parametrize(
    "name",
    [
        "",
        " ",
        "../outside",
        "nested/name",
        "name with spaces",
        ".",
        "..",
        "a" * 65,
        123,
        None,
    ],
)
def test_get_app_rejects_invalid_names(tmp_path, monkeypatch, name):
    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    with pytest.raises(ValueError):
        registry.get_app(name)


def test_registry_rejects_duplicate_names_case_insensitively(
    tmp_path, monkeypatch
):
    create_app(tmp_path, "first", "Registry-Test")
    create_app(tmp_path, "second", "registry-test")
    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    assert registry.list_apps() == []

    with pytest.raises(ValueError):
        registry.get_app("Registry-Test")


def test_registry_rejects_non_string_description(tmp_path, monkeypatch):
    create_app(
        tmp_path,
        "bad-description",
        "bad-description",
        description=123,
    )
    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    assert registry.list_apps() == []


def test_registry_rejects_symlinked_app_directory(tmp_path, monkeypatch):
    outside = tmp_path / "outside"
    outside.mkdir()
    create_app(outside, "real-app", "real-app")

    apps = tmp_path / "apps"
    apps.mkdir()
    (apps / "linked-app").symlink_to(
        outside / "real-app",
        target_is_directory=True,
    )

    monkeypatch.setattr(registry, "APPS_DIR", apps)

    assert registry.list_apps() == []


def test_registry_rejects_symlinked_manifest(tmp_path, monkeypatch):
    folder = create_app(tmp_path, "linked-manifest", "linked-manifest")
    outside_manifest = tmp_path / "outside.json"
    outside_manifest.write_text(
        json.dumps({"name": "linked-manifest"}),
        encoding="utf-8",
    )

    (folder / "app.json").unlink()
    (folder / "app.json").symlink_to(outside_manifest)

    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    assert registry.list_apps() == []


def test_registry_rejects_symlinked_app_file(tmp_path, monkeypatch):
    folder = create_app(tmp_path, "linked-code", "linked-code")
    outside_code = tmp_path / "outside.py"
    outside_code.write_text(
        "def run(payload): return payload\n",
        encoding="utf-8",
    )

    (folder / "app.py").unlink()
    (folder / "app.py").symlink_to(outside_code)

    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    assert registry.list_apps() == []


def test_registry_returns_resolved_app_directory(tmp_path, monkeypatch):
    create_app(tmp_path, "valid-app", "valid-app")
    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)

    app = registry.get_app("valid-app")

    assert app["path"] == (tmp_path / "valid-app").resolve()
    assert registry.run_app("valid-app", {}) == {"ok": True}


def test_missing_apps_directory_returns_empty_list(tmp_path, monkeypatch):
    monkeypatch.setattr(
        registry,
        "APPS_DIR",
        tmp_path / "missing",
    )

    assert registry.list_apps() == []
