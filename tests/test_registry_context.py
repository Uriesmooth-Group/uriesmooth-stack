import json

import alphauriel_pascal.registry as registry


def create_test_app(tmp_path, monkeypatch, source):
    app_dir = tmp_path / "test_app"
    app_dir.mkdir()

    (app_dir / "app.json").write_text(
        json.dumps({
            "name": "registry-test",
            "description": "Registry compatibility test",
        }),
        encoding="utf-8",
    )

    (app_dir / "app.py").write_text(
        source,
        encoding="utf-8",
    )

    monkeypatch.setattr(registry, "APPS_DIR", tmp_path)


def test_legacy_app_receives_payload_only(tmp_path, monkeypatch):
    create_test_app(
        tmp_path,
        monkeypatch,
        "def run(payload):\n"
        "    return {'message': payload['message']}\n",
    )

    result = registry.run_app(
        "registry-test",
        {"message": "legacy"},
        context={"job_id": "job-1"},
    )

    assert result == {"message": "legacy"}


def test_keyword_context_app_receives_context(tmp_path, monkeypatch):
    create_test_app(
        tmp_path,
        monkeypatch,
        "def run(payload, context=None):\n"
        "    return {'job_id': context['job_id']}\n",
    )

    result = registry.run_app(
        "registry-test",
        {},
        context={"job_id": "job-2"},
    )

    assert result == {"job_id": "job-2"}


def test_kwargs_app_receives_context(tmp_path, monkeypatch):
    create_test_app(
        tmp_path,
        monkeypatch,
        "def run(payload, **kwargs):\n"
        "    return {'job_id': kwargs['context']['job_id']}\n",
    )

    result = registry.run_app(
        "registry-test",
        {},
        context={"job_id": "job-3"},
    )

    assert result == {"job_id": "job-3"}


def test_positional_only_context_app_receives_context(
    tmp_path, monkeypatch
):
    create_test_app(
        tmp_path,
        monkeypatch,
        "def run(payload, context, /):\n"
        "    return {'job_id': context['job_id']}\n",
    )

    result = registry.run_app(
        "registry-test",
        {},
        context={"job_id": "job-4"},
    )

    assert result == {"job_id": "job-4"}
