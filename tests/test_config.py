import pytest


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("ALPHAURIEL_POLL_SECONDS", "0"),
        ("ALPHAURIEL_POLL_SECONDS", "-1"),
        ("ALPHAURIEL_POLL_SECONDS", "invalid"),
        ("ALPHAURIEL_MAX_ATTEMPTS", "0"),
        ("ALPHAURIEL_MAX_ATTEMPTS", "-2"),
        ("ALPHAURIEL_LEASE_SECONDS", "invalid"),
        ("ALPHAURIEL_WORKER_HEARTBEAT_SECONDS", "0"),
        ("ALPHAURIEL_WORKER_STALE_SECONDS", "-1"),
        ("ALPHAURIEL_RETRY_BASE_SECONDS", "0"),
        ("ALPHAURIEL_RETRY_MAX_SECONDS", "-5"),
    ],
)
def test_invalid_integer_configuration_is_rejected(
    monkeypatch,
    variable,
    value,
):
    import importlib
    import alphauriel_pascal.config as config

    monkeypatch.setenv(variable, value)

    with pytest.raises(ValueError, match=variable):
        importlib.reload(config)


def test_lease_minimum_is_preserved(monkeypatch):
    import importlib
    import alphauriel_pascal.config as config

    monkeypatch.setenv("ALPHAURIEL_LEASE_SECONDS", "10")

    importlib.reload(config)

    assert config.LEASE_SECONDS == 30


def test_worker_stale_threshold_is_at_least_twice_heartbeat(
    monkeypatch,
):
    import importlib
    import alphauriel_pascal.config as config

    monkeypatch.setenv("ALPHAURIEL_WORKER_HEARTBEAT_SECONDS", "15")
    monkeypatch.setenv("ALPHAURIEL_WORKER_STALE_SECONDS", "20")

    importlib.reload(config)

    assert config.WORKER_STALE_SECONDS == 30
