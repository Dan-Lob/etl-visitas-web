import pytest

from etl_visitas.config.secret_provider import (
    EnvironmentSecretProvider,
    create_secret_provider,
)


def test_environment_secret_provider(
    monkeypatch,
):

    monkeypatch.setenv(
        "TEST_SECRET",
        "secret-value",
    )

    provider = (
        EnvironmentSecretProvider()
    )

    result = provider.get_secret(
        "TEST_SECRET"
    )

    assert result == "secret-value"


def test_environment_secret_missing(
    monkeypatch,
):

    monkeypatch.delenv(
        "MISSING_SECRET",
        raising=False,
    )

    provider = (
        EnvironmentSecretProvider()
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "Required environment secret"
        ),
    ):
        provider.get_secret(
            "MISSING_SECRET"
        )


def test_factory_defaults_to_env(
    monkeypatch,
):

    monkeypatch.delenv(
        "ETL_SECRET_PROVIDER",
        raising=False,
    )

    provider = (
        create_secret_provider()
    )

    assert isinstance(
        provider,
        EnvironmentSecretProvider,
    )


def test_factory_env_provider(
    monkeypatch,
):

    monkeypatch.setenv(
        "ETL_SECRET_PROVIDER",
        "env",
    )

    provider = (
        create_secret_provider()
    )

    assert isinstance(
        provider,
        EnvironmentSecretProvider,
    )


def test_factory_rejects_unknown_provider(
    monkeypatch,
):

    monkeypatch.setenv(
        "ETL_SECRET_PROVIDER",
        "unknown",
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "Unsupported secret provider"
        ),
    ):
        create_secret_provider()