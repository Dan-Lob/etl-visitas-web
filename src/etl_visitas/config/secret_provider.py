from __future__ import annotations

import os
from abc import ABC, abstractmethod


class SecretProvider(ABC):

    @abstractmethod
    def get_secret(
        self,
        secret_name: str,
    ) -> str:
        pass

class EnvironmentSecretProvider(
    SecretProvider
):

    def get_secret(
        self,
        secret_name: str,
    ) -> str:

        value = os.getenv(
            secret_name
        )

        if value is None:
            raise RuntimeError(
                "Required environment "
                "secret was not found. "
                f"name={secret_name}"
            )

        return value

class GCPSecretManagerProvider(
    SecretProvider
):

    def __init__(
        self,
        project_id: str,
    ) -> None:

        try:
            from google.cloud import (
                secretmanager,
            )

        except ImportError as error:
            raise RuntimeError(
                "google-cloud-secret-manager "
                "is required when using "
                "the GCP secret provider"
            ) from error

        self._project_id = project_id

        self._client = (
            secretmanager
            .SecretManagerServiceClient()
        )

    def get_secret(
        self,
        secret_name: str,
    ) -> str:

        resource_name = (
            f"projects/"
            f"{self._project_id}/"
            f"secrets/"
            f"{secret_name}/"
            f"versions/latest"
        )

        response = (
            self._client
            .access_secret_version(
                request={
                    "name": (
                        resource_name
                    )
                }
            )
        )

        return (
            response
            .payload
            .data
            .decode("UTF-8")
        )

def create_secret_provider(
) -> SecretProvider:

    provider = os.getenv(
        "ETL_SECRET_PROVIDER",
        "env",
    ).lower()

    if provider == "env":
        return (
            EnvironmentSecretProvider()
        )

    if provider == "gcp":

        project_id = os.getenv(
            "GCP_PROJECT_ID"
        )

        if not project_id:
            raise RuntimeError(
                "GCP_PROJECT_ID is required "
                "when ETL_SECRET_PROVIDER=gcp"
            )

        return (
            GCPSecretManagerProvider(
                project_id=project_id
            )
        )

    raise RuntimeError(
        "Unsupported secret provider. "
        f"provider={provider}"
    )