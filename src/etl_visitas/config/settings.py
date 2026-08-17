import os
from dataclasses import dataclass

from etl_visitas.config.secret_provider import (
    SecretProvider,
    create_secret_provider,
)


@dataclass(frozen=True)
class SFTPSettings:
    host: str
    port: int
    username: str
    password: str
    remote_path: str


@dataclass(frozen=True)
class MySQLSettings:
    host: str
    port: int
    database: str
    username: str
    password: str


@dataclass(frozen=True)
class StorageSettings:
    staging_path: str
    backup_path: str

@dataclass(frozen=True)
class AirflowSettings:
    timezone: str
    schedule: str

@dataclass(frozen=True)
class AppSettings:
    environment: str
    sftp: SFTPSettings
    mysql: MySQLSettings
    storage: StorageSettings
    airflow: AirflowSettings



def _load_secret(
    provider: SecretProvider,
    env_name: str,
    secret_name_env: str,
) -> str:

    provider_type = os.getenv(
        "ETL_SECRET_PROVIDER",
        "env",
    ).lower()

    if provider_type == "env":
        return provider.get_secret(
            env_name
        )

    secret_name = os.getenv(
        secret_name_env
    )

    if not secret_name:
        raise RuntimeError(
            "Secret name configuration "
            "was not found. "
            f"variable={secret_name_env}"
        )

    return provider.get_secret(
        secret_name
    )


def load_settings() -> AppSettings:

    secret_provider = (
        create_secret_provider()
    )

    return AppSettings(

        environment=os.getenv(
            "ETL_ENV",
            "dev",
        ),

        sftp=SFTPSettings(
            host=os.environ[
                "SFTP_HOST"
            ],
            port=int(
                os.environ[
                    "SFTP_PORT"
                ]
            ),
            username=_load_secret(
                provider=secret_provider,
                env_name="SFTP_USER",
                secret_name_env=(
                    "GCP_SECRET_SFTP_USER"
                ),
            ),
            password=_load_secret(
                provider=secret_provider,
                env_name="SFTP_PASSWORD",
                secret_name_env=(
                    "GCP_SECRET_SFTP_PASSWORD"
                ),
            ),
            remote_path=os.environ[
                "SFTP_REMOTE_PATH"
            ],
        ),

        mysql=MySQLSettings(
            host=os.environ[
                "MYSQL_HOST"
            ],
            port=int(
                os.environ[
                    "MYSQL_PORT"
                ]
            ),
            database=os.environ[
                "MYSQL_DATABASE"
            ],
            username=_load_secret(
                provider=secret_provider,
                env_name="MYSQL_USER",
                secret_name_env=(
                    "GCP_SECRET_MYSQL_USER"
                ),
            ),
            password=_load_secret(
                provider=secret_provider,
                env_name="MYSQL_PASSWORD",
                secret_name_env=(
                    "GCP_SECRET_MYSQL_PASSWORD"
                ),
            ),
        ),

        storage=StorageSettings(
            staging_path=os.environ[
                "ETL_STAGING_PATH"
            ],
            backup_path=os.environ[
                "ETL_BACKUP_PATH"
            ],
        ),
            airflow=load_airflow_settings(),        
    )

def load_airflow_settings() -> AirflowSettings:
    return AirflowSettings(
        timezone=os.getenv(
            "ETL_TIMEZONE",
            "America/Mexico_City",
        ),
        schedule=os.getenv(
            "ETL_SCHEDULE",
            "0 6 * * *",
        ),
    )

