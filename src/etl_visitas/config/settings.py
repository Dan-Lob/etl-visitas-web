import os
from dataclasses import dataclass


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
class AppSettings:
    environment: str
    sftp: SFTPSettings
    mysql: MySQLSettings
    storage: StorageSettings


def load_settings() -> AppSettings:
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
            username=os.environ[
                "SFTP_USER"
            ],
            password=os.environ[
                "SFTP_PASSWORD"
            ],
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
            username=os.environ[
                "MYSQL_USER"
            ],
            password=os.environ[
                "MYSQL_PASSWORD"
            ],
        ),
        storage=StorageSettings(
            staging_path=os.environ[
                "ETL_STAGING_PATH"
            ],
            backup_path=os.environ[
                "ETL_BACKUP_PATH"
            ],
        ),
    )