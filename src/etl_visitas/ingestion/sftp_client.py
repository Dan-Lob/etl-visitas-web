from datetime import datetime, timezone
from pathlib import Path
import posixpath

import paramiko

from etl_visitas.config.settings import SFTPSettings
from etl_visitas.models.file_models import RemoteFileMetadata


class SFTPClient:
    def __init__(self, settings: SFTPSettings):
        self._settings = settings
        self._transport: paramiko.Transport | None = None
        self._client: paramiko.SFTPClient | None = None

    def connect(self) -> None:
        transport = paramiko.Transport(
            (
                self._settings.host,
                self._settings.port,
            )
        )

        transport.connect(
            username=self._settings.username,
            password=self._settings.password,
        )

        self._transport = transport
        self._client = paramiko.SFTPClient.from_transport(transport)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

        if self._transport is not None:
            self._transport.close()
            self._transport = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def list_files(self) -> list[RemoteFileMetadata]:
        client = self._require_client()

        files: list[RemoteFileMetadata] = []

        for item in client.listdir_attr(self._settings.remote_path):
            remote_path = posixpath.join(
                self._settings.remote_path,
                item.filename,
            )

            modified_at = datetime.fromtimestamp(
                item.st_mtime,
                tz=timezone.utc,
            )

            files.append(
                RemoteFileMetadata(
                    file_name=item.filename,
                    remote_path=remote_path,
                    size_bytes=item.st_size,
                    modified_at=modified_at,
                )
            )

        return files

    def download(
        self,
        remote_path: str,
        local_path: str,
    ) -> None:
        client = self._require_client()

        target = Path(local_path)
        target.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        client.get(
            remote_path,
            str(target),
        )

    def delete(self, remote_path: str) -> None:
        client = self._require_client()
        client.remove(remote_path)

    def _require_client(self) -> paramiko.SFTPClient:
        if self._client is None:
            raise RuntimeError(
                "SFTP client is not connected"
            )

        return self._client