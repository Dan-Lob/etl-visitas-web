from contextlib import contextmanager
from typing import Iterator

import pymysql
from pymysql.connections import Connection

from etl_visitas.config.settings import MySQLSettings


class MySQLConnectionFactory:

    def __init__(
        self,
        settings: MySQLSettings,
    ):
        self._settings = settings

    def create_connection(self) -> Connection:
        return pymysql.connect(
            host=self._settings.host,
            port=self._settings.port,
            user=self._settings.username,
            password=self._settings.password,
            database=self._settings.database,
            charset="utf8mb4",
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )

    @contextmanager
    def connection(
        self,
    ) -> Iterator[Connection]:

        connection = self.create_connection()

        try:
            yield connection
        finally:
            connection.close()