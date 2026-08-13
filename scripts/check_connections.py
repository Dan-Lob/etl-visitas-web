import os

import paramiko
import pymysql


def check_mysql() -> None:
    connection = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ["MYSQL_PORT"]),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

        if result != (1,):
            raise RuntimeError("Unexpected MySQL response")

        print("MySQL connection: OK")

    finally:
        connection.close()


def check_sftp() -> None:
    transport = paramiko.Transport(
        (
            os.environ["SFTP_HOST"],
            int(os.environ["SFTP_PORT"]),
        )
    )

    try:
        transport.connect(
            username=os.environ["SFTP_USER"],
            password=os.environ["SFTP_PASSWORD"],
        )

        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            files = sftp.listdir(
                os.environ["SFTP_REMOTE_PATH"]
            )

            print("SFTP connection: OK")
            print(f"Remote files: {files}")

        finally:
            sftp.close()

    finally:
        transport.close()


if __name__ == "__main__":
    check_mysql()
    check_sftp()