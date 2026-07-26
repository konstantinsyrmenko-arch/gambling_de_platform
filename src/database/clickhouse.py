from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from src.config import CLICKHOUSE_CONN_ID

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client


def _parse_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Cannot parse boolean value: {value!r}")


@contextmanager
def clickhouse_client(
    connection_id: str = CLICKHOUSE_CONN_ID,
) -> Iterator[Client]:
    import clickhouse_connect
    from airflow.sdk import Connection

    airflow_connection = Connection.get(connection_id)
    if not airflow_connection.host:
        raise ValueError(
            f"Airflow Connection {connection_id!r} has no host",
        )

    extra = airflow_connection.extra_dejson or {}
    secure = _parse_bool(extra.get("secure"), default=False)
    verify = _parse_bool(extra.get("verify"), default=True)

    client = clickhouse_connect.get_client(
        host=airflow_connection.host,
        port=airflow_connection.port or (8443 if secure else 8123),
        username=airflow_connection.login or "default",
        password=airflow_connection.password or "",
        database=airflow_connection.schema or "default",
        secure=secure,
        verify=verify,
        connect_timeout=int(extra.get("connect_timeout", 10)),
        send_receive_timeout=int(
            extra.get("send_receive_timeout", 300),
        ),
    )
    try:
        yield client
    finally:
        client.close()
