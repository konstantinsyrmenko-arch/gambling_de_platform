from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from src.config import POSTGRES_CONN_ID

if TYPE_CHECKING:
    from psycopg import Connection as PsycopgConnection


@contextmanager
def postgres_connection(
    connection_id: str = POSTGRES_CONN_ID,
) -> Iterator[PsycopgConnection[Any]]:
    import psycopg
    from airflow.sdk import Connection

    airflow_connection = Connection.get(connection_id)
    if not airflow_connection.host:
        raise ValueError(
            f"Airflow Connection {connection_id!r} has no host",
        )
    if not airflow_connection.login:
        raise ValueError(
            f"Airflow Connection {connection_id!r} has no login",
        )

    connection = psycopg.connect(
        host=airflow_connection.host,
        port=airflow_connection.port or 5432,
        dbname=airflow_connection.schema or "postgres",
        user=airflow_connection.login,
        password=airflow_connection.password or "",
        connect_timeout=10,
    )
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def postgres_transaction(
    connection_id: str = POSTGRES_CONN_ID,
) -> Iterator[PsycopgConnection[Any]]:
    with postgres_connection(connection_id) as connection:
        try:
            yield connection
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()
