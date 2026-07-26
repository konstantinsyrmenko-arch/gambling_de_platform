"""Transactional staging-based loader for PostgreSQL dictionaries."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from psycopg import sql

from src.config import POSTGRES_CONN_ID
from src.database.postgres import postgres_transaction
from src.validation.values import utc_now


@dataclass(frozen=True, slots=True)
class DictionaryLoadResult:
    table: str
    source_rows: int
    inserted_rows: int
    updated_rows: int
    unchanged_rows: int
    loaded_at: datetime

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["loaded_at"] = self.loaded_at.isoformat()
        return result


@dataclass(frozen=True, slots=True)
class DictionarySpec:
    schema: str
    table: str
    columns: tuple[tuple[str, str], ...]
    key_columns: tuple[str, ...]
    mutable_columns: tuple[str, ...]
    create_table_sql: str
    migration_sql: tuple[str, ...] = ()

    @property
    def full_table_name(self) -> str:
        return f"{self.schema}.{self.table}"


def load_dictionary(
    *,
    spec: DictionarySpec,
    rows: Sequence[tuple[Any, ...]],
    loaded_at: datetime | None = None,
    connection_id: str = POSTGRES_CONN_ID,
) -> DictionaryLoadResult:
    if not rows:
        raise ValueError("Dictionary source contains no rows")
    if any(len(row) != len(spec.columns) for row in rows):
        raise ValueError("Dictionary row does not match its data contract")

    load_timestamp = loaded_at or utc_now()
    staging_table = f"{spec.table}_load_staging"

    with (
        postgres_transaction(connection_id) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(spec.create_table_sql)
        for statement in spec.migration_sql:
            cursor.execute(statement)

        _create_staging_table(cursor, spec, staging_table)
        _insert_staging_rows(cursor, spec, staging_table, rows)

        inserted_rows, updated_rows = _classify_changes(
            cursor,
            spec,
            staging_table,
        )
        unchanged_rows = len(rows) - inserted_rows - updated_rows

        _upsert_changed_rows(
            cursor,
            spec,
            staging_table,
            load_timestamp,
        )
    return DictionaryLoadResult(
        table=spec.full_table_name,
        source_rows=len(rows),
        inserted_rows=inserted_rows,
        updated_rows=updated_rows,
        unchanged_rows=unchanged_rows,
        loaded_at=load_timestamp,
    )


def _create_staging_table(cursor, spec: DictionarySpec, table: str) -> None:
    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} {}").format(
            sql.Identifier(name),
            sql.SQL(sql_type),
        )
        for name, sql_type in spec.columns
    )
    cursor.execute(
        sql.SQL(
            "CREATE TEMP TABLE {} ({}) ON COMMIT DROP",
        ).format(
            sql.Identifier(table),
            column_definitions,
        )
    )


def _insert_staging_rows(
    cursor,
    spec: DictionarySpec,
    table: str,
    rows: Sequence[tuple[Any, ...]],
) -> None:
    columns = sql.SQL(", ").join(sql.Identifier(name) for name, _ in spec.columns)
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in spec.columns)
    cursor.executemany(
        sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            columns,
            placeholders,
        ),
        rows,
    )


def _classify_changes(
    cursor,
    spec: DictionarySpec,
    staging_table: str,
) -> tuple[int, int]:
    join_condition = _equality_condition(
        sql.Identifier("target"),
        sql.Identifier("staging"),
        spec.key_columns,
    )
    changed_condition = _distinct_condition(
        sql.Identifier("target"),
        sql.Identifier("staging"),
        spec.mutable_columns,
    )
    first_key = sql.Identifier(spec.key_columns[0])

    cursor.execute(
        sql.SQL(
            """
            SELECT
                count(*) FILTER (WHERE target.{} IS NULL),
                count(*) FILTER (
                    WHERE target.{} IS NOT NULL AND ({})
                )
            FROM {} AS staging
            LEFT JOIN {}.{} AS target ON {}
            """
        ).format(
            first_key,
            first_key,
            changed_condition,
            sql.Identifier(staging_table),
            sql.Identifier(spec.schema),
            sql.Identifier(spec.table),
            join_condition,
        )
    )
    inserted, updated = cursor.fetchone()
    return int(inserted), int(updated)


def _upsert_changed_rows(
    cursor,
    spec: DictionarySpec,
    staging_table: str,
    loaded_at: datetime,
) -> None:
    source_columns = [name for name, _ in spec.columns]
    insert_columns = (*source_columns, "loaded_at", "updated_at")
    selected_columns = sql.SQL(", ").join(
        [
            *(sql.SQL("staging.{}").format(sql.Identifier(column)) for column in source_columns),
            sql.Placeholder(),
            sql.Placeholder(),
        ]
    )
    assignments = sql.SQL(", ").join(
        [
            *(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in spec.mutable_columns
            ),
            sql.SQL("updated_at = EXCLUDED.updated_at"),
        ]
    )
    changed_condition = sql.SQL(" OR ").join(
        sql.SQL("{}.{} IS DISTINCT FROM EXCLUDED.{}").format(
            sql.Identifier(spec.table),
            sql.Identifier(column),
            sql.Identifier(column),
        )
        for column in spec.mutable_columns
    )

    cursor.execute(
        sql.SQL(
            """
            INSERT INTO {}.{} ({})
            SELECT {}
            FROM {} AS staging
            ON CONFLICT ({}) DO UPDATE
            SET {}
            WHERE {}
            """
        ).format(
            sql.Identifier(spec.schema),
            sql.Identifier(spec.table),
            sql.SQL(", ").join(sql.Identifier(column) for column in insert_columns),
            selected_columns,
            sql.Identifier(staging_table),
            sql.SQL(", ").join(sql.Identifier(column) for column in spec.key_columns),
            assignments,
            changed_condition,
        ),
        (loaded_at, loaded_at),
    )


def _equality_condition(left, right, columns: tuple[str, ...]):
    return sql.SQL(" AND ").join(
        sql.SQL("{}.{} = {}.{}").format(
            left,
            sql.Identifier(column),
            right,
            sql.Identifier(column),
        )
        for column in columns
    )


def _distinct_condition(left, right, columns: tuple[str, ...]):
    return sql.SQL(" OR ").join(
        sql.SQL("{}.{} IS DISTINCT FROM {}.{}").format(
            left,
            sql.Identifier(column),
            right,
            sql.Identifier(column),
        )
        for column in columns
    )
