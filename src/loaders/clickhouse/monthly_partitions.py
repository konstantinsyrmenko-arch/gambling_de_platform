"""Safe monthly partition replacement for ClickHouse statistics."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any
from uuid import uuid4

from src.config import CLICKHOUSE_CONN_ID
from src.database.clickhouse import clickhouse_client
from src.validation.values import utc_now


@dataclass(frozen=True, slots=True)
class PartitionLoadResult:
    table: str
    source_rows: int
    published_rows: int
    replaced_partitions: tuple[str, ...]
    period_from: date
    period_to: date
    loaded_at: datetime

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["replaced_partitions"] = list(self.replaced_partitions)
        result["period_from"] = self.period_from.isoformat()
        result["period_to"] = self.period_to.isoformat()
        result["loaded_at"] = self.loaded_at.isoformat()
        return result


@dataclass(frozen=True, slots=True)
class ReferenceCheck:
    local_columns: tuple[str, ...]
    reference_table: str
    reference_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class StatisticsSpec:
    database: str
    table: str
    columns: tuple[str, ...]
    id_column: str
    event_date_column: str
    create_table_sql: str
    reference_checks: tuple[ReferenceCheck, ...] = ()

    @property
    def full_table_name(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def id_index(self) -> int:
        return self.columns.index(self.id_column)

    @property
    def event_date_index(self) -> int:
        return self.columns.index(self.event_date_column)

def load_monthly_partitions(
    *,
    spec: StatisticsSpec,
    rows: Sequence[tuple[Any, ...]],
    connection_id: str = CLICKHOUSE_CONN_ID,
    loaded_at: datetime | None = None,
) -> PartitionLoadResult:
    grouped_rows = group_rows_by_month(spec, rows)
    load_timestamp = loaded_at or utc_now()

    with clickhouse_client(connection_id) as client:
        client.command(spec.create_table_sql)
        _check_monthly_partitioning(client, spec)
        published_rows = 0
        for month, partition_rows in grouped_rows.items():
            published_rows += _replace_month(
                client,
                spec,
                month,
                partition_rows,
            )

    event_dates = [
        row[spec.event_date_index]
        for partition_rows in grouped_rows.values()
        for row in partition_rows
    ]
    return PartitionLoadResult(
        table=spec.full_table_name,
        source_rows=len(rows),
        published_rows=published_rows,
        replaced_partitions=tuple(str(month) for month in grouped_rows),
        period_from=min(event_dates),
        period_to=max(event_dates),
        loaded_at=load_timestamp,
    )


def _check_monthly_partitioning(client, spec: StatisticsSpec) -> None:
    metadata = client.query(
        """
        SELECT partition_key, engine
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        """,
        parameters={
            "database": spec.database,
            "table": spec.table,
        },
    ).result_rows
    if not metadata:
        raise RuntimeError(f"Table {spec.full_table_name} was not created")

    expected_key = f"toYYYYMM({spec.event_date_column})"
    actual_key = str(metadata[0][0]).replace(" ", "")
    if actual_key != expected_key:
        raise RuntimeError(
            f"{spec.full_table_name} must use monthly partitions",
        )
    if metadata[0][1] != "MergeTree":
        raise RuntimeError(
            f"{spec.full_table_name} must use MergeTree",
        )


def group_rows_by_month(
    spec: StatisticsSpec,
    rows: Sequence[tuple[Any, ...]],
) -> dict[int, tuple[tuple[Any, ...], ...]]:
    if not rows:
        raise ValueError("Statistics source contains no rows")

    grouped: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
    seen_ids = set()

    for row in rows:
        if len(row) != len(spec.columns):
            raise ValueError(
                "Statistics row does not match its data contract",
            )

        row_id = row[spec.id_index]
        if row_id in seen_ids:
            raise ValueError(f"Duplicate statistics id: {row_id!r}")
        seen_ids.add(row_id)

        event_date = row[spec.event_date_index]
        if isinstance(event_date, datetime):
            event_date = event_date.date()
        if not isinstance(event_date, date):
            raise ValueError(
                f"{spec.event_date_column} must be a date",
            )

        month = event_date.year * 100 + event_date.month
        grouped[month].append(tuple(row))

    return {month: tuple(partition_rows) for month, partition_rows in sorted(grouped.items())}


def _replace_month(
    client,
    spec: StatisticsSpec,
    month: int,
    rows: tuple[tuple[Any, ...], ...],
) -> int:
    suffix = uuid4().hex[:12]
    staging = f"{spec.database}.{spec.table}_staging_{suffix}"
    incoming_ids = [row[spec.id_index] for row in rows]

    client.command(f"CREATE TABLE {staging} AS {spec.full_table_name}")
    try:
        _assert_ids_do_not_move_between_months(
            client,
            spec,
            incoming_ids,
            month,
        )

        client.command(
            f"""
            INSERT INTO {staging}
            SELECT {", ".join(spec.columns)}
            FROM {spec.full_table_name}
            WHERE toYYYYMM({spec.event_date_column}) = {month}
              AND {spec.id_column} NOT IN
                  {{incoming_ids:Array(UInt64)}}
            """,
            parameters={"incoming_ids": incoming_ids},
        )
        client.insert(
            table=staging,
            data=list(rows),
            column_names=list(spec.columns),
        )

        retained_rows = _count_retained_rows(
            client,
            spec.full_table_name,
            spec.id_column,
            spec.event_date_column,
            incoming_ids,
            month,
        )
        expected_rows = retained_rows + len(rows)
        actual_rows = _count_partition_rows(
            client,
            staging,
            spec.event_date_column,
            month,
        )
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"Staging validation failed for partition {month}: "
                f"expected {expected_rows}, got {actual_rows}",
            )
        _validate_references(client, spec, staging)
        _replace_partition(
            client,
            spec.full_table_name,
            staging,
            month,
        )
        return expected_rows
    finally:
        client.command(f"DROP TABLE IF EXISTS {staging}")


def _assert_ids_do_not_move_between_months(
    client,
    spec: StatisticsSpec,
    incoming_ids: list[Any],
    month: int,
) -> None:
    result = client.query(
        f"""
        SELECT count()
        FROM {spec.full_table_name}
        WHERE {spec.id_column} IN {{incoming_ids:Array(UInt64)}}
          AND toYYYYMM({spec.event_date_column}) != {month}
        """,
        parameters={"incoming_ids": incoming_ids},
    )
    if int(result.result_rows[0][0]):
        raise ValueError(
            "An existing id cannot be moved to another month",
        )


def _count_retained_rows(
    client,
    table: str,
    id_column: str,
    event_date_column: str,
    incoming_ids: list[Any],
    month: int,
) -> int:
    result = client.query(
        f"""
        SELECT count()
        FROM {table}
        WHERE toYYYYMM({event_date_column}) = {month}
          AND {id_column} NOT IN {{incoming_ids:Array(UInt64)}}
        """,
        parameters={"incoming_ids": incoming_ids},
    )
    return int(result.result_rows[0][0])


def _count_partition_rows(
    client,
    table: str,
    event_date_column: str,
    month: int,
) -> int:
    result = client.query(
        f"""
        SELECT count()
        FROM {table}
        WHERE toYYYYMM({event_date_column}) = {month}
        """
    )
    return int(result.result_rows[0][0])


def _validate_references(
    client,
    spec: StatisticsSpec,
    table: str,
) -> None:
    for reference in spec.reference_checks:
        join_condition = " AND ".join(
            f"toString(source.{local}) = toString(reference.{external})"
            for local, external in zip(
                reference.local_columns,
                reference.reference_columns,
                strict=True,
            )
        )
        missing_rows = int(
            client.query(
                f"""
                SELECT count()
                FROM {table} AS source
                LEFT ANTI JOIN {reference.reference_table} AS reference
                    ON {join_condition}
                """
            ).result_rows[0][0]
        )
        if missing_rows:
            columns = ", ".join(reference.local_columns)
            raise RuntimeError(
                f"Reference validation failed for {table}.{columns}: "
                f"{missing_rows} rows are absent from "
                f"{reference.reference_table}",
            )


def _replace_partition(
    client,
    target_table: str,
    source_table: str,
    month: int,
) -> None:
    client.command(
        f"""
        ALTER TABLE {target_table}
        REPLACE PARTITION {month}
        FROM {source_table}
        """
    )
