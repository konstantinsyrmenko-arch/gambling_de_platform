from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from src.database.clickhouse import clickhouse_client
from src.loaders.clickhouse.monthly_partitions import (
    ReferenceCheck,
    StatisticsSpec,
    load_monthly_partitions,
)
from src.validation.values import utc_now

pytestmark = pytest.mark.integration

TABLE = "analytics._integration_monthly_statistics"
REFERENCE_TABLE = "analytics._integration_reference_ids"
SPEC = StatisticsSpec(
    database="analytics",
    table="_integration_monthly_statistics",
    columns=("id", "event_date", "amount", "loaded_at"),
    id_column="id",
    event_date_column="event_date",
    reference_checks=(
        ReferenceCheck(
            local_columns=("id",),
            reference_table=REFERENCE_TABLE,
            reference_columns=("id",),
        ),
    ),
    create_table_sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE}
        (
            id UInt64,
            event_date Date,
            amount Decimal(18, 2),
            loaded_at DateTime('UTC')
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, id)
    """,
)


def test_partial_month_merge_preserves_existing_rows():
    _drop_test_table()
    try:
        _create_test_table()
        first_timestamp = utc_now()
        load_monthly_partitions(
            spec=SPEC,
            rows=(
                (1, date(2026, 1, 1), Decimal("10.00"), first_timestamp),
                (2, date(2026, 1, 2), Decimal("20.00"), first_timestamp),
                (3, date(2026, 2, 1), Decimal("30.00"), first_timestamp),
            ),
        )
        second_timestamp = utc_now()
        result = load_monthly_partitions(
            spec=SPEC,
            rows=(
                (1, date(2026, 1, 1), Decimal("11.00"), second_timestamp),
                (4, date(2026, 1, 3), Decimal("40.00"), second_timestamp),
            ),
        )

        with clickhouse_client() as client:
            actual = client.query(
                f"""
                SELECT id, amount, toYYYYMM(event_date)
                FROM {TABLE}
                ORDER BY id
                """
            ).result_rows
        assert result.published_rows == 3
        assert actual == [
            (1, Decimal("11.00"), 202601),
            (2, Decimal("20.00"), 202601),
            (3, Decimal("30.00"), 202602),
            (4, Decimal("40.00"), 202601),
        ]
        with pytest.raises(
            RuntimeError,
            match="Reference validation failed",
        ):
            load_monthly_partitions(
                spec=SPEC,
                rows=(
                    (
                        999,
                        date(2026, 1, 4),
                        Decimal("99.00"),
                        utc_now(),
                    ),
                ),
            )

        with clickhouse_client() as client:
            production_rows = client.query(
                f"SELECT count() FROM {TABLE}",
            ).result_rows[0][0]
        assert production_rows == 4
    finally:
        _drop_test_table()


def _drop_test_table():
    with clickhouse_client() as client:
        client.command(f"DROP TABLE IF EXISTS {TABLE}")
        client.command(f"DROP TABLE IF EXISTS {REFERENCE_TABLE}")


def _create_test_table():
    with clickhouse_client() as client:
        client.command(
            f"""
            CREATE TABLE {REFERENCE_TABLE}
            (
                id UInt64
            )
            ENGINE = MergeTree
            ORDER BY id
            """
        )
        client.command(
            f"INSERT INTO {REFERENCE_TABLE} VALUES (1), (2), (3), (4)",
        )
        client.command(
            f"""
            CREATE TABLE {TABLE}
            (
                id UInt64,
                event_date Date,
                amount Decimal(18, 2),
                loaded_at DateTime('UTC')
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (event_date, id)
            """
        )
