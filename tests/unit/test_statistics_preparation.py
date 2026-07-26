from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from src.loaders.clickhouse.monthly_partitions import (
    StatisticsSpec,
    group_rows_by_month,
)


def test_group_rows_by_month_and_reject_duplicate_ids():
    spec = StatisticsSpec(
        database="analytics",
        table="events",
        columns=("id", "event_date", "amount", "loaded_at"),
        id_column="id",
        event_date_column="event_date",
        create_table_sql="SELECT 1",
    )
    now = datetime.now(UTC)
    rows = (
        (1, date(2026, 1, 1), Decimal("1.00"), now),
        (2, date(2026, 2, 1), Decimal("2.00"), now),
    )

    assert tuple(group_rows_by_month(spec, rows)) == (202601, 202602)

    with pytest.raises(ValueError, match="Duplicate statistics id"):
        group_rows_by_month(spec, (rows[0], rows[0]))
