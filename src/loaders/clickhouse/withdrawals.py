from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from src.config import SUPPORTED_CURRENCIES
from src.loaders.clickhouse.monthly_partitions import (
    ReferenceCheck,
    StatisticsSpec,
    load_monthly_partitions,
)
from src.validation.csv_reader import read_csv_rows
from src.validation.values import (
    date_in_period,
    ensure_unique,
    parse_choice,
    parse_date,
    parse_decimal,
    parse_int,
    resolve_period,
    utc_now,
)

WITHDRAWALS_SPEC = StatisticsSpec(
    database="analytics",
    table="withdrawals",
    columns=(
        "id",
        "player_id",
        "withdrawal_date",
        "provider_id",
        "amount",
        "currency",
        "loaded_at",
    ),
    id_column="id",
    event_date_column="withdrawal_date",
    reference_checks=(
        ReferenceCheck(
            local_columns=("player_id",),
            reference_table="postgres_source.players",
            reference_columns=("id",),
        ),
        ReferenceCheck(
            local_columns=("provider_id",),
            reference_table="postgres_source.providers",
            reference_columns=("id",),
        ),
        ReferenceCheck(
            local_columns=("withdrawal_date", "currency"),
            reference_table="postgres_source.exchange_rates",
            reference_columns=("rate_date", "currency"),
        ),
    ),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS analytics.withdrawals
        (
            id UInt64,
            player_id UInt64,
            withdrawal_date Date,
            provider_id UInt32,
            amount Decimal(18, 2),
            currency LowCardinality(String),
            loaded_at DateTime('UTC')
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(withdrawal_date)
        ORDER BY (withdrawal_date, player_id, currency, id)
    """,
)


def prepare_withdrawals(
    file_path: str | Path,
    period_from: str | date | None = None,
    period_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> list[tuple[int, int, date, int, Decimal, str, datetime]]:
    start, end = resolve_period(period_from, period_to, lookback_days)
    source_rows = read_csv_rows(
        file_path,
        expected_columns={
            "id",
            "player_id",
            "withdrawal_date",
            "provider_id",
            "amount",
            "currency",
        },
    )

    rows = []
    seen_ids: set[object] = set()
    loaded_at = utc_now()
    for line_number, source in enumerate(source_rows, start=2):
        withdrawal_id = parse_int(
            source["id"],
            "id",
            line_number,
            min_value=1,
        )
        ensure_unique(withdrawal_id, seen_ids, "id", line_number)
        withdrawal_date = parse_date(
            source["withdrawal_date"],
            "withdrawal_date",
            line_number,
        )
        if not date_in_period(withdrawal_date, start, end):
            continue

        rows.append(
            (
                withdrawal_id,
                parse_int(
                    source["player_id"],
                    "player_id",
                    line_number,
                    min_value=1,
                ),
                withdrawal_date,
                parse_int(
                    source["provider_id"],
                    "provider_id",
                    line_number,
                    min_value=1,
                    max_value=4294967295,
                ),
                parse_decimal(
                    source["amount"],
                    "amount",
                    line_number,
                    scale=2,
                    min_value=Decimal("0.01"),
                ),
                parse_choice(
                    source["currency"],
                    "currency",
                    line_number,
                    allowed_values=SUPPORTED_CURRENCIES,
                    uppercase=True,
                ),
                loaded_at,
            )
        )

    if not rows:
        raise ValueError(f"No withdrawals found for period {start} — {end}")
    return rows


def load_withdrawals(
    file_path: str | Path,
    period_from: str | date | None = None,
    period_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> dict[str, object]:
    return load_monthly_partitions(
        spec=WITHDRAWALS_SPEC,
        rows=prepare_withdrawals(
            file_path,
            period_from,
            period_to,
            lookback_days,
        ),
    ).as_dict()
