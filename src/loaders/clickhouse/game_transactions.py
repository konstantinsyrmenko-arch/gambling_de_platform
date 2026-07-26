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

GAME_TRANSACTIONS_SPEC = StatisticsSpec(
    database="analytics",
    table="game_transactions",
    columns=(
        "id",
        "player_id",
        "game_date",
        "amount",
        "currency",
        "provider_id",
        "game_id",
        "loaded_at",
    ),
    id_column="id",
    event_date_column="game_date",
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
            local_columns=("game_id",),
            reference_table="postgres_source.games",
            reference_columns=("id",),
        ),
        ReferenceCheck(
            local_columns=("game_date", "currency"),
            reference_table="postgres_source.exchange_rates",
            reference_columns=("rate_date", "currency"),
        ),
    ),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS analytics.game_transactions
        (
            id UInt64,
            player_id UInt64,
            game_date Date,
            amount Decimal(18, 2),
            currency LowCardinality(String),
            provider_id UInt32,
            game_id UInt32,
            loaded_at DateTime('UTC')
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(game_date)
        ORDER BY (
            game_date,
            player_id,
            provider_id,
            game_id,
            currency,
            id
        )
    """,
)


def prepare_game_transactions(
    file_path: str | Path,
    period_from: str | date | None = None,
    period_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> list[tuple[int, int, date, Decimal, str, int, int, datetime]]:
    start, end = resolve_period(period_from, period_to, lookback_days)
    source_rows = read_csv_rows(
        file_path,
        expected_columns={
            "id",
            "player_id",
            "game_date",
            "amount",
            "currency",
            "provider_id",
            "game_id",
        },
    )

    rows = []
    seen_ids: set[object] = set()
    loaded_at = utc_now()
    for line_number, source in enumerate(source_rows, start=2):
        transaction_id = parse_int(
            source["id"],
            "id",
            line_number,
            min_value=1,
        )
        ensure_unique(transaction_id, seen_ids, "id", line_number)
        game_date = parse_date(
            source["game_date"],
            "game_date",
            line_number,
        )
        if not date_in_period(game_date, start, end):
            continue

        rows.append(
            (
                transaction_id,
                parse_int(
                    source["player_id"],
                    "player_id",
                    line_number,
                    min_value=1,
                ),
                game_date,
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
                parse_int(
                    source["provider_id"],
                    "provider_id",
                    line_number,
                    min_value=1,
                    max_value=4294967295,
                ),
                parse_int(
                    source["game_id"],
                    "game_id",
                    line_number,
                    min_value=1,
                    max_value=4294967295,
                ),
                loaded_at,
            )
        )

    if not rows:
        raise ValueError(
            f"No game transactions found for period {start} — {end}",
        )
    return rows


def load_game_transactions(
    file_path: str | Path,
    period_from: str | date | None = None,
    period_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> dict[str, object]:
    return load_monthly_partitions(
        spec=GAME_TRANSACTIONS_SPEC,
        rows=prepare_game_transactions(
            file_path,
            period_from,
            period_to,
            lookback_days,
        ),
    ).as_dict()
