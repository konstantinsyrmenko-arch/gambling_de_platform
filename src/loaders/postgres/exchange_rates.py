from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from src.loaders.postgres.dictionary import (
    DictionarySpec,
    load_dictionary,
)
from src.validation.csv_reader import read_csv_rows
from src.validation.values import (
    date_in_period,
    ensure_unique,
    parse_date,
    parse_decimal,
    parse_required_string,
    resolve_period,
)

EXCHANGE_RATES_SPEC = DictionarySpec(
    schema="public",
    table="exchange_rates",
    columns=(
        ("rate_date", "DATE"),
        ("currency", "VARCHAR(3)"),
        ("rate_to_usd", "NUMERIC(18, 6)"),
    ),
    key_columns=("rate_date", "currency"),
    mutable_columns=("rate_to_usd",),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS public.exchange_rates
        (
            rate_date DATE NOT NULL,
            currency VARCHAR(3) NOT NULL,
            rate_to_usd NUMERIC(18, 6) NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT exchange_rates_pk
                PRIMARY KEY (rate_date, currency),
            CONSTRAINT exchange_rates_currency_length_chk
                CHECK (char_length(currency) = 3),
            CONSTRAINT exchange_rates_rate_positive_chk
                CHECK (rate_to_usd > 0)
        )
    """,
    migration_sql=(
        """
        ALTER TABLE public.exchange_rates
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
            NOT NULL DEFAULT CURRENT_TIMESTAMP
        """,
    ),
)


def prepare_exchange_rates(
    file_path: str | Path,
    period_from: str | date | None = None,
    period_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> list[tuple[date, str, Decimal]]:
    start, end = resolve_period(period_from, period_to, lookback_days)

    source_rows = read_csv_rows(
        file_path,
        expected_columns={"date", "currency", "rate_to_usd"},
    )
    rows = []
    seen_keys: set[object] = set()

    for line_number, source in enumerate(source_rows, start=2):
        rate_date = parse_date(
            source["date"],
            "date",
            line_number,
        )
        if not date_in_period(rate_date, start, end):
            continue

        currency = parse_required_string(
            source["currency"],
            "currency",
            line_number,
        ).upper()
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError(
                f"Currency must be a three-letter code at line {line_number}",
            )

        business_key = (rate_date, currency)
        ensure_unique(
            business_key,
            seen_keys,
            "(date, currency)",
            line_number,
        )
        rows.append(
            (
                rate_date,
                currency,
                parse_decimal(
                    source["rate_to_usd"],
                    "rate_to_usd",
                    line_number,
                    scale=6,
                    min_value=Decimal("0.000001"),
                ),
            )
        )

    if not rows:
        raise ValueError(
            f"No exchange rates found for period {start} — {end}",
        )
    return rows


def load_exchange_rates(
    file_path: str | Path,
    period_from: str | date | None = None,
    period_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> dict[str, object]:
    return load_dictionary(
        spec=EXCHANGE_RATES_SPEC,
        rows=prepare_exchange_rates(
            file_path,
            period_from,
            period_to,
            lookback_days,
        ),
    ).as_dict()
