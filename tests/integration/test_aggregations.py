from __future__ import annotations

from decimal import Decimal

import pytest
from src.aggregations.build import build_monthly_summary
from src.database.clickhouse import clickhouse_client

pytestmark = pytest.mark.integration


def test_monthly_summary_is_repeatable_and_uses_usd_rates():
    first = build_monthly_summary()
    second = build_monthly_summary()
    assert first == second
    assert second["rows"] > 0

    with clickhouse_client() as client:
        summary_totals = client.query(
            """
            SELECT
                sum(deposits_usd),
                sum(withdrawals_usd),
                sum(bets_usd)
            FROM analytics.monthly_summary
            """
        ).result_rows[0]
        source_totals = client.query(
            """
            SELECT
                (
                    SELECT sum(
                        toDecimal128(deposits.amount, 6)
                        / toDecimal128(rates.rate_to_usd, 6)
                    )
                    FROM analytics.deposits AS deposits
                    INNER JOIN postgres_source.exchange_rates AS rates
                        ON rates.rate_date = deposits.deposit_date
                       AND rates.currency = deposits.currency
                ),
                (
                    SELECT sum(
                        toDecimal128(withdrawals.amount, 6)
                        / toDecimal128(rates.rate_to_usd, 6)
                    )
                    FROM analytics.withdrawals AS withdrawals
                    INNER JOIN postgres_source.exchange_rates AS rates
                        ON rates.rate_date = withdrawals.withdrawal_date
                       AND rates.currency = withdrawals.currency
                ),
                (
                    SELECT sum(
                        toDecimal128(games.amount, 6)
                        / toDecimal128(rates.rate_to_usd, 6)
                    )
                    FROM analytics.game_transactions AS games
                    INNER JOIN postgres_source.exchange_rates AS rates
                        ON rates.rate_date = games.game_date
                       AND rates.currency = games.currency
                )
            """
        ).result_rows[0]

    for summary_value, source_value in zip(
        summary_totals,
        source_totals,
        strict=True,
    ):
        source_value = Decimal(str(source_value))
        difference = abs(summary_value - source_value)
        assert difference <= abs(source_value) * Decimal("0.00005")
