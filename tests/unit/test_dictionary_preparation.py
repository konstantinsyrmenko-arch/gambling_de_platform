from __future__ import annotations

from src.loaders.postgres.exchange_rates import prepare_exchange_rates


def test_exchange_rates_period_is_inclusive(data_dir):
    rows = prepare_exchange_rates(
        data_dir / "currency_rates.csv",
        "2023-01-01",
        "2023-01-02",
    )

    assert len(rows) == 8
    assert {row[0].isoformat() for row in rows} == {
        "2023-01-01",
        "2023-01-02",
    }
