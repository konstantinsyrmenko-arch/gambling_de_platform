from __future__ import annotations

from datetime import date

import pytest
from src.validation.csv_reader import read_csv_rows
from src.validation.errors import DataValidationError
from src.validation.values import ensure_unique, resolve_period, validate_period


def test_read_csv_rows_rejects_invalid_schema(tmp_path):
    source = tmp_path / "invalid.csv"
    source.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(DataValidationError, match="schema does not match"):
        read_csv_rows(source, {"id", "name"})


def test_period_and_unique_key_validation():
    with pytest.raises(DataValidationError):
        validate_period(date(2026, 2, 1), date(2026, 1, 1))

    seen = set()
    ensure_unique(1, seen, "id", 2)
    with pytest.raises(DataValidationError, match="Duplicate key"):
        ensure_unique(1, seen, "id", 3)


def test_lookback_period_is_inclusive_and_cannot_mix_with_dates():
    start, end = resolve_period(
        lookback_days=7,
        current_date=date(2026, 7, 26),
    )
    assert (start, end) == (
        date(2026, 7, 20),
        date(2026, 7, 26),
    )
    assert resolve_period(
        lookback_days=0,
        current_date=date(2026, 7, 26),
    ) == (None, None)

    with pytest.raises(DataValidationError, match="cannot be combined"):
        resolve_period(
            "2026-07-01",
            None,
            7,
            current_date=date(2026, 7, 26),
        )
