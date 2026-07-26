from __future__ import annotations

import pytest
from src.visualization.report import build_report

pytestmark = pytest.mark.integration


def test_build_report_from_monthly_summary(tmp_path):
    destination = build_report(
        output_path=tmp_path / "report.html",
        date_from="2023-01-01",
        date_to="2023-01-31",
    )

    content = destination.read_text(encoding="utf-8")
    assert destination.stat().st_size > 1_000_000
    assert "2023-01-01 — 2023-01-31" in content
    assert "Monthly deposits, withdrawals and bets, USD" in content
    assert "Financial distribution by country, USD" in content
