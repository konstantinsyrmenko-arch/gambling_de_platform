"""Create the monthly ClickHouse summary used by reports."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.database.clickhouse import clickhouse_client

SQL_FILE = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "clickhouse"
    / "aggregations"
    / "monthly_summary_view.sql"
)


def build_monthly_summary() -> dict[str, object]:
    sql = SQL_FILE.read_text(encoding="utf-8").strip()

    with clickhouse_client() as client:
        client.command(sql)
        rows, period_from, period_to = client.query(
            """
            SELECT
                count(),
                min(summary_month),
                max(summary_month)
            FROM analytics.monthly_summary
            """
        ).result_rows[0]

    if not rows:
        raise RuntimeError("analytics.monthly_summary is empty")
    return {
        "rows": int(rows),
        "period_from": period_from.isoformat(),
        "period_to": period_to.isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Confirm rebuilding the monthly summary",
    )
    arguments = parser.parse_args()
    if not arguments.rebuild:
        parser.error("--rebuild is required")

    print(build_monthly_summary())


if __name__ == "__main__":
    main()
