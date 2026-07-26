"""Build the monthly gaming activity report."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
from plotly.io import to_html

from src.database.clickhouse import clickhouse_client
from src.validation.values import resolve_period


@dataclass(frozen=True)
class ReportPeriod:
    date_from: date
    date_to: date


def build_report(
    *,
    output_path: str | Path,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
    lookback_days: int | str | None = 0,
) -> Path:
    with clickhouse_client() as client:
        period = _resolve_period(
            client,
            date_from,
            date_to,
            lookback_days,
        )
        monthly, countries = _fetch_data(client, period)

    document = _render_report(period, monthly, countries)
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(document, encoding="utf-8")
    return destination


def _resolve_period(
    client,
    date_from: str | date | None,
    date_to: str | date | None,
    lookback_days: int | str | None,
) -> ReportPeriod:
    start, end = resolve_period(
        date_from,
        date_to,
        lookback_days,
    )

    available_from, available_to = client.query(
        """
        SELECT
            min(summary_month),
            addDays(addMonths(max(summary_month), 1), -1)
        FROM analytics.monthly_summary
        """
    ).result_rows[0]
    if available_from is None:
        raise RuntimeError("The monthly summary is empty")

    resolved_from = max(start or available_from, available_from)
    resolved_to = min(end or available_to, available_to)
    if resolved_from > resolved_to:
        raise ValueError("Requested period is outside available data")

    return ReportPeriod(resolved_from, resolved_to)


def _fetch_data(
    client,
    period: ReportPeriod,
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
    parameters = {
        "date_from": period.date_from,
        "date_to": period.date_to,
    }
    period_filter = """
        summary_month BETWEEN
            toStartOfMonth({date_from:Date})
            AND toStartOfMonth({date_to:Date})
    """
    monthly = client.query(
        f"""
        SELECT
            summary_month,
            sum(deposits_usd),
            sum(withdrawals_usd),
            sum(bets_usd)
        FROM analytics.monthly_summary
        WHERE {period_filter}
        GROUP BY summary_month
        ORDER BY summary_month
        """,
        parameters=parameters,
    ).result_rows
    countries = client.query(
        f"""
        SELECT
            country,
            sum(deposits_usd) AS deposits,
            sum(withdrawals_usd),
            sum(bets_usd)
        FROM analytics.monthly_summary
        WHERE {period_filter}
        GROUP BY country
        ORDER BY deposits DESC
        """,
        parameters=parameters,
    ).result_rows

    if not monthly or not countries:
        raise RuntimeError("No data found for the report period")
    return tuple(monthly), tuple(countries)


def _render_report(
    period: ReportPeriod,
    monthly: tuple[tuple[Any, ...], ...],
    countries: tuple[tuple[Any, ...], ...],
) -> str:
    figures = (
        _monthly_figure(monthly),
        _country_figure(countries),
    )
    charts = [
        to_html(
            figure,
            full_html=False,
            include_plotlyjs="inline" if index == 0 else False,
            config={"displaylogo": False, "responsive": True},
        )
        for index, figure in enumerate(figures)
    ]
    period_label = f"{period.date_from.isoformat()} — {period.date_to.isoformat()}"
    generated_at = (
        datetime.now()
        .astimezone()
        .isoformat(
            timespec="seconds",
        )
    )
    return _html_document(period_label, generated_at, charts)


def _monthly_figure(rows):
    figure = go.Figure()
    months = [row[0] for row in rows]
    for index, name, color in _SERIES:
        figure.add_trace(
            go.Scatter(
                x=months,
                y=[float(row[index]) for row in rows],
                name=name,
                mode="lines+markers",
                line={"color": color, "width": 2},
            )
        )
    return _style(
        figure,
        "Monthly deposits, withdrawals and bets, USD",
    )


def _country_figure(rows):
    figure = go.Figure()
    countries = [str(row[0]) for row in rows]
    for index, name, color in _SERIES:
        figure.add_trace(
            go.Bar(
                x=countries,
                y=[float(row[index]) for row in rows],
                name=name,
                marker_color=color,
            )
        )
    figure.update_layout(barmode="group")
    return _style(figure, "Financial distribution by country, USD")


_SERIES = (
    (1, "Deposits", "#22c55e"),
    (2, "Withdrawals", "#ef4444"),
    (3, "Bets", "#f59e0b"),
)


def _style(figure, title):
    figure.update_layout(
        title=title,
        template="plotly_dark",
        height=460,
        legend={"orientation": "h"},
        margin={"l": 60, "r": 25, "t": 70, "b": 55},
        yaxis_title="USD",
    )
    return figure


def _html_document(period: str, generated_at: str, charts: list[str]) -> str:
    content = "".join(f"<section>{chart}</section>" for chart in charts)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Gaming analytics report</title>
  <style>
    body {{
      margin: 0;
      background: #111827;
      color: #e5e7eb;
      font-family: system-ui, sans-serif;
    }}
    main {{ max-width: 1400px; margin: auto; padding: 32px; }}
    header {{ margin-bottom: 24px; }}
    h1 {{ margin-bottom: 8px; }}
    p, footer {{ color: #94a3b8; }}
    section {{ margin-bottom: 20px; }}
    footer {{ font-size: 12px; }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Gaming analytics</h1>
      <p>Period: {period}</p>
    </header>
    {content}
    <footer>Generated at {generated_at}</footer>
  </main>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--lookback-days", type=int, default=0)
    parser.add_argument(
        "--output",
        default="reports/gaming_overview.html",
    )
    arguments = parser.parse_args()
    destination = build_report(
        output_path=arguments.output,
        date_from=arguments.date_from,
        date_to=arguments.date_to,
        lookback_days=arguments.lookback_days,
    )
    print(destination)


if __name__ == "__main__":
    main()
