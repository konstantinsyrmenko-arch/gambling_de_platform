from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from src.aggregations.build import build_monthly_summary
from src.config import DAG_DEFAULT_ARGS
from src.visualization.report import build_report

LOGGER = logging.getLogger(__name__)

REPORT_PATH = Path("/opt/airflow/reports/gaming_overview.html")
MINIMUM_REPORT_SIZE = 1_000_000


@dag(
    dag_id="build_analytics_outputs",
    description="Rebuild the monthly summary and Plotly report",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DAG_DEFAULT_ARGS,
    params={
        "lookback_days": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Days through today to include; 0 means all data",
        ),
        "date_from": Param(
            default="",
            type=["null", "string"],
            title="Report period start",
            description="Inclusive, YYYY-MM-DD",
        ),
        "date_to": Param(
            default="",
            type=["null", "string"],
            title="Report period end",
            description="Inclusive, YYYY-MM-DD",
        ),
    },
    tags=["analytics", "clickhouse", "plotly"],
)
def build_analytics_outputs():
    @task(do_xcom_push=False)
    def build_summary() -> None:
        result = build_monthly_summary()
        LOGGER.info("Monthly summary rebuilt: %s", result)

    @task(do_xcom_push=False)
    def build_plotly_report() -> None:
        parameters = get_current_context()["params"]
        destination = build_report(
            output_path=REPORT_PATH,
            date_from=parameters.get("date_from"),
            date_to=parameters.get("date_to"),
            lookback_days=parameters.get("lookback_days"),
        )
        report_size = destination.stat().st_size
        if report_size < MINIMUM_REPORT_SIZE:
            raise RuntimeError(
                f"Plotly report is unexpectedly small: {report_size} bytes",
            )
        LOGGER.info(
            "Plotly report generated: path=%s, size=%s bytes",
            destination,
            report_size,
        )

    build_summary() >> build_plotly_report()


build_analytics_outputs()
