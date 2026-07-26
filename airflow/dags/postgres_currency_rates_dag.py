from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from src.config import DAG_DEFAULT_ARGS
from src.loaders.postgres.exchange_rates import load_exchange_rates

LOGGER = logging.getLogger(__name__)

CSV_FILE = Path("/opt/airflow/dags/files/currency_rates.csv")


@dag(
    dag_id="load_exchange_rates_to_postgres",
    description="Load exchange rates from CSV to PostgreSQL",
    schedule=None,
    start_date=datetime(2026, 7, 22),
    catchup=False,
    max_active_runs=1,
    default_args=DAG_DEFAULT_ARGS,
    tags=["postgres", "csv", "exchange-rates"],
    params={
        "lookback_days": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Days through today to reload; 0 means all data",
        ),
        "period_from": Param(
            default="",
            type=["null", "string"],
            description="Inclusive period start, YYYY-MM-DD",
        ),
        "period_to": Param(
            default="",
            type=["null", "string"],
            description="Inclusive period end, YYYY-MM-DD",
        ),
    },
)
def load_exchange_rates_to_postgres():
    @task(do_xcom_push=False)
    def load_file() -> None:
        context = get_current_context()
        params = context["params"]

        result = load_exchange_rates(
            file_path=CSV_FILE,
            period_from=params.get("period_from"),
            period_to=params.get("period_to"),
            lookback_days=params.get("lookback_days"),
        )

        LOGGER.info("Exchange rates loaded: %s", result)

    load_file()


load_exchange_rates_to_postgres()
