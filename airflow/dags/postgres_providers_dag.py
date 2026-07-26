from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task
from src.config import DAG_DEFAULT_ARGS
from src.loaders.postgres.providers import load_providers

LOGGER = logging.getLogger(__name__)

CSV_FILE = Path(__file__).parent / "files" / "providers_map.csv"


@dag(
    dag_id="load_providers_to_postgres",
    description="Load providers from CSV to PostgreSQL",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DAG_DEFAULT_ARGS,
    tags=["postgres", "providers"],
)
def load_providers_to_postgres():
    @task(do_xcom_push=False)
    def load_file() -> None:
        result = load_providers(file_path=CSV_FILE)
        LOGGER.info("Providers loaded: %s", result)

    load_file()


load_providers_to_postgres()
