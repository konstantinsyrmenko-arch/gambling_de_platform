from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task
from src.config import DAG_DEFAULT_ARGS
from src.loaders.postgres.games import load_games

LOGGER = logging.getLogger(__name__)

CSV_FILE = Path(__file__).parent / "files" / "games_map.csv"


@dag(
    dag_id="load_games_to_postgres",
    description="Load games from CSV to PostgreSQL",
    schedule=None,
    start_date=datetime(2026, 7, 22),
    catchup=False,
    max_active_runs=1,
    default_args=DAG_DEFAULT_ARGS,
    tags=["postgres", "csv", "games"],
)
def load_games_to_postgres():
    @task(do_xcom_push=False)
    def load_file() -> None:
        result = load_games(file_path=CSV_FILE)
        LOGGER.info("Games loaded: %s", result)

    load_file()


load_games_to_postgres()
