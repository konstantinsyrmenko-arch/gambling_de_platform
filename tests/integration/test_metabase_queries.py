from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from src.database.clickhouse import clickhouse_client

pytestmark = pytest.mark.integration


def test_all_metabase_queries_execute():
    sql_directory = _sql_directory()
    cards = sorted(sql_directory.glob("[0-9][0-9]_*.sql"))
    parameters = {
        "date_from": date(2023, 1, 1),
        "date_to": date(2023, 1, 31),
    }

    with clickhouse_client() as client:
        for card in cards:
            query = (
                card.read_text(encoding="utf-8")
                .replace("[[", "")
                .replace("]]", "")
                .replace("{{date_from}}", "{date_from:Date}")
                .replace("{{date_to}}", "{date_to:Date}")
            )
            rows = client.query(
                query,
                parameters=parameters,
            ).result_rows
            assert rows, f"{card.name} returned no rows"


def _sql_directory() -> Path:
    candidates = (
        Path("/opt/airflow/sql/metabase"),
        Path("sql/metabase"),
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    raise RuntimeError("Metabase SQL directory was not found")
