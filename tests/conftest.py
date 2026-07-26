from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def data_dir() -> Path:
    candidates = (
        Path("/opt/airflow/dags/files"),
        Path("airflow/dags/files"),
    )
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    raise RuntimeError("Test CSV directory was not found")
