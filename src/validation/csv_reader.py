from __future__ import annotations

import csv
from collections.abc import Collection
from pathlib import Path

from src.validation.errors import DataValidationError


def read_csv_rows(
    file_path: str | Path,
    expected_columns: Collection[str],
) -> list[dict[str, str]]:
    path = Path(file_path)
    if not path.is_file():
        raise DataValidationError(f"CSV file does not exist: {path}")

    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        columns = reader.fieldnames or []
        if len(columns) != len(set(columns)):
            raise DataValidationError(
                f"CSV header contains duplicate columns: {path}",
            )
        if set(columns) != set(expected_columns):
            raise DataValidationError(
                f"CSV schema does not match the data contract: {path}",
            )

        rows = []
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise DataValidationError(
                    f"Malformed CSV row at line {line_number}: {path}",
                )
            rows.append(
                {column: value if value is not None else "" for column, value in row.items()}
            )

    if not rows:
        raise DataValidationError(f"CSV file contains no data: {path}")
    return rows
