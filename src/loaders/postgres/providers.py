from __future__ import annotations

from pathlib import Path

from src.loaders.postgres.dictionary import (
    DictionarySpec,
    load_dictionary,
)
from src.validation.csv_reader import read_csv_rows
from src.validation.values import (
    ensure_unique,
    parse_int,
    parse_required_string,
)

PROVIDERS_SPEC = DictionarySpec(
    schema="public",
    table="providers",
    columns=(
        ("id", "BIGINT"),
        ("provider_name", "TEXT"),
    ),
    key_columns=("id",),
    mutable_columns=("provider_name",),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS public.providers
        (
            id BIGINT PRIMARY KEY,
            provider_name TEXT NOT NULL UNIQUE,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT providers_name_not_empty
                CHECK (char_length(trim(provider_name)) > 0)
        )
    """,
    migration_sql=(
        """
        ALTER TABLE public.providers
        ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ
            NOT NULL DEFAULT CURRENT_TIMESTAMP
        """,
        """
        ALTER TABLE public.providers
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
            NOT NULL DEFAULT CURRENT_TIMESTAMP
        """,
    ),
)


def prepare_providers(file_path: str | Path) -> list[tuple[int, str]]:
    source_rows = read_csv_rows(
        file_path,
        expected_columns={"id", "provider_name"},
    )
    rows = []
    seen_ids: set[object] = set()
    seen_names: set[object] = set()

    for line_number, source in enumerate(source_rows, start=2):
        provider_id = parse_int(
            source["id"],
            "id",
            line_number,
            min_value=1,
        )
        provider_name = parse_required_string(
            source["provider_name"],
            "provider_name",
            line_number,
        )
        ensure_unique(provider_id, seen_ids, "id", line_number)
        ensure_unique(
            provider_name.casefold(),
            seen_names,
            "provider_name",
            line_number,
        )
        rows.append((provider_id, provider_name))

    return rows


def load_providers(file_path: str | Path) -> dict[str, object]:
    result = load_dictionary(
        spec=PROVIDERS_SPEC,
        rows=prepare_providers(file_path),
    )
    return result.as_dict()
