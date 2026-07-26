from __future__ import annotations

import pytest
from src.database.postgres import postgres_transaction
from src.loaders.postgres.dictionary import (
    DictionarySpec,
    load_dictionary,
)

pytestmark = pytest.mark.integration

SPEC = DictionarySpec(
    schema="public",
    table="_integration_dictionary",
    columns=(
        ("id", "BIGINT"),
        ("name", "TEXT"),
    ),
    key_columns=("id",),
    mutable_columns=("name",),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS public._integration_dictionary
        (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """,
)


def test_dictionary_insert_update_and_unchanged():
    _drop_test_table()
    try:
        first = load_dictionary(
            spec=SPEC,
            rows=((1, "first"), (2, "second")),
        )
        unchanged = load_dictionary(
            spec=SPEC,
            rows=((1, "first"), (2, "second")),
        )
        changed = load_dictionary(
            spec=SPEC,
            rows=((1, "updated"), (2, "second"), (3, "third")),
        )

        assert (first.inserted_rows, first.updated_rows) == (2, 0)
        assert unchanged.unchanged_rows == 2
        assert (
            changed.inserted_rows,
            changed.updated_rows,
            changed.unchanged_rows,
        ) == (1, 1, 1)
    finally:
        _drop_test_table()


def _drop_test_table():
    with (
        postgres_transaction() as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("DROP TABLE IF EXISTS public._integration_dictionary")
