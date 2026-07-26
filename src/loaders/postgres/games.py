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

GAMES_SPEC = DictionarySpec(
    schema="public",
    table="games",
    columns=(
        ("id", "INTEGER"),
        ("game_name", "VARCHAR(255)"),
        ("provider_id", "BIGINT"),
    ),
    key_columns=("id",),
    mutable_columns=("game_name", "provider_id"),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS public.games
        (
            id INTEGER PRIMARY KEY,
            game_name VARCHAR(255) NOT NULL,
            provider_id BIGINT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT games_name_not_empty_chk
                CHECK (char_length(trim(game_name)) > 0),
            CONSTRAINT games_provider_fk
                FOREIGN KEY (provider_id) REFERENCES public.providers(id)
        )
    """,
    migration_sql=(
        """
        ALTER TABLE public.games
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
            NOT NULL DEFAULT CURRENT_TIMESTAMP
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'games_provider_fk'
                  AND conrelid = 'public.games'::regclass
            ) THEN
                ALTER TABLE public.games
                ADD CONSTRAINT games_provider_fk
                    FOREIGN KEY (provider_id)
                    REFERENCES public.providers(id);
            END IF;
        END
        $$
        """,
        """
        CREATE INDEX IF NOT EXISTS games_provider_id_idx
            ON public.games (provider_id)
        """,
    ),
)


def prepare_games(
    file_path: str | Path,
) -> list[tuple[int, str, int]]:
    source_rows = read_csv_rows(
        file_path,
        expected_columns={"id", "game_name", "provider_id"},
    )
    rows = []
    seen_ids: set[object] = set()
    seen_names: set[object] = set()

    for line_number, source in enumerate(source_rows, start=2):
        game_id = parse_int(
            source["id"],
            "id",
            line_number,
            min_value=1,
        )
        game_name = parse_required_string(
            source["game_name"],
            "game_name",
            line_number,
        )
        ensure_unique(game_id, seen_ids, "id", line_number)
        ensure_unique(
            game_name.casefold(),
            seen_names,
            "game_name",
            line_number,
        )
        rows.append(
            (
                game_id,
                game_name,
                parse_int(
                    source["provider_id"],
                    "provider_id",
                    line_number,
                    min_value=1,
                ),
            )
        )

    return rows


def load_games(file_path: str | Path) -> dict[str, object]:
    return load_dictionary(
        spec=GAMES_SPEC,
        rows=prepare_games(file_path),
    ).as_dict()
