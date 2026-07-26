from __future__ import annotations

from datetime import date
from pathlib import Path

from src.loaders.postgres.dictionary import (
    DictionarySpec,
    load_dictionary,
)
from src.validation.csv_reader import read_csv_rows
from src.validation.values import (
    ensure_unique,
    parse_choice,
    parse_date,
    parse_int,
    parse_required_string,
)

REGISTRATION_TYPES = {"standard", "premium", "vip"}

PLAYERS_SPEC = DictionarySpec(
    schema="public",
    table="players",
    columns=(
        ("id", "INTEGER"),
        ("registration_date", "DATE"),
        ("registration_type", "VARCHAR(20)"),
        ("country", "VARCHAR(2)"),
    ),
    key_columns=("id",),
    mutable_columns=(
        "registration_date",
        "registration_type",
        "country",
    ),
    create_table_sql="""
        CREATE TABLE IF NOT EXISTS public.players
        (
            id INTEGER PRIMARY KEY,
            registration_date DATE NOT NULL,
            registration_type VARCHAR(20) NOT NULL,
            country VARCHAR(2) NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT players_registration_type_chk
                CHECK (registration_type IN ('standard', 'premium', 'vip')),
            CONSTRAINT players_country_length_chk
                CHECK (char_length(country) = 2)
        )
    """,
    migration_sql=(
        """
        ALTER TABLE public.players
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ
            NOT NULL DEFAULT CURRENT_TIMESTAMP
        """,
        """
        CREATE INDEX IF NOT EXISTS players_country_idx
            ON public.players (country)
        """,
    ),
)


def prepare_players(
    file_path: str | Path,
) -> list[tuple[int, date, str, str]]:
    source_rows = read_csv_rows(
        file_path,
        expected_columns={
            "id",
            "registration_date",
            "registration_type",
            "country",
        },
    )
    rows = []
    seen_ids: set[object] = set()

    for line_number, source in enumerate(source_rows, start=2):
        player_id = parse_int(
            source["id"],
            "id",
            line_number,
            min_value=1,
        )
        ensure_unique(player_id, seen_ids, "id", line_number)

        registration_type = parse_choice(
            source["registration_type"].lower(),
            "registration_type",
            line_number,
            allowed_values=REGISTRATION_TYPES,
        )
        country = parse_required_string(
            source["country"],
            "country",
            line_number,
        ).upper()
        if len(country) != 2 or not country.isalpha():
            raise ValueError(
                f"Country must be a two-letter code at line {line_number}",
            )

        rows.append(
            (
                player_id,
                parse_date(
                    source["registration_date"],
                    "registration_date",
                    line_number,
                ),
                registration_type,
                country,
            )
        )

    return rows


def load_players(file_path: str | Path) -> dict[str, object]:
    return load_dictionary(
        spec=PLAYERS_SPEC,
        rows=prepare_players(file_path),
    ).as_dict()
