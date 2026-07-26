"""Migrate fact tables from ReplacingMergeTree to MergeTree."""

from __future__ import annotations

from uuid import uuid4

from src.database.clickhouse import clickhouse_client
from src.loaders.clickhouse.deposits import DEPOSITS_SPEC
from src.loaders.clickhouse.game_transactions import GAME_TRANSACTIONS_SPEC
from src.loaders.clickhouse.monthly_partitions import StatisticsSpec
from src.loaders.clickhouse.withdrawals import WITHDRAWALS_SPEC

FACT_SPECS = (
    DEPOSITS_SPEC,
    WITHDRAWALS_SPEC,
    GAME_TRANSACTIONS_SPEC,
)


def migrate_fact_tables_to_merge_tree() -> dict[str, str]:
    result = {}
    with clickhouse_client() as client:
        for spec in FACT_SPECS:
            result[spec.full_table_name] = _migrate_table(client, spec)
    return result


def _migrate_table(client, spec: StatisticsSpec) -> str:
    engine = _table_engine(client, spec)
    if engine is None:
        client.command(spec.create_table_sql)
        return "created"
    if engine == "MergeTree":
        return "unchanged"
    if engine != "ReplacingMergeTree":
        raise RuntimeError(
            f"Cannot migrate {spec.full_table_name} from engine {engine}",
        )

    staging = f"{spec.full_table_name}_merge_tree_{uuid4().hex[:12]}"
    create_sql = spec.create_table_sql.replace(
        spec.full_table_name,
        staging,
        1,
    )
    client.command(create_sql)
    try:
        columns = ", ".join(spec.columns)
        client.command(
            f"""
            INSERT INTO {staging} ({columns})
            SELECT {columns}
            FROM {spec.full_table_name} FINAL
            """
        )
        source_rows = _row_count(client, f"{spec.full_table_name} FINAL")
        migrated_rows = _row_count(client, staging)
        if source_rows != migrated_rows:
            raise RuntimeError(
                f"Migration validation failed for {spec.full_table_name}: "
                f"expected {source_rows}, got {migrated_rows}",
            )

        client.command(
            f"EXCHANGE TABLES {spec.full_table_name} AND {staging}",
        )
    finally:
        client.command(f"DROP TABLE IF EXISTS {staging}")

    return "migrated"


def _table_engine(client, spec: StatisticsSpec) -> str | None:
    rows = client.query(
        """
        SELECT engine
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        """,
        parameters={
            "database": spec.database,
            "table": spec.table,
        },
    ).result_rows
    return str(rows[0][0]) if rows else None


def _row_count(client, table_expression: str) -> int:
    return int(
        client.query(
            f"SELECT count() FROM {table_expression}",
        ).result_rows[0][0]
    )


if __name__ == "__main__":
    print(migrate_fact_tables_to_merge_tree())
