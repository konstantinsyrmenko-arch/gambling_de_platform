#!/usr/bin/env bash
set -euo pipefail

clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};

CREATE DATABASE IF NOT EXISTS postgres_source
ENGINE = PostgreSQL(
  '${POSTGRES_HOST}:${POSTGRES_PORT}',
  '${APP_DB_NAME}',
  '${APP_DB_USER}',
  '${APP_DB_PASSWORD}'
);
SQL
