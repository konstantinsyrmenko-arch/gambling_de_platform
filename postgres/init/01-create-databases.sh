#!/usr/bin/env bash
set -euo pipefail

create_database_and_user() {
  local database="$1"
  local username="$2"
  local password="$3"

  psql --username "$POSTGRES_USER" --dbname postgres -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${username}') THEN
    CREATE ROLE ${username} LOGIN PASSWORD '${password}';
  END IF;
END
\$\$;
SQL

  if ! psql --username "$POSTGRES_USER" --dbname postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='${database}'" | grep -q 1; then
    psql --username "$POSTGRES_USER" --dbname postgres -v ON_ERROR_STOP=1 \
      -c "CREATE DATABASE ${database} OWNER ${username};"
  fi
}

create_database_and_user "$AIRFLOW_DB_NAME" "$AIRFLOW_DB_USER" "$AIRFLOW_DB_PASSWORD"
create_database_and_user "$APP_DB_NAME" "$APP_DB_USER" "$APP_DB_PASSWORD"
create_database_and_user "$METABASE_DB_NAME" "$METABASE_DB_USER" "$METABASE_DB_PASSWORD"
