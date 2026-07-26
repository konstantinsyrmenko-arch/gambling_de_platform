from datetime import timedelta

POSTGRES_CONN_ID = "PG_analyst"
CLICKHOUSE_CONN_ID = "CH_analyst"

SUPPORTED_CURRENCIES = frozenset({"USD", "EUR", "GBP", "RUB"})

DAG_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}
