# Local DE stack: Airflow CeleryExecutor

Локальная data-платформа для загрузки игровой статистики, расчёта витрин и
визуализации результатов. Пайплайн построен на Airflow, PostgreSQL и
ClickHouse; для визуализации используются Plotly и Metabase.

## Архитектура

```text
CSV ──> PostgreSQL dictionaries ──┐
                                  ├──> ClickHouse facts
CSV ──────────────────────────────┘             │
                                                ▼
                                      monthly_summary
                                        ├──> Plotly
                                        └──> Metabase
```

В `docker-compose.yml` входят Airflow 3.0.6 с CeleryExecutor, PostgreSQL 16,
Redis, ClickHouse 25.6, Metabase 0.56 и Flower. Код находится в `src`, DAG — в
`airflow/dags`, SQL — в `sql`, тесты — в `tests`.

## Запуск

Понадобятся Docker с Compose v2 и не менее 6 ГБ свободной памяти.

```bash
cp .env.example .env
openssl rand -base64 24
openssl rand -hex 32
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Запишите сгенерированные значения в `.env`: отдельный пароль для каждого
`*_PASSWORD`, hex-значение для `AIRFLOW_JWT_SECRET` и Fernet key для
`AIRFLOW_FERNET_KEY`. Файл `.env` исключён из Git и предназначен только для
локального окружения.

После заполнения `.env`:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Airflow создаёт локальный пароль администратора при первом запуске. Получить
его можно из запущенного контейнера:

```bash
docker compose exec airflow-api-server \
  cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Логин — `admin`. Пароль не фиксируется в репозитории и может измениться после
пересоздания контейнера. Учётная запись Metabase создаётся вручную в мастере
первого запуска; её данные также не нужно сохранять в проекте.

| Компонент | Адрес |
|---|---|
| Airflow | <http://localhost:8080> |
| Flower | <http://localhost:5555> |
| Metabase | <http://localhost:3000> |
| PostgreSQL | `localhost:5432` |
| ClickHouse HTTP | `localhost:8123` |
| ClickHouse native | `localhost:9000` |

Airflow Connections `PG_analyst` и `CH_analyst` создаются из переменных
окружения в `docker-compose.yml`; вручную добавлять их в UI не требуется.

## Исходные данные

CSV-файлы находятся в `airflow/dags/files`.

| Файл | Таблица | Ключ |
|---|---|---|
| `providers_map.csv` | `public.providers` | `id` |
| `games_map.csv` | `public.games` | `id` |
| `players.csv` | `public.players` | `id` |
| `currency_rates.csv` | `public.exchange_rates` | `date, currency` |
| `deposits.csv` | `analytics.deposits` | `id` |
| `withdrawals.csv` | `analytics.withdrawals` | `id` |
| `games.csv` | `analytics.game_transactions` | `id` |

Перед загрузкой проверяются структура CSV, типы, денежные значения, валюты и
уникальность ключей.

## ETL

Управляющий DAG `gaming_etl_pipeline` запускается ежедневно в `01:00 UTC`:

```text
providers ──> games ─────────────────────┐
players ─────────────────────────────────┼─> facts
exchange_rates ──────────────────────────┘      └─> monthly_summary ─> Plotly
```

Дочерние DAG запускаются управляющим DAG или вручную. `lookback_days`
ограничивает загрузку последними днями до текущей UTC-даты включительно:
`1` — сегодня, `7` — последние семь дней, `0` — весь файл. Для исторического
перезапуска используются `period_from` и `period_to`. Смешивать явный период
с положительным `lookback_days` нельзя.

При временной ошибке задача повторяется до двух раз с увеличением задержки.

### Справочники

Справочники загружаются в PostgreSQL через временную staging-таблицу. В
целевую таблицу вставляются новые строки и обновляются только действительно
изменившиеся. Повторная загрузка того же файла не изменяет данные и
`updated_at`.

Поле `exchange_rates.rate_to_usd` трактуется так:

```text
1 USD = rate_to_usd единиц валюты
amount_usd = amount / rate_to_usd
```

### Статистика

Депозиты, выводы и игровые транзакции хранятся в ClickHouse в месячных
партициях на `MergeTree`. Загрузка заменяет только затронутые месяцы и
объединяет новую выборку с уже существующими строками месяца по `id`.

Перед заменой партиции проверяются ссылки на игроков, провайдеров, игры и
курсы валют в PostgreSQL. При ошибке staging не публикуется. Каждый месяц
проверяется и заменяется отдельно с помощью атомарного `REPLACE PARTITION`.

## Аналитика и визуализация

DAG `build_analytics_outputs` пересоздаёт представление
`analytics.monthly_summary`.

`monthly_summary` содержит суммы депозитов, выводов и ставок в USD по месяцам
и странам.

Ручная пересборка:

```bash
docker compose run --rm --no-deps \
  --entrypoint python airflow-scheduler \
  -m src.aggregations.build --rebuild
```

Plotly-отчёт создаётся последним шагом общего пайплайна и сохраняется в
`reports/gaming_overview.html`. Он содержит динамику финансовых показателей по
месяцам и распределение по странам. Отчёт можно собрать отдельно:

```bash
docker compose run --rm --no-deps \
  --entrypoint python airflow-scheduler \
  -m src.visualization.report \
  --output /opt/airflow/reports/gaming_overview.html
```

Для ограничения периода доступны параметры `--date-from` и `--date-to`.
Последние дни можно выбрать параметром `--lookback-days`; значение `0`
обрабатывает весь доступный период.

### Metabase

При первом входе добавьте ClickHouse с параметрами:

| Поле | Значение |
|---|---|
| Host | `clickhouse` |
| Port | `8123` |
| Database | `analytics` |
| User | `CLICKHOUSE_USER` из `.env` |
| Password | `CLICKHOUSE_PASSWORD` из `.env` |
| SSL | выключен |

SQL-карточки и рекомендуемый layout dashboard находятся в
[sql/metabase/README.md](sql/metabase/README.md).

## Проверки

```bash
# Все тесты
docker compose run --rm --no-deps \
  --entrypoint pytest airflow-scheduler /opt/airflow/tests -q

# Статический анализ
docker compose run --rm --no-deps \
  --entrypoint ruff airflow-scheduler \
  check /opt/airflow/src /opt/airflow/dags /opt/airflow/tests

# Ошибки импорта DAG
docker compose run --rm --no-deps \
  --entrypoint airflow airflow-scheduler dags list-import-errors
```

## Диагностика

```bash
docker compose ps
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 airflow-worker
```

После изменения `.env`, `docker-compose.yml` или переменных окружения
пересоздайте контейнеры:

```bash
docker compose up -d --force-recreate
```

Init-скрипты PostgreSQL и ClickHouse выполняются только при создании нового
volume.

## Сброс окружения

> [!CAUTION]
> Следующая команда удаляет локальные данные PostgreSQL, ClickHouse, Redis,
> Airflow и Metabase.

```bash
docker compose down -v --remove-orphans
```
