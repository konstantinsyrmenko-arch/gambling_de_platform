# Metabase dashboard

Create a ClickHouse connection in Metabase:

- Host: `clickhouse`
- Port: `8123`
- Database: `analytics`
- Username and password: ClickHouse values from `.env`
- SSL: disabled for the local environment

Create two native-query questions from the SQL files in this directory:

| SQL file | Visualization |
|---|---|
| `01_monthly_financial_activity.sql` | Line chart; X: month; Y: three USD series |
| `02_financial_by_country.sql` | Grouped bar; X: country; Y: three USD series |

Configure `date_from` and `date_to` as optional variables of type `Date`.
Add both questions to a dashboard named `Gaming Overview` and connect the
dashboard date filters to these variables.

The questions read `analytics.monthly_summary`. Rebuild the view after loading
new statistics:

```bash
python -m src.aggregations.build --rebuild
```
