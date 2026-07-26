SELECT
    summary_month,
    sum(deposits_usd) AS total_deposits_usd,
    sum(withdrawals_usd) AS total_withdrawals_usd,
    sum(bets_usd) AS total_bets_usd
FROM analytics.monthly_summary
WHERE 1 = 1
[[AND summary_month >= toStartOfMonth({{date_from}})]]
[[AND summary_month <= toStartOfMonth({{date_to}})]]
GROUP BY summary_month
ORDER BY summary_month
