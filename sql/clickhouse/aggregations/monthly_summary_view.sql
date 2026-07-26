CREATE OR REPLACE VIEW analytics.monthly_summary AS
SELECT
    summary_month,
    country,
    toDecimal128(sum(deposits_usd), 2) AS deposits_usd,
    toDecimal128(sum(withdrawals_usd), 2) AS withdrawals_usd,
    toDecimal128(sum(bets_usd), 2) AS bets_usd
FROM
(
    SELECT
        toStartOfMonth(deposits.deposit_date) AS summary_month,
        players.country AS country,
        sum(
            toDecimal128(
                toDecimal128(deposits.amount, 6)
                / toDecimal128(rates.rate_to_usd, 6),
                6
            )
        ) AS deposits_usd,
        toDecimal128(0, 2) AS withdrawals_usd,
        toDecimal128(0, 2) AS bets_usd
    FROM analytics.deposits AS deposits
    INNER JOIN postgres_source.players AS players
        ON toUInt64(players.id) = deposits.player_id
    INNER JOIN postgres_source.exchange_rates AS rates
        ON rates.rate_date = deposits.deposit_date
       AND rates.currency = deposits.currency
    GROUP BY summary_month, country

    UNION ALL

    SELECT
        toStartOfMonth(withdrawals.withdrawal_date) AS summary_month,
        players.country AS country,
        toDecimal128(0, 2) AS deposits_usd,
        sum(
            toDecimal128(
                toDecimal128(withdrawals.amount, 6)
                / toDecimal128(rates.rate_to_usd, 6),
                6
            )
        ) AS withdrawals_usd,
        toDecimal128(0, 2) AS bets_usd
    FROM analytics.withdrawals AS withdrawals
    INNER JOIN postgres_source.players AS players
        ON toUInt64(players.id) = withdrawals.player_id
    INNER JOIN postgres_source.exchange_rates AS rates
        ON rates.rate_date = withdrawals.withdrawal_date
       AND rates.currency = withdrawals.currency
    GROUP BY summary_month, country

    UNION ALL

    SELECT
        toStartOfMonth(games.game_date) AS summary_month,
        players.country AS country,
        toDecimal128(0, 2) AS deposits_usd,
        toDecimal128(0, 2) AS withdrawals_usd,
        sum(
            toDecimal128(
                toDecimal128(games.amount, 6)
                / toDecimal128(rates.rate_to_usd, 6),
                6
            )
        ) AS bets_usd
    FROM analytics.game_transactions AS games
    INNER JOIN postgres_source.players AS players
        ON toUInt64(players.id) = games.player_id
    INNER JOIN postgres_source.exchange_rates AS rates
        ON rates.rate_date = games.game_date
       AND rates.currency = games.currency
    GROUP BY summary_month, country
)
GROUP BY summary_month, country
