WITH ranked AS (
  SELECT id, name, market_cap,
    ROW_NUMBER() OVER (PARTITION BY name ORDER BY market_cap DESC) AS rn
  FROM gecko_coins
)
SELECT id, name, market_cap
FROM ranked
WHERE rn = 1
ORDER BY market_cap DESC
LIMIT 10