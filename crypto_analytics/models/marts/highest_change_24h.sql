WITH ranked AS (
  SELECT id, name, price_change_24h,
    ROW_NUMBER() OVER (PARTITION BY name ORDER BY price_change_24h DESC) AS rn
  FROM gecko_coins
)

SELECT id, name, price_change_24h
FROM ranked
WHERE rn = 1 AND price_change_24h IS NOT NULL
ORDER BY price_change_24h DESC
LIMIT 10