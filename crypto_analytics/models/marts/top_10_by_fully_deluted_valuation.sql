WITH ranked AS (
  SELECT id, name, fully_diluted_valuation,
    ROW_NUMBER() OVER (PARTITION BY name ORDER BY fully_diluted_valuation DESC) AS rn
  FROM gecko_coins
)

SELECT id, name, fully_diluted_valuation
FROM ranked
WHERE rn = 1 AND fully_diluted_valuation IS NOT NULL
ORDER BY fully_diluted_valuation DESC
LIMIT 10