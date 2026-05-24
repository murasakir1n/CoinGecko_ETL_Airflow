select
    id,
    name,
    max(current_price) as current_price
from gecko_coins
group by name, id
order by current_price desc
limit 10