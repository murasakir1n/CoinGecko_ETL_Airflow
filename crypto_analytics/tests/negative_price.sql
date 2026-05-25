select
    id,
    current_price
from {{ ref('top_10_coins_by_price') }}
where current_price < 0