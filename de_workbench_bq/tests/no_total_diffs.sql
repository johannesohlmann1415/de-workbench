SELECT *
FROM {{ ref('fct_orders') }}
WHERE total_amount <> items_revenue