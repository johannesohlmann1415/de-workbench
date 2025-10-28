{{ config(materialized='view') }}
with order_item_cte as (
    Select
        safe_cast(trim(cast(order_id as string)) as int64) as order_id,
        safe_cast(trim(cast(product_id as string)) as int64) as product_id,
        safe_cast(trim(cast(quantity as string)) as int64) as quantity,
        safe_cast(trim(cast(unit_price as string)) as NUMERIC) as unit_price,
    FROM {{ ref('order_items') }}
)
Select
*,
unit_price * quantity as line_revenue
From order_item_cte