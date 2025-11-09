{{ config(materialized='incremental',
    unique_key = ['month', 'category'],
    partition_by={
      "field": "month",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ['category'],
    incremental_strategy="merge",
    ) }}

)
with top_prod as (
  Select
  DATE_TRUNC(fo.order_date, MONTH) as month,
  sp.category,
  sp.product_id as top_prod,
  SUM(so.quantity*so.unit_price) as prod_revenue,
  Row_Number() over(partition_by month, category order by SUM(so.quantity*so.unit_price) desc) as rk
  From {{ ref('fct_orders') }} as fo
  left join {{ ref('stg_order_items') }} as so using(order_id)
  left join {{ ref('stg_products_ext') }} sp using(product_id)
  Where fo.status = 'completed'
  Group by 1, 4
  Qualify rk = 1
),
base_cte as (
  Select
  DATE_TRUNC(fo.order_date, MONTH) as month,
  sp.category as category,
  fo.order_id as order_id,
  so.quantity as quantity,
  so.unit_price as unit_price,
  From {{ ref('fct_orders') }} as fo
  left join {{ ref('stg_order_items') }} as so using(order_id)
  left join {{ ref('stg_products_ext') }} sp using(product_id)
  left join top_prod as tp using(month, category)
  Where fo.status = 'completed'
  {% if is_incremental() %}
    and month >= (
      SELECT DATE_SUB(COALESCE(MAX(DATE_TRUNC(fo.order_date, MONTH), DATE '1900-01-01'), INTERVAL 1 MONTH)
      FROM {{ this }}
    )
  {% endif %}
)
Select
  b.month,
  b.category,
  Count(Distinct(b.order_id)) as orders_cnt,
  SUM(b.quantity*b.unit_price) as revenue,
  tp.top_prod
From base_cte as b
left join top_prod as tp using(month, category)
Group by 1