{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['month','category'],
  partition_by={
    "field": "month",
    "data_type": "date",
    "granularity": "month"
  },
  cluster_by=['category']
) }}

with base_cte as (
  Select
  DATE_TRUNC(fo.order_date, MONTH) as month,
  sp.category as category,
  fo.order_id as order_id,
  so.quantity as quantity,
  sp.product_id,
  so.unit_price as unit_price,
  From {{ ref('fct_orders') }} as fo
  left join {{ ref('stg_order_items') }} as so using(order_id)
  left join {{ ref('stg_products_ext') }} sp using(product_id)
  Where fo.status = 'completed'
  {% if is_incremental() %}
    and DATE_TRUNC(fo.order_date, MONTH) >= (
      SELECT DATE_SUB(COALESCE(MAX(month), DATE '1900-01-01'), INTERVAL 1 MONTH)
      FROM {{ this }}
    )
  {% endif %}
),
prod_rev as (
    Select
    month,
    category,
    product_id,
    Sum(quantity*unit_price) as prod_revenue,
    From base_cte
    Group by 1,2,3
),
top_prod as(
    Select
    month,
    category,
    product_id as top_product,
    prod_revenue,
    From prod_rev
    Qualify Row_Number() over(partition by month, category order by prod_revenue desc, product_id asc) = 1
),

month_cat as (
    Select
    month,
    category,
    Count(Distinct(order_id)) as orders_cnt,
    SUM(quantity*unit_price) as revenue,
    from base_cte
    Group by 1,2
)

Select
  m.*,
  tp.top_product,
  tp.prod_revenue
From month_cat as m
left join top_prod as tp using(month, category)
