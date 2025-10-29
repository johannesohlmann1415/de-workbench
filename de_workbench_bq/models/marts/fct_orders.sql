{{ config(materialized='incremental',
    unique_key = 'order_id',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ['customer_id', 'status'],
    incremental_strategy="merge",
    merge_update_columns = ['status', 'shipping_price', 'payment_method', 'items_revenue', 'order_total_calc']
    ) }}
With items as (
  Select
    order_id,
    SUM(i.quantity*i.unit_price) as items_revenue,
  From {{ ref('stg_order_items') }} as i
  Group by order_id
)
Select
  o.order_id, 
  o.customer_id, 
  o.order_date,
  o.status,
  o.total_amount,
  o.coupon_count, 
  o.first_coupon, 
  o.device, 
  o.payment_method, 
  o.shipping_method,
  o.shipping_price,
  CASE WHEN status = "completed" then True else False End as is_completed,
  COALESCE(i.items_revenue, 0) as items_revenue,
  COALESCE(shipping_price + items_revenue, 0) as order_total_calc
From {{ ref('stg_orders_ext') }} as o
left join items as i using(order_id)
{% if is_incremental() %}
  WHERE order_date >= (
    SELECT DATE_SUB(COALESCE(MAX(order_date), DATE '1900-01-01'), INTERVAL 2 DAY)
    FROM {{ this }}
  )
{% endif %}