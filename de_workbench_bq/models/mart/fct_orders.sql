{{ config(materialized='incremental',
    unique_key = 'order_id',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ['customer_id', 'status']) }}
Select
o.order_id, 
o.customer_id, 
o.order_date,
o.status,
--o.is_completed,
o.total_amount,
o.coupon_count, 
o.first_coupon, 
o.device, 
o.payment_method, 
o.shipping_method,
--as items_revenue,
--as shipping_price,
--as order_total_calc,
SUM(i.quantity*i.unit_price) over(Partition by i.order_id) as items_revenue
From {{ ref('stg_orders_ext') }} as o
left join {{ ref('stg_order_items') }} as i using(order_id)