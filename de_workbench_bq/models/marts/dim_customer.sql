{{ config(materialized='table') }}
with orders_cte as (
    Select
    o.customer_id,
    Sum(o.items_revenue) as ltv_completed,
    Min(o.order_date) as first_completed_order_date,
    Max(o.order_date) as last_completed_order_date,
    Count(Distinct(o.order_id)) as orders_completed_cnt,
    From {{ ref('fct_orders') }} as o
    where o.status='completed'
    Group by o.customer_id
)

Select
    c.customer_id, 
    c.customer_name, 
    c.country, 
    c.signup_date, 
    COALESCE(o.ltv_completed, 0) as ltv_completed,
    o.first_completed_order_date,
    o.last_completed_order_date,
    COALESCE(o.orders_completed_cnt, 0) as orders_completed_cnt,
    Case when o.ltv_completed >= 300 then True else False end as vip_flag,
    Case when last_completed_order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 Day) then True else False end as recent_30d_flag,
FROM {{ ref('stg_customers') }} as c
left join orders_cte as o using (customer_id)
