{% snapshot snap_customers %}
    {{
        config(
            target_shema='snapshot',
            unique_key='customer_id',
            strategy='check',
            check_cols=['customer_name', 'country', 'vip_flag', 'recent_30d_flag'],
            invalidate_hard_deletes=true
        )
    }}

select
    customer_id,
    customer_name,
    country,
    vip_flag,
    recent_30d_flag,
  from {{ ref('dim_customer') }}
{% endsnapshot %}