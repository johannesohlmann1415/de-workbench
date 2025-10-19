{{ config(materialized='view') }}
With json_extract as (
    Select
    safe_cast(trim(cast(order_id as string)) as int64) as order_id,
    safe_cast(trim(cast(customer_id as string)) as int64) as customer_id,
    safe_cast(order_date as date) as order_date,
    lower(trim(safe_cast(status as string))) as status_raw,
    safe_cast(trim(cast(total_amount as string)) as NUMERIC) as total_amount,
    SAFE.Parse_JSON(NULLIF(TRIM(CAST(coupon_codes AS STRING)), '')) as coupon_codes_json,
    SAFE.Parse_JSON(NULLIF(TRIM(CAST(meta AS STRING)), '')) as meta_json
    FROM {{ ref('orders_ext') }}
),
array_extract as (
    SELECT
    order_id,
    customer_id,
    order_date,
    CASE
        WHEN status_raw in ('completed','cancelled','refunded')
        THEN status_raw
        ElSE Null
    END AS status,
    total_amount,
    coupon_codes_json,
    CASE
        WHEN coupon_codes_json IS NULL THEN NULL
        ELSE ARRAY(
            SELECT JSON_VALUE(elem)
            FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(coupon_codes_json), [])) AS elem
            WHERE JSON_VALUE(elem) IS NOT NULL
    )
    END AS coupon_codes,
    meta_json,
    FROM json_extract
)
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    coupon_codes_json,
    coupon_codes,
    ARRAY_LENGTH(coupon_codes) as coupon_count,
    coupon_codes[SAFE_OFFSET(0)] as first_coupon,
    meta_json,
    json_value(meta_json, '$.device') as device,
    json_value(meta_json, '$.payment.method') as payment_method,
    json_value(meta_json, '$.shipping.method') as shipping_method,
    safe_cast(trim(cast(json_value(meta_json, '$.shipping.price') as string)) as NUMERIC) as shipping_price
from array_extract
