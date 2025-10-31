{{ config(materialized='table') }}
Select
    product_id, 
    product_name, 
    category, 
    tags,
    Nullif(lower(trim(SAFE_CAST(JSON_VALUE(attributes_json, '$.color') as STRING))),'') as color
FROM {{ ref('stg_products_ext') }}
