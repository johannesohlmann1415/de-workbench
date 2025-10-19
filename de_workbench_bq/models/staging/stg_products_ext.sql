{{ config(materialized='view') }}
With json_extract as (
    Select
    safe_cast(trim(cast(product_id as string)) as int64) as product_id,
    Nullif(trim(safe_cast(product_name as string)), '') as product_name,
    Nullif(lower(trim((safe_cast(category as string)))),'') as category,
    SAFE.Parse_JSON(NULLIF(TRIM(CAST(tags AS STRING)), '')) as tags_json,
    SAFE.Parse_JSON(NULLIF(TRIM(CAST(attributes AS STRING)), '')) as attributes_json
    FROM {{ ref('products_ext') }}
)
Select
*,
CASE
  WHEN tags_json IS NULL THEN NULL
  ELSE ARRAY(
    SELECT JSON_VALUE(elem)
    FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(tags_json), [])) AS elem
    WHERE JSON_VALUE(elem) IS NOT NULL
  )
END AS tags
FROM json_extract