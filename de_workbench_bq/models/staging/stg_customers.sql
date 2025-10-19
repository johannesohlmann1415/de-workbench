{{ config(materialized='view') }}
Select
safe_cast(trim(cast(customer_id as string)) as int64) as customer_id, -- (INT64) – Primary Key, getrimmt/valide.
Nullif(trim(safe_cast(name as string)), '') as customer_name, -- (STRING) unverändert; führende/trailing Spaces entfernen.
Nullif(upper(trim((safe_cast(country as string)))),'') as country, -- (STRING) – Uppercase ISO (z. B. DE, US).
safe_cast(signup_date as date) as signup_date
FROM {{ ref('customers') }}