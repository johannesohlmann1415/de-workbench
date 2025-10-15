{{ config(materialized='table') }}

-- Mini-Datensatz inline erstellen
select * from unnest([
  struct('alpha' as word, 1 as total_count),
  struct('beta'  as word, 2 as total_count),
  struct('gamma' as word, 3 as total_count)
])