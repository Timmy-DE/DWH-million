{{
  config(
    materialized='incremental',
    unique_key='customer_hash_key',
    engine='ReplacingMergeTree()',
    order_by='customer_hash_key',
    incremental_strategy='delete+insert'
  )
}}

SELECT DISTINCT
  customer_id,
  customer_hash_key,
  load_date,
  record_source
FROM {{ ref('raw_transactions') }}
