{{
  config(
    materialized='incremental',
    unique_key='merchant_hash_key',
    engine='ReplacingMergeTree()',
    order_by='merchant_hash_key',
    incremental_strategy='delete+insert'
  )
}}

SELECT DISTINCT
  merchant_id,
  merchant_hash_key,
  merchant_name,
  category,
  load_date,
  record_source
FROM {{ ref('raw_transactions') }}
