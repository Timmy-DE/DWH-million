{{
  config(
    materialized='incremental',
    unique_key='account_hash_key',
    engine='ReplacingMergeTree()',
    order_by='account_hash_key',
    incremental_strategy='delete+insert'  )
}}

SELECT DISTINCT
  account_hash_key,
  account_id,
  load_date,
  record_source
FROM {{ ref('raw_transactions') }}
