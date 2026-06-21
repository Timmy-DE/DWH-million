{{
  config(
    materialized='incremental',
    unique_key='account_hash_key',
    engine='ReplacingMergeTree()',
    order_by='account_hash_key'
  )
}}

SELECT DISTINCT
  account_hash_key,
  account_id,
  load_date,
  record_source
FROM {{ ref('raw_transactions') }}

{{ incremental_filter('load_date') }}
