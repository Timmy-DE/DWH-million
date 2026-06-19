{{
  config(
    materialized='incremental',
    unique_key='account_hash_key',
    engine='ReplacingMergeTree()',
    order_by='account_hash_key'
  )
}}

SELECT DISTINCT
  account_id,
  account_name,
  account_hash_key,
  load_date,
  record_source
FROM {{ source('default', ref('raw_transactions')) }}

{{ incremental_filter('load_date') }}
