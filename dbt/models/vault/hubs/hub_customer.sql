{{
  config(
    materialized='incremental',
    unique_key='customer_hash_key',
    engine='ReplacingMergeTree()',
    order_by='customer_hash_key'
  )
}}

SELECT DISTINCT
  customer_id,
  customer_hash_key,
  load_date,
  record_source
FROM {{ source('default', ref('raw_transactions')) }}

{{ incremental_filter('load_date') }}
