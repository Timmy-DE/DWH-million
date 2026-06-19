{{
  config(
    materialized='incremental',
    unique_key='merchant_hash_key',
    engine='ReplacingMergeTree()',
    order_by='merchant_hash_key'
  )
}}

SELECT DISTINCT
  merchant_id,
  merchant_hash_key,
  merchant_name,
  load_date,
  record_source
FROM {{ source('default', ref('raw_transactions')) }}

{{ incremental_filter('load_date') }}
