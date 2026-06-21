{{
  config(
    materialized='incremental',
    unique_key='link_hash_key',
    engine='ReplacingMergeTree()',
    order_by='link_hash_key'
  )
}}

SELECT DISTINCT
    transaction_hash_key    AS link_hash_key,
    customer_hash_key,
    merchant_hash_key,
    account_hash_key,
    transaction_id,
    load_date,
    record_source
FROM {{ ref('raw_transactions') }}

{{ incremental_filter('load_date') }}
