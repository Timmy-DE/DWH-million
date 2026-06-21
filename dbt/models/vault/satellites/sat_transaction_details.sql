{{
  config(
    materialized='incremental',
    unique_key=['link_hash_key', 'load_date'],
    engine='ReplacingMergeTree()',
    order_by=['link_hash_key', 'load_date']
  )
}}
