{{
  config(
    materialized='incremental',
    unique_key=['link_hash_key', 'load_date'],
    engine='ReplacingMergeTree()',
    order_by=['link_hash_key', 'load_date']
  )
}}

WITH source AS (
    SELECT
        transaction_hash_key AS link_hash_key,
        amount,
        currency,
        status,
        category,
        is_suspicious,
        load_date,
        record_source,
        MD5(concat(
            coalesce(toString(amount), ''),
            coalesce(currency, ''),
            coalesce(status, ''),
            coalesce(category, ''),
            coalesce(toString(is_suspicious), '')
        )) AS hashdiff
    FROM {{ ref('raw_transactions') }}
    {{ incremental_filter('load_date') }}
),

deduplicated AS (
    SELECT
        link_hash_key,
        amount,
        currency,
        status,
        category,
        is_suspicious,
        hashdiff,
        load_date,
        record_source,
        row_number() OVER (
            PARTITION BY link_hash_key, hashdiff
            ORDER BY load_date ASC
        ) AS rn
    FROM source
)

SELECT
    link_hash_key,
    amount,
    currency,
    status,
    category,
    is_suspicious,
    hashdiff,
    load_date,
    record_source
FROM deduplicated
WHERE rn = 1
