{{
  config(
    materialized='incremental',
    unique_key=['customer_hash_key', 'load_date'],
    engine='ReplacingMergeTree()',
    order_by=['customer_hash_key', 'load_date'],
    incremental_strategy='delete+insert'
  )
}}

WITH source AS (
    SELECT
        customer_hash_key,
        customer_name,
        customer_email,
        customer_country,
        load_date,
        record_source,

        MD5(concat(
            coalesce(customer_name, ''),
            coalesce(customer_email, ''),
            coalesce(customer_country, '')
        )) AS hashdiff
    FROM {{ ref('raw_transactions') }}
),
deduplicated AS (
    SELECT
        customer_hash_key,
        customer_name,
        customer_email,
        customer_country,
        hashdiff,
        load_date,
        record_source,
        row_number() OVER (
            PARTITION BY customer_hash_key, hashdiff
            ORDER BY load_date ASC
        ) AS rn
    FROM source
)

SELECT
    customer_hash_key,
    customer_name,
    customer_email,
    customer_country,
    hashdiff,
    load_date,
    record_source
FROM deduplicated
WHERE rn = 1
