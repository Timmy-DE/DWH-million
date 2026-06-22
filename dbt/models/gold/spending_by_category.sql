{{
   config(
      materialized='table',
      engine='MergeTree()',
      order_by='category'
   )
}}

SELECT
    h.merchant_hash_key,
    s.category,
    count(*)                    AS transaction_count,
    sum(s.amount)               AS total_amount,
    avg(s.amount)               AS avg_amount,
    min(s.amount)               AS min_amount,
    max(s.amount)               AS max_amount,
    countIf(s.is_suspicious)    AS suspicious_count
FROM {{ ref('link_transaction') }} l
JOIN {{ ref('sat_transaction_details') }} s
    ON l.link_hash_key = s.link_hash_key
JOIN {{ ref('hub_merchant') }} h
    ON l.merchant_hash_key = h.merchant_hash_key
GROUP BY
    h.merchant_hash_key,
    s.category
ORDER BY
    total_amount DESC
