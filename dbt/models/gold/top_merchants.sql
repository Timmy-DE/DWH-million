{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='total_amount'
  )
}}

SELECT
    h.merchant_id,
    h.merchant_name,
    s.category,
    count(*)                        AS transaction_count,
    sum(s.amount)                   AS total_amount,
    avg(s.amount)                   AS avg_amount,
    countIf(s.status = 'completed') AS completed_count,
    countIf(s.status = 'failed')    AS failed_count,
    countIf(s.is_suspicious)        AS suspicious_count
FROM {{ ref('link_transaction') }} l
JOIN {{ ref('sat_transaction_details') }} s
    ON l.link_hash_key = s.link_hash_key
JOIN {{ ref('hub_merchant') }} h
    ON l.merchant_hash_key = h.merchant_hash_key
GROUP BY
    h.merchant_id,
    h.merchant_name,
    s.category
ORDER BY
    total_amount DESC
