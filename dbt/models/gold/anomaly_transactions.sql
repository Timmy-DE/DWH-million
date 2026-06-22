{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='transaction_id'
  )
}}

SELECT
    l.transaction_id,
    l.link_hash_key,


    c.customer_id,
    ci.customer_name,
    ci.customer_email,
    ci.customer_country,


    s.amount,
    s.currency,
    s.status,
    s.category,


    CASE
        WHEN s.amount > 5000                        THEN 'large_amount'
        WHEN s.status = 'failed' AND s.amount > 100 THEN 'large_failed'
        WHEN s.is_suspicious = 1                    THEN 'flagged_by_system'
        ELSE 'unknown'
    END AS anomaly_reason,

    s.load_date

FROM {{ ref('link_transaction') }} l
JOIN {{ ref('sat_transaction_details') }} s
    ON l.link_hash_key = s.link_hash_key
JOIN {{ ref('hub_customer') }} c
    ON l.customer_hash_key = c.customer_hash_key
JOIN {{ ref('sat_customer_info') }} ci
    ON c.customer_hash_key = ci.customer_hash_key

    WHERE
        s.is_suspicious = 1
        OR s.amount > 5000
        OR (s.status = 'failed' AND s.amount > 100)

ORDER BY
    s.amount DESC
