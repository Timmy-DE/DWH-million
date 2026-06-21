SELECT *
FROM {{ source('default', 'raw_transactions') }}
