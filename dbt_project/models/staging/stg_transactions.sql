{{
  config(
    materialized='view',
    contract={"enforced": true}
  )
}}

with source as (
    select * from {{ source('bronze', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        customer_id,
        account_id,
        cast(amount as numeric(18, 2))                          as amount_usd,
        merchant_id,
        upper(trim(merchant_category))                          as merchant_category,
        upper(trim(merchant_country))                           as merchant_country_code,
        card_present,
        cast(transaction_ts as timestamp)                       as transaction_ts,
        ip_address,
        device_fingerprint,
        cast(latitude as float)                                 as latitude,
        cast(longitude as float)                                as longitude,
        cast(z_score as float)                                  as z_score,
        cast(fraud_score as integer)                            as fraud_score,
        is_suspicious,
        coalesce(fraud_rules_fired, '')                         as fraud_rules_fired,
        ingested_at,
        -- audit columns
        current_timestamp()                                     as dbt_loaded_at

    from source

    -- exclude obviously corrupt records that slipped through
    where transaction_id is not null
      and customer_id    is not null
      and amount_usd     > 0
      and length(merchant_country_code) = 2
)

select * from renamed
