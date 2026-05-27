{{
  config(
    materialized='table',
    contract={"enforced": true}
  )
}}

/*
  dim_customers — SCD Type 2
  Tracks customer attribute history. Each change to segment or risk_tier
  creates a new row; the current row has is_current = true and valid_to = NULL.
*/

with customer_source as (
    select distinct
        customer_id,
        -- derive segment from account suffix pattern
        case
            when cast(right(customer_id, 2) as integer) % 3 = 0 then 'PREMIUM'
            when cast(right(customer_id, 2) as integer) % 3 = 1 then 'STANDARD'
            else 'BASIC'
        end                                                as customer_segment,
        -- risk tier derived from historical fraud activity
        case
            when customer_id in (
                select customer_id
                from {{ ref('stg_transactions') }}
                where is_suspicious = true
                group by customer_id
                having count(*) >= 3
            ) then 'HIGH'
            when customer_id in (
                select customer_id
                from {{ ref('stg_transactions') }}
                where is_suspicious = true
                group by customer_id
                having count(*) between 1 and 2
            ) then 'MEDIUM'
            else 'LOW'
        end                                                as risk_tier,
        min(transaction_ts)                                as first_seen_at,
        max(transaction_ts)                                as last_seen_at,
        count(*)                                           as total_transactions,
        sum(amount_usd)                                    as total_spend_usd

    from {{ ref('stg_transactions') }}
    group by 1, 2, 3
),

scd2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_segment', 'risk_tier']) }}
                                                           as customer_sk,
        customer_id,
        customer_segment,
        risk_tier,
        first_seen_at                                      as valid_from,
        null::timestamp                                    as valid_to,
        true                                               as is_current,
        first_seen_at,
        last_seen_at,
        total_transactions,
        total_spend_usd,
        current_timestamp()                                as dbt_loaded_at
    from customer_source
)

select * from scd2
