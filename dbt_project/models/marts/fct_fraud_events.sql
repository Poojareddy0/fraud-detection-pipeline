{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    contract={"enforced": true}
  )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- only process new/updated records since last run
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

enriched as (
    select
        t.transaction_id,
        t.customer_id,
        t.account_id,
        c.customer_segment,
        c.risk_tier                                                    as customer_risk_tier,
        t.amount_usd,
        t.merchant_id,
        t.merchant_category,
        t.merchant_country_code,
        t.card_present,
        t.transaction_ts,
        date_trunc('hour', t.transaction_ts)                           as transaction_hour,
        date_trunc('day',  t.transaction_ts)                           as transaction_date,
        extract('dayofweek' from t.transaction_ts)                     as day_of_week,
        extract('hour'      from t.transaction_ts)                     as hour_of_day,
        -- fraud signals
        t.z_score,
        t.fraud_score,
        t.is_suspicious,
        t.fraud_rules_fired,
        -- derived flags from rules string
        contains(t.fraud_rules_fired, 'z_score')                       as z_score_triggered,
        contains(t.fraud_rules_fired, 'high_risk_country')             as high_risk_country_triggered,
        contains(t.fraud_rules_fired, 'velocity')                      as velocity_triggered,
        contains(t.fraud_rules_fired, 'amount_threshold')              as threshold_triggered,
        -- off-hours flag (transactions between midnight and 5am local)
        case when extract('hour' from t.transaction_ts) between 0 and 4
             then true else false end                                   as is_off_hours,
        -- audit
        t.ip_address,
        t.device_fingerprint,
        t.latitude,
        t.longitude,
        t.ingested_at,
        t.dbt_loaded_at,
        current_timestamp()                                            as mart_updated_at

    from transactions t
    left join customers c
           on t.customer_id = c.customer_id
          and c.is_current = true
)

select * from enriched
