-- models/intermediate/int_customers_enriched.sql
-- Joins customers with their current subscription plan.
-- One row per customer.

with customers as (
    select * from {{ ref('stg_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_customer_subscriptions') }}
    where is_active = true
),

plans as (
    select * from {{ ref('stg_subscription_plans') }}
),

-- Each customer gets their most recent active subscription
latest_sub as (
    select distinct on (customer_id)
        customer_id,
        plan_id,
        start_date           as sub_start_date,
        end_date             as sub_end_date,
        is_active,
        contract_length,
        auto_renew
    from subscriptions
    order by customer_id, start_date desc
),

joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.gender,
        c.age,
        c.state,
        c.city,
        c.signup_date,
        c.churn_date,
        c.is_churned,
        c.churn_reason,
        -- Tenure in days
        extract(day from
            coalesce(c.churn_date, current_date) - c.signup_date
        )::int                                              as tenure_days,
        -- Plan details
        p.plan_id,
        p.plan_name,
        p.plan_type,
        p.monthly_fee,
        p.data_limit_gb,
        p.is_international,
        p.contract_length,
        ls.auto_renew,
        ls.sub_start_date,
        c.updated_at,
        c.cdc_op,
        c.cdc_event_at,
        c._loaded_at
    from customers c
    left join latest_sub ls using (customer_id)
    left join plans      p  using (plan_id)
)

select * from joined
