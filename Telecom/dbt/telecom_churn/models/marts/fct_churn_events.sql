-- models/marts/fct_churn_events.sql
-- One row per churned customer — complete churn profile for analysis and ML.

with customers as (
    select * from {{ ref('dim_customers') }}
    where is_churned = true
),

-- Average revenue per month during the customer's lifetime
lifetime_revenue as (
    select
        customer_id,
        sum(total_amount)                               as total_revenue,
        avg(total_amount)                               as avg_monthly_revenue,
        count(distinct billing_month)                   as active_months,
        sum(case when is_overdue then 1 else 0 end)     as overdue_months,
        avg(data_used_gb)                               as avg_monthly_data_gb,
        avg(call_minutes_used)                          as avg_monthly_minutes
    from {{ ref('fct_monthly_revenue') }}
    group by customer_id
),

final as (
    select
        -- ── Event identity ────────────────────────────────────
        c.customer_id,
        c.churn_date,
        c.churn_reason,

        -- ── Customer profile at churn ─────────────────────────
        c.full_name,
        c.gender,
        c.age,
        c.age_band,
        c.state,
        c.plan_name,
        c.plan_type,
        c.monthly_fee,
        c.tenure_days,
        c.tenure_months,
        c.contract_length,
        c.auto_renew,
        c.is_international,

        -- ── Revenue impact ────────────────────────────────────
        coalesce(r.total_revenue, 0)                    as lifetime_revenue,
        coalesce(r.avg_monthly_revenue, 0)              as avg_monthly_revenue,
        coalesce(r.active_months, 0)                    as active_months,
        coalesce(r.overdue_months, 0)                   as overdue_months,
        coalesce(r.avg_monthly_data_gb, 0)              as avg_monthly_data_gb,
        coalesce(r.avg_monthly_minutes, 0)              as avg_monthly_minutes,

        -- Estimated 12-month revenue lost
        round(coalesce(r.avg_monthly_revenue, 0) * 12, 2)  as estimated_annual_revenue_lost,

        -- ── Churn signals ─────────────────────────────────────
        c.total_tickets,
        c.critical_tickets,
        c.avg_satisfaction,
        c.tickets_last_90d,
        c.support_priority_score,
        c.avg_signal_strength,
        c.avg_data_speed_mbps,
        c.poor_network_pct,
        c.total_outage_minutes,
        c.churn_risk_score,

        -- Churn reason category (broader grouping)
        case
            when c.churn_reason ilike '%price%' or c.churn_reason ilike '%competitor%'
                then 'Price / Competitor'
            when c.churn_reason ilike '%network%' or c.churn_reason ilike '%quality%'
                then 'Network Quality'
            when c.churn_reason ilike '%service%' or c.churn_reason ilike '%support%'
                then 'Customer Service'
            when c.churn_reason ilike '%relocation%' or c.churn_reason ilike '%move%'
                then 'Relocation'
            when c.churn_reason ilike '%financial%' or c.churn_reason ilike '%billing%'
                then 'Financial'
            else 'Other'
        end                                             as churn_category,

        -- ── Metadata ─────────────────────────────────────────
        c._loaded_at
    from customers c
    left join lifetime_revenue r using (customer_id)
)

select * from final
