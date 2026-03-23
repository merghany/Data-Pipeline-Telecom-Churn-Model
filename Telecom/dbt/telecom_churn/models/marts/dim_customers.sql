-- models/marts/dim_customers.sql
-- Master customer dimension table — one row per customer.
-- Combines enriched profile, support signals, and network signals.

with customers as (
    select * from {{ ref('int_customers_enriched') }}
),

support as (
    select * from {{ ref('int_customer_support_summary') }}
),

network as (
    select * from {{ ref('int_customer_network_summary') }}
),

final as (
    select
        -- ── Identity ─────────────────────────────────────────
        c.customer_id,
        c.full_name,
        c.email,
        c.gender,
        c.age,

        -- Age band
        case
            when c.age < 25 then '18-24'
            when c.age < 35 then '25-34'
            when c.age < 45 then '35-44'
            when c.age < 55 then '45-54'
            when c.age < 65 then '55-64'
            else '65+'
        end                                                 as age_band,

        -- ── Geography ────────────────────────────────────────
        c.state,
        c.city,

        -- ── Subscription ─────────────────────────────────────
        c.signup_date,
        c.churn_date,
        c.is_churned,
        c.churn_reason,
        c.tenure_days,
        round(c.tenure_days / 30.0, 1)                     as tenure_months,
        c.plan_id,
        c.plan_name,
        c.plan_type,
        c.monthly_fee,
        c.data_limit_gb,
        c.is_international,
        c.contract_length,
        c.auto_renew,

        -- ── Support signals ───────────────────────────────────
        coalesce(s.total_tickets, 0)                        as total_tickets,
        coalesce(s.open_tickets, 0)                         as open_tickets,
        coalesce(s.critical_tickets, 0)                     as critical_tickets,
        coalesce(s.high_tickets, 0)                         as high_tickets,
        coalesce(s.avg_resolution_days, 0)                  as avg_resolution_days,
        coalesce(s.avg_satisfaction, 0)                     as avg_satisfaction,
        coalesce(s.tickets_last_90d, 0)                     as tickets_last_90d,
        coalesce(s.priority_score, 0)                       as support_priority_score,
        s.last_ticket_date,

        -- ── Network signals ───────────────────────────────────
        coalesce(n.avg_signal_strength, -70)                as avg_signal_strength,
        coalesce(n.avg_data_speed_mbps, 50)                 as avg_data_speed_mbps,
        coalesce(n.total_outage_minutes, 0)                 as total_outage_minutes,
        coalesce(n.total_dropped_calls, 0)                  as total_dropped_calls,
        coalesce(n.poor_quality_pct, 0)                     as poor_network_pct,
        coalesce(n.poor_quality_readings, 0)                as poor_network_months,

        -- ── Churn risk score (0–100) ──────────────────────────
        -- Weighted heuristic combining 5 churn signals
        least(100, greatest(0,
            -- Signal 1: high ticket volume in last 90 days (max 25 pts)
            least(25, coalesce(s.tickets_last_90d, 0) * 5)
            -- Signal 2: poor network experience (max 25 pts)
            + least(25, coalesce(n.poor_quality_pct, 0) * 0.5)
            -- Signal 3: low satisfaction score (max 20 pts)
            + case when coalesce(s.avg_satisfaction, 5) < 3
                   then (3 - coalesce(s.avg_satisfaction, 5)) * 10
                   else 0 end
            -- Signal 4: short tenure (max 15 pts)
            + case when c.tenure_days < 90  then 15
                   when c.tenure_days < 180 then 8
                   else 0 end
            -- Signal 5: overdue payments (proxy via overage/plan type)
            + case when c.plan_type = 'Prepaid' then 5 else 0 end
        ))::int                                             as churn_risk_score,

        -- ── Metadata ─────────────────────────────────────────
        c.updated_at,
        c._loaded_at
    from customers c
    left join support s using (customer_id)
    left join network n using (customer_id)
)

select * from final
