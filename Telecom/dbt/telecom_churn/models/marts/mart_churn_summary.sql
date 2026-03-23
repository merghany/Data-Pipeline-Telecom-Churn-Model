-- models/marts/mart_churn_summary.sql
-- Executive-level churn KPI summary.
-- Aggregated by plan type, state, and month — ready for BI dashboards.

with revenue as (
    select * from {{ ref('fct_monthly_revenue') }}
),

churn as (
    select * from {{ ref('fct_churn_events') }}
),

-- Monthly cohort metrics
monthly_revenue as (
    select
        billing_month,
        plan_type,
        state,
        count(distinct customer_id)             as active_customers,
        sum(total_amount)                       as total_revenue,
        avg(total_amount)                       as arpu,
        sum(case when is_overdue then 1 else 0 end) as overdue_count,
        sum(overage_charge)                     as total_overage_revenue,
        sum(case when is_churn_month then 1 else 0 end) as churned_customers,
        sum(case when is_churn_month then total_amount else 0 end) as lost_revenue
    from revenue
    group by billing_month, plan_type, state
),

final as (
    select
        billing_month,
        plan_type,
        state,
        active_customers,
        churned_customers,
        round(total_revenue, 2)                 as total_revenue,
        round(arpu, 2)                          as arpu,
        round(total_overage_revenue, 2)         as overage_revenue,
        overdue_count,
        round(lost_revenue, 2)                  as lost_revenue,

        -- Churn rate %
        round(
            100.0 * churned_customers / nullif(active_customers, 0), 2
        )                                       as churn_rate_pct,

        -- Overdue rate %
        round(
            100.0 * overdue_count / nullif(active_customers, 0), 2
        )                                       as overdue_rate_pct

    from monthly_revenue
    order by billing_month desc, total_revenue desc
)

select * from final
