-- models/staging/stg_customers.sql
-- Cleans raw CDC customer records. Converts epoch ms dates to proper timestamps.

with source as (
    select * from {{ source('raw', 'customers') }}
    where coalesce(_cdc_op, 'c') != 'd'
),

cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name                       as full_name,
        lower(trim(email))                                   as email,
        phone_number,
        gender,
        age,
        state,
        city,
        zip_code,
        -- Debezium sends DATE as epoch days; convert to date
        (date '1970-01-01' + (signup_date * interval '1 day'))::date  as signup_date,
        case when churn_date is not null
             then (date '1970-01-01' + (churn_date * interval '1 day'))::date
        end                                                  as churn_date,
        (is_churned = 1)                                     as is_churned,
        churn_reason,
        to_timestamp(created_at / 1000.0)                   as created_at,
        to_timestamp(updated_at / 1000.0)                   as updated_at,
        _cdc_op                                              as cdc_op,
        to_timestamp(_cdc_ts  / 1000.0)                     as cdc_event_at,
        _loaded_at
    from source
)

select * from cleaned
