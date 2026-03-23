-- PostgreSQL init script
-- Creates raw landing tables (written by Kafka Connect JDBC Sink)
-- and empty schemas for dbt output layers

-- ── dbt output schemas ────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- ── raw.customers ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id        VARCHAR(20)  PRIMARY KEY,
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    email              VARCHAR(100),
    phone_number       VARCHAR(20),
    gender             VARCHAR(10),
    age                INT,
    state              VARCHAR(50),
    city               VARCHAR(100),
    zip_code           VARCHAR(10),
    signup_date        BIGINT,
    churn_date         BIGINT,
    is_churned         SMALLINT     DEFAULT 0,
    churn_reason       VARCHAR(255),
    created_at         BIGINT,
    updated_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- ── raw.subscription_plans ────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.subscription_plans (
    plan_id            INT          PRIMARY KEY,
    plan_name          VARCHAR(100),
    plan_type          VARCHAR(20),
    monthly_fee        NUMERIC(10,4),
    data_limit_gb      NUMERIC(10,4),
    call_minutes       INT,
    sms_limit          INT,
    international      SMALLINT,
    created_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- ── raw.customer_subscriptions ────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.customer_subscriptions (
    subscription_id    INT          PRIMARY KEY,
    customer_id        VARCHAR(20),
    plan_id            INT,
    start_date         BIGINT,
    end_date           BIGINT,
    is_active          SMALLINT,
    contract_length    INT,
    auto_renew         SMALLINT,
    created_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- ── raw.usage_records ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.usage_records (
    usage_id           INT          PRIMARY KEY,
    customer_id        VARCHAR(20),
    billing_month      BIGINT,
    data_used_gb       NUMERIC(10,4),
    call_minutes_used  INT,
    sms_sent           INT,
    intl_calls_min     INT,
    roaming_days       INT,
    created_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- ── raw.billing_records ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.billing_records (
    bill_id            INT          PRIMARY KEY,
    customer_id        VARCHAR(20),
    billing_month      BIGINT,
    base_charge        NUMERIC(10,4),
    overage_charge     NUMERIC(10,4),
    discount_amount    NUMERIC(10,4),
    total_amount       NUMERIC(10,4),
    payment_status     VARCHAR(20),
    payment_date       BIGINT,
    created_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- ── raw.support_tickets ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.support_tickets (
    ticket_id          INT          PRIMARY KEY,
    customer_id        VARCHAR(20),
    ticket_date        BIGINT,
    category           VARCHAR(20),
    priority           VARCHAR(20),
    status             VARCHAR(20),
    resolution_days    INT,
    satisfaction_score INT,
    created_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);

-- ── raw.network_quality ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.network_quality (
    nq_id              INT          PRIMARY KEY,
    customer_id        VARCHAR(20),
    event_date         BIGINT,
    signal_strength    NUMERIC(6,2),
    dropped_calls      INT,
    data_speed_mbps    NUMERIC(10,4),
    outage_minutes     INT,
    created_at         BIGINT,
    _cdc_op            VARCHAR(10),
    _cdc_ts            BIGINT,
    _loaded_at         TIMESTAMPTZ  DEFAULT NOW()
);
