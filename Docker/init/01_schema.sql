-- Telecom Customer Churn Database Schema
-- Auto-executed on first MySQL container startup

CREATE DATABASE IF NOT EXISTS telecom_churn;
USE telecom_churn;

CREATE TABLE IF NOT EXISTS customers (
    customer_id       VARCHAR(20)  PRIMARY KEY,
    first_name        VARCHAR(50)  NOT NULL,
    last_name         VARCHAR(50)  NOT NULL,
    email             VARCHAR(100) UNIQUE,
    phone_number      VARCHAR(20),
    gender            ENUM('Male','Female','Other'),
    age               INT,
    state             VARCHAR(50),
    city              VARCHAR(100),
    zip_code          VARCHAR(10),
    signup_date       DATE         NOT NULL,
    churn_date        DATE         DEFAULT NULL,
    is_churned        TINYINT(1)   NOT NULL DEFAULT 0,
    churn_reason      VARCHAR(255) DEFAULT NULL,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subscription_plans (
    plan_id           INT          AUTO_INCREMENT PRIMARY KEY,
    plan_name         VARCHAR(100) NOT NULL,
    plan_type         ENUM('Prepaid','Postpaid') NOT NULL,
    monthly_fee       DECIMAL(8,2) NOT NULL,
    data_limit_gb     DECIMAL(6,2) DEFAULT NULL,
    call_minutes      INT          DEFAULT NULL,
    sms_limit         INT          DEFAULT NULL,
    international     TINYINT(1)   DEFAULT 0,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_subscriptions (
    subscription_id   INT          AUTO_INCREMENT PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    plan_id           INT          NOT NULL,
    start_date        DATE         NOT NULL,
    end_date          DATE         DEFAULT NULL,
    is_active         TINYINT(1)   NOT NULL DEFAULT 1,
    contract_length   INT,
    auto_renew        TINYINT(1)   DEFAULT 1,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (plan_id)     REFERENCES subscription_plans(plan_id)
);

CREATE TABLE IF NOT EXISTS usage_records (
    usage_id          INT          AUTO_INCREMENT PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    billing_month     DATE         NOT NULL,
    data_used_gb      DECIMAL(6,2) DEFAULT 0,
    call_minutes_used INT          DEFAULT 0,
    sms_sent          INT          DEFAULT 0,
    intl_calls_min    INT          DEFAULT 0,
    roaming_days      INT          DEFAULT 0,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    UNIQUE KEY uq_customer_month (customer_id, billing_month)
);

CREATE TABLE IF NOT EXISTS billing_records (
    bill_id           INT          AUTO_INCREMENT PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    billing_month     DATE         NOT NULL,
    base_charge       DECIMAL(8,2) NOT NULL,
    overage_charge    DECIMAL(8,2) DEFAULT 0,
    discount_amount   DECIMAL(8,2) DEFAULT 0,
    total_amount      DECIMAL(8,2) NOT NULL,
    payment_status    ENUM('Paid','Pending','Overdue','Waived') DEFAULT 'Pending',
    payment_date      DATE         DEFAULT NULL,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id         INT          AUTO_INCREMENT PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    ticket_date       DATE         NOT NULL,
    category          ENUM('Billing','Network','Device','Account','Other') NOT NULL,
    priority          ENUM('Low','Medium','High','Critical') DEFAULT 'Medium',
    status            ENUM('Open','In Progress','Resolved','Closed') DEFAULT 'Open',
    resolution_days   INT          DEFAULT NULL,
    satisfaction_score INT         DEFAULT NULL,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS network_quality (
    nq_id             INT          AUTO_INCREMENT PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    event_date        DATE         NOT NULL,
    signal_strength   DECIMAL(4,1),
    dropped_calls     INT          DEFAULT 0,
    data_speed_mbps   DECIMAL(6,2),
    outage_minutes    INT          DEFAULT 0,
    created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Seed subscription plans
INSERT IGNORE INTO subscription_plans
    (plan_name, plan_type, monthly_fee, data_limit_gb, call_minutes, sms_limit, international)
VALUES
    ('Basic Prepaid',      'Prepaid',   9.99,  2.0,  100,  100, 0),
    ('Standard Prepaid',   'Prepaid',  19.99,  5.0,  300,  300, 0),
    ('Premium Prepaid',    'Prepaid',  29.99, 15.0, NULL,  500, 0),
    ('Basic Postpaid',     'Postpaid', 34.99, 10.0,  500,  500, 0),
    ('Standard Postpaid',  'Postpaid', 49.99, 25.0, 1000, 1000, 0),
    ('Premium Postpaid',   'Postpaid', 69.99, NULL, NULL, NULL,  1),
    ('Business Plan',      'Postpaid', 99.99, NULL, NULL, NULL,  1),
    ('Student Plan',       'Prepaid',  14.99,  8.0,  500,  500, 0);
