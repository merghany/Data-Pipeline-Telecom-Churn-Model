# Telecom Customer Churn — MySQL + Docker Setup

## Project Structure

```
.
├── docker-compose.yml          # MySQL 8.0 + Adminer services
├── init/
│   └── 01_schema.sql           # Auto-run on first startup (tables + plan seed)
├── generate_churn_data.py      # Python data generator
└── README.md
```

## Quick Start

### 1. Start the database
```bash
docker compose up -d
```
MySQL will be ready in ~30 seconds. The schema and seed plans are created automatically.

### 2. Install Python dependencies
```bash
pip install mysql-connector-python faker python-dateutil
```

### 3. Generate sample data
```bash
# Default: 500 customers, 1 year of data
python generate_churn_data.py

# Custom number of customers
python generate_churn_data.py --customers 2000
```

### 4. Browse data via Adminer (Web UI)
Open http://localhost:8080 in your browser:
| Field    | Value          |
|----------|----------------|
| System   | MySQL          |
| Server   | mysql          |
| Username | churn_user     |
| Password | churn_pass     |
| Database | telecom_churn  |

---

## Database Schema

| Table                    | Description                                      |
|--------------------------|--------------------------------------------------|
| `customers`              | Customer profiles, signup & churn dates          |
| `subscription_plans`     | 8 pre-seeded plans (Prepaid / Postpaid)          |
| `customer_subscriptions` | Which plan each customer is on                   |
| `usage_records`          | Monthly data, calls, SMS per customer            |
| `billing_records`        | Monthly invoices with payment status             |
| `support_tickets`        | Customer service interactions                    |
| `network_quality`        | Monthly signal, speed, and outage metrics        |

### Churn simulation details
- ~25% of customers are marked as churned
- Churned customers show declining usage in the last 1–2 months
- Churned customers generate more support tickets
- Customers who churned due to "Poor network quality" have worse signal/speed metrics

---

## Connection Details

| Parameter | Value          |
|-----------|----------------|
| Host      | localhost      |
| Port      | 3306           |
| Database  | telecom_churn  |
| User      | churn_user     |
| Password  | churn_pass     |
| Root PW   | rootpassword   |

---

## Useful Queries

```sql
-- Churn rate by plan type
SELECT sp.plan_type,
       COUNT(*) AS total,
       SUM(c.is_churned) AS churned,
       ROUND(100.0 * SUM(c.is_churned) / COUNT(*), 1) AS churn_pct
FROM customers c
JOIN customer_subscriptions cs ON c.customer_id = cs.customer_id
JOIN subscription_plans sp ON cs.plan_id = sp.plan_id
GROUP BY sp.plan_type;

-- Average support tickets: churned vs retained
SELECT is_churned,
       ROUND(AVG(ticket_count), 2) AS avg_tickets
FROM (
    SELECT c.customer_id, c.is_churned,
           COUNT(t.ticket_id) AS ticket_count
    FROM customers c
    LEFT JOIN support_tickets t ON c.customer_id = t.customer_id
    GROUP BY c.customer_id, c.is_churned
) sub
GROUP BY is_churned;

-- Monthly revenue trend
SELECT billing_month,
       SUM(total_amount) AS revenue,
       COUNT(*) AS invoices
FROM billing_records
GROUP BY billing_month
ORDER BY billing_month;
```

## Stop / Clean Up

```bash
# Stop containers
docker compose down

# Remove containers + data volume
docker compose down -v
```
