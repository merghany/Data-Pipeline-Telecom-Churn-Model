#!/usr/bin/env python3
"""
Telecom Customer Churn — Sample Data Generator
Generates 1 year of realistic data and loads it into MySQL.

Dependencies:
    pip install mysql-connector-python faker

Usage:
    python generate_churn_data.py [--customers 500] [--host localhost]
"""

import argparse
import random
import sys
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

try:
    import mysql.connector
    from faker import Faker
except ImportError:
    print("Missing dependencies. Run:  pip install mysql-connector-python faker python-dateutil")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
fake = Faker("en_US")
random.seed(42)
Faker.seed(42)

TODAY         = date.today()
START_DATE    = TODAY - relativedelta(years=1)   # 1 year back
CHURN_REASONS = [
    "Better price from competitor",
    "Poor network quality",
    "Customer service dissatisfaction",
    "Relocation",
    "Financial difficulties",
    "Switched to family plan elsewhere",
    "Device incompatibility",
    "Excessive billing issues",
    "No longer needs service",
]
US_STATES = [
    "CA","TX","FL","NY","PA","IL","OH","GA","NC","MI",
    "NJ","VA","WA","AZ","MA","TN","IN","MO","MD","WI",
]


# ─────────────────────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection(host, port, user, password, database):
    return mysql.connector.connect(
        host=host, port=port, user=user,
        password=password, database=database,
        autocommit=False,
    )


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def random_date_between(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))


def months_between(start: date, end: date) -> list[date]:
    """Return list of first-of-month dates from start to end inclusive."""
    months = []
    cur = start.replace(day=1)
    end_m = end.replace(day=1)
    while cur <= end_m:
        months.append(cur)
        cur += relativedelta(months=1)
    return months


# ─────────────────────────────────────────────────────────────
# GENERATORS
# ─────────────────────────────────────────────────────────────
def generate_customers(n: int) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        signup = random_date_between(START_DATE, TODAY - timedelta(days=30))
        is_churned = random.random() < 0.25   # ~25% churn rate
        churn_date = None
        churn_reason = None
        if is_churned:
            earliest_churn = signup + timedelta(days=30)
            if earliest_churn < TODAY:
                churn_date = random_date_between(earliest_churn, TODAY)
                churn_reason = random.choice(CHURN_REASONS)
            else:
                is_churned = False

        customers.append({
            "customer_id":   f"CUST{i:06d}",
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "email":         fake.unique.email(),
            "phone_number":  fake.phone_number()[:20],
            "gender":        random.choice(["Male", "Female", "Other"]),
            "age":           random.randint(18, 75),
            "state":         random.choice(US_STATES),
            "city":          fake.city(),
            "zip_code":      fake.zipcode(),
            "signup_date":   signup,
            "churn_date":    churn_date,
            "is_churned":    int(is_churned),
            "churn_reason":  churn_reason,
        })
    return customers


def generate_subscriptions(customers: list[dict], plan_ids: list[int]) -> list[dict]:
    subs = []
    for c in customers:
        plan_id       = random.choice(plan_ids)
        contract_len  = random.choice([1, 12, 24])
        end_date      = c["churn_date"] if c["is_churned"] else None
        subs.append({
            "customer_id":    c["customer_id"],
            "plan_id":        plan_id,
            "start_date":     c["signup_date"],
            "end_date":       end_date,
            "is_active":      0 if c["is_churned"] else 1,
            "contract_length": contract_len,
            "auto_renew":     random.choice([0, 1]),
        })
    return subs


def generate_usage(customers: list[dict], plan_map: dict) -> list[dict]:
    """Generate monthly usage records for each customer."""
    records = []
    for c in customers:
        active_end = c["churn_date"] if c["is_churned"] else TODAY
        # For churned customers, usage drops off in last 1-2 months
        months = months_between(c["signup_date"], active_end)
        plan   = plan_map.get(c["customer_id"], {})
        limit  = plan.get("data_limit_gb") or 30.0

        for idx, month in enumerate(months):
            # Simulate declining usage pre-churn
            decay = 1.0
            if c["is_churned"] and len(months) > 1:
                months_to_churn = len(months) - 1 - idx
                if months_to_churn <= 2:
                    decay = 0.4 if months_to_churn == 0 else 0.7

            records.append({
                "customer_id":      c["customer_id"],
                "billing_month":    month,
                "data_used_gb":     round(random.uniform(0.1, float(limit) * 1.2) * decay, 2),
                "call_minutes_used": int(random.randint(10, 900) * decay),
                "sms_sent":         int(random.randint(0, 500) * decay),
                "intl_calls_min":   int(random.randint(0, 60) * decay) if plan.get("international") else 0,
                "roaming_days":     random.randint(0, 5) if random.random() < 0.1 else 0,
            })
    return records


def generate_billing(customers: list[dict], plan_map: dict, usage: list[dict]) -> list[dict]:
    usage_idx = {}
    for u in usage:
        usage_idx[(u["customer_id"], u["billing_month"])] = u

    records = []
    for c in customers:
        active_end = c["churn_date"] if c["is_churned"] else TODAY
        months     = months_between(c["signup_date"], active_end)
        plan       = plan_map.get(c["customer_id"], {})
        base       = float(plan.get("monthly_fee", 29.99))
        limit      = float(plan.get("data_limit_gb") or 999)

        for month in months:
            u          = usage_idx.get((c["customer_id"], month), {})
            data_used  = float(u.get("data_used_gb", 0))
            overage    = max(0, data_used - limit) * 10.0   # $10/GB overage
            discount   = base * random.uniform(0, 0.1) if random.random() < 0.15 else 0
            total      = base + overage - discount

            is_overdue = c["is_churned"] and random.random() < 0.3
            paid       = random.random() < 0.92 and not is_overdue
            payment_date = (month + relativedelta(months=1) -
                            timedelta(days=random.randint(1, 15))) if paid else None

            records.append({
                "customer_id":    c["customer_id"],
                "billing_month":  month,
                "base_charge":    round(base, 2),
                "overage_charge": round(overage, 2),
                "discount_amount": round(discount, 2),
                "total_amount":   round(total, 2),
                "payment_status": "Paid" if paid else ("Overdue" if is_overdue else "Pending"),
                "payment_date":   payment_date,
            })
    return records


def generate_support_tickets(customers: list[dict]) -> list[dict]:
    tickets = []
    for c in customers:
        active_end = c["churn_date"] if c["is_churned"] else TODAY
        # Churning customers file more tickets
        n_tickets = random.randint(0, 2)
        if c["is_churned"]:
            n_tickets = random.randint(1, 6)

        for _ in range(n_tickets):
            ticket_date = random_date_between(c["signup_date"], active_end)
            resolved    = random.random() < 0.8
            tickets.append({
                "customer_id":      c["customer_id"],
                "ticket_date":      ticket_date,
                "category":         random.choice(["Billing","Network","Device","Account","Other"]),
                "priority":         random.choices(
                    ["Low","Medium","High","Critical"], weights=[30,40,20,10])[0],
                "status":           "Closed" if resolved else random.choice(["Open","In Progress","Resolved"]),
                "resolution_days":  random.randint(1, 14) if resolved else None,
                "satisfaction_score": random.randint(1, 5) if resolved else None,
            })
    return tickets


def generate_network_quality(customers: list[dict]) -> list[dict]:
    records = []
    for c in customers:
        active_end = c["churn_date"] if c["is_churned"] else TODAY
        # Monthly network quality snapshot
        months = months_between(c["signup_date"], active_end)
        poor_network = c["is_churned"] and c.get("churn_reason","").startswith("Poor")

        for month in months:
            signal = random.uniform(-100, -60) if poor_network else random.uniform(-85, -55)
            records.append({
                "customer_id":    c["customer_id"],
                "event_date":     month,
                "signal_strength": round(signal, 1),
                "dropped_calls":   random.randint(3, 12) if poor_network else random.randint(0, 3),
                "data_speed_mbps": round(random.uniform(1, 15) if poor_network else random.uniform(10, 100), 2),
                "outage_minutes":  random.randint(30, 180) if poor_network else random.randint(0, 20),
            })
    return records


# ─────────────────────────────────────────────────────────────
# BULK INSERT HELPER
# ─────────────────────────────────────────────────────────────
def bulk_insert(cursor, table: str, rows: list[dict], batch_size: int = 500):
    if not rows:
        return
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, [list(r.values()) for r in batch])
    print(f"  ✓  {table:<28} {len(rows):>7,} rows")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Generate telecom churn sample data")
    parser.add_argument("--customers", type=int, default=500,   help="Number of customers (default 500)")
    parser.add_argument("--host",      default="localhost",     help="MySQL host")
    parser.add_argument("--port",      type=int, default=3306,  help="MySQL port")
    parser.add_argument("--user",      default="pg_user",    help="MySQL user")
    parser.add_argument("--password",  default="pg_password_2024",    help="MySQL password")
    parser.add_argument("--database",  default="telecom_churn", help="MySQL database")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  Telecom Churn Data Generator")
    print(f"  Customers : {args.customers:,}")
    print(f"  Period    : {START_DATE} → {TODAY}")
    print(f"  Target DB : {args.host}:{args.port}/{args.database}")
    print(f"{'='*55}\n")

    # ── Connect ──────────────────────────────────────────────
    print("Connecting to MySQL …")
    try:
        conn   = get_connection(args.host, args.port, args.user, args.password, args.database)
        cursor = conn.cursor()
        print("Connected ✓\n")
    except mysql.connector.Error as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    # ── Fetch plan data ───────────────────────────────────────
    cursor.execute("SELECT plan_id, plan_type, monthly_fee, data_limit_gb, international FROM subscription_plans")
    plans = [{"plan_id": r[0], "plan_type": r[1], "monthly_fee": r[2],
              "data_limit_gb": r[3], "international": r[4]} for r in cursor.fetchall()]
    plan_ids = [p["plan_id"] for p in plans]

    # ── Generate customers ────────────────────────────────────
    print("Generating data …")
    customers = generate_customers(args.customers)

    # Assign a plan per customer (for usage/billing lookups)
    plan_assignments = {c["customer_id"]: random.choice(plans) for c in customers}

    subs    = generate_subscriptions(customers, plan_ids)
    usage   = generate_usage(customers, plan_assignments)
    billing = generate_billing(customers, plan_assignments, usage)
    tickets = generate_support_tickets(customers)
    nq      = generate_network_quality(customers)

    print(f"\nInserting rows into MySQL …\n")
    try:
        bulk_insert(cursor, "customers",             customers)
        bulk_insert(cursor, "customer_subscriptions", subs)
        bulk_insert(cursor, "usage_records",         usage)
        bulk_insert(cursor, "billing_records",        billing)
        bulk_insert(cursor, "support_tickets",        tickets)
        bulk_insert(cursor, "network_quality",        nq)
        conn.commit()
        print(f"\n{'='*55}")
        print("  All data committed successfully! ✓")
        print(f"{'='*55}\n")
    except Exception as e:
        conn.rollback()
        print(f"\nError during insert — rolled back: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
