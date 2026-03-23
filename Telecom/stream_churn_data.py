#!/usr/bin/env python3
"""
Telecom Customer Churn — Real-Time Data Streamer
Continuously streams live events into MySQL every second.

Simulates real-world telecom activity:
  • New customer sign-ups
  • Usage events  (data, calls, SMS)
  • Billing transactions
  • Support ticket opens / closures
  • Network quality fluctuations
  • Churn events

Dependencies:
    pip install mysql-connector-python faker python-dateutil

Usage:
    python stream_churn_data.py [options]

    --interval    seconds between ticks  (default 1.0)
    --events      events per tick        (default 3)
    --host        MySQL host             (default localhost)
    --port        MySQL port             (default 3306)
    --user        MySQL user             (default pg_user)
    --password    MySQL password         (default pg_password_2024)
    --database    MySQL database         (default telecom_churn)
    --verbose     print every event      (default: summary only)
"""

import argparse
import random
import signal
import sys
import time
from datetime import date, datetime, timedelta
from typing import Optional

try:
    import mysql.connector
    from faker import Faker
except ImportError:
    print("Missing deps:  pip install mysql-connector-python faker python-dateutil")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
fake = Faker("en_US")

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
TICKET_CATEGORIES = ["Billing", "Network", "Device", "Account", "Other"]
TICKET_PRIORITIES = ["Low", "Medium", "High", "Critical"]
US_STATES = [
    "CA","TX","FL","NY","PA","IL","OH","GA","NC","MI",
    "NJ","VA","WA","AZ","MA","TN","IN","MO","MD","WI",
]

# Event weights — tune to change the mix of live events
EVENT_TYPES = [
    "usage",           # customer uses data/calls/SMS
    "billing",         # billing transaction processed
    "support_open",    # new support ticket
    "support_close",   # existing ticket resolved
    "network",         # network quality reading
    "new_customer",    # brand new sign-up
    "churn",           # customer churns
]
EVENT_WEIGHTS = [40, 20, 12, 10, 10, 5, 3]   # must sum ≈ 100

# ANSI colours for terminal output
C = {
    "reset":   "\033[0m",
    "bold":    "\033[1m",
    "green":   "\033[92m",
    "yellow":  "\033[93m",
    "red":     "\033[91m",
    "cyan":    "\033[96m",
    "magenta": "\033[95m",
    "blue":    "\033[94m",
    "grey":    "\033[90m",
}

EVENT_COLORS = {
    "usage":         C["cyan"],
    "billing":       C["green"],
    "support_open":  C["yellow"],
    "support_close": C["blue"],
    "network":       C["magenta"],
    "new_customer":  C["green"] + C["bold"],
    "churn":         C["red"]   + C["bold"],
}


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def connect(host, port, user, password, database) -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=host, port=port, user=user,
        password=password, database=database,
        autocommit=True,
        connection_timeout=10,
    )


def fetch_active_customers(cursor) -> list[dict]:
    cursor.execute("""
        SELECT c.customer_id, cs.plan_id, sp.monthly_fee,
               sp.data_limit_gb, sp.international
        FROM customers c
        JOIN customer_subscriptions cs ON c.customer_id = cs.customer_id AND cs.is_active = 1
        JOIN subscription_plans     sp ON cs.plan_id = sp.plan_id
        WHERE c.is_churned = 0
        ORDER BY RAND()
        LIMIT 2000
    """)
    return [
        {"customer_id": r[0], "plan_id": r[1], "monthly_fee": float(r[2]),
         "data_limit_gb": float(r[3] or 30), "international": bool(r[4])}
        for r in cursor.fetchall()
    ]


def fetch_open_tickets(cursor) -> list[int]:
    cursor.execute("""
        SELECT ticket_id FROM support_tickets
        WHERE status IN ('Open','In Progress')
        ORDER BY RAND()
        LIMIT 200
    """)
    return [r[0] for r in cursor.fetchall()]


def fetch_plan_ids(cursor) -> list[int]:
    cursor.execute("SELECT plan_id FROM subscription_plans")
    return [r[0] for r in cursor.fetchall()]


def next_customer_id(cursor) -> str:
    cursor.execute("SELECT COUNT(*) FROM customers")
    n = cursor.fetchone()[0]
    return f"CUST{(n + 1):06d}"


# ─────────────────────────────────────────────────────────────────────────────
# EVENT GENERATORS  — each returns (table, row_dict, log_message)
# ─────────────────────────────────────────────────────────────────────────────

def evt_usage(customer: dict) -> tuple:
    limit  = customer["data_limit_gb"]
    month  = date.today().replace(day=1)
    row = {
        "customer_id":       customer["customer_id"],
        "billing_month":     month,
        "data_used_gb":      round(random.uniform(0.01, limit * 0.15), 3),
        "call_minutes_used": random.randint(1, 60),
        "sms_sent":          random.randint(0, 30),
        "intl_calls_min":    random.randint(0, 10) if customer["international"] else 0,
        "roaming_days":      1 if random.random() < 0.05 else 0,
    }
    msg = (f"📶 USAGE       {customer['customer_id']}  "
           f"{row['data_used_gb']:.3f} GB  "
           f"{row['call_minutes_used']} min  "
           f"{row['sms_sent']} SMS")
    return ("usage_records", row, msg, "ON DUPLICATE KEY UPDATE "
            "data_used_gb = data_used_gb + VALUES(data_used_gb), "
            "call_minutes_used = call_minutes_used + VALUES(call_minutes_used), "
            "sms_sent = sms_sent + VALUES(sms_sent), "
            "intl_calls_min = intl_calls_min + VALUES(intl_calls_min), "
            "roaming_days = roaming_days + VALUES(roaming_days)")


def evt_billing(customer: dict) -> tuple:
    base     = customer["monthly_fee"]
    overage  = round(random.uniform(0, 5), 2) if random.random() < 0.2 else 0.0
    discount = round(base * random.uniform(0, 0.1), 2) if random.random() < 0.1 else 0.0
    total    = round(base + overage - discount, 2)
    paid     = random.random() < 0.93
    row = {
        "customer_id":    customer["customer_id"],
        "billing_month":  date.today().replace(day=1),
        "base_charge":    round(base, 2),
        "overage_charge": overage,
        "discount_amount": discount,
        "total_amount":   total,
        "payment_status": "Paid" if paid else "Pending",
        "payment_date":   date.today() if paid else None,
    }
    status_icon = "✅" if paid else "⏳"
    msg = (f"💳 BILLING     {customer['customer_id']}  "
           f"${total:.2f}  {status_icon} {'Paid' if paid else 'Pending'}")
    return ("billing_records", row, msg, "ON DUPLICATE KEY UPDATE "
            "overage_charge = VALUES(overage_charge), "
            "discount_amount = VALUES(discount_amount), "
            "total_amount = VALUES(total_amount), "
            "payment_status = VALUES(payment_status), "
            "payment_date = VALUES(payment_date)")


def evt_support_open(customer: dict) -> tuple:
    category = random.choice(TICKET_CATEGORIES)
    priority = random.choices(TICKET_PRIORITIES, weights=[30, 40, 20, 10])[0]
    row = {
        "customer_id":       customer["customer_id"],
        "ticket_date":       date.today(),
        "category":          category,
        "priority":          priority,
        "status":            "Open",
        "resolution_days":   None,
        "satisfaction_score": None,
    }
    icon = {"Low": "🟢", "Medium": "🟡", "High": "🟠", "Critical": "🔴"}[priority]
    msg = (f"🎫 TICKET OPEN {customer['customer_id']}  "
           f"{icon} {priority}  [{category}]")
    return ("support_tickets", row, msg, None)


def evt_support_close(ticket_id: int) -> tuple:
    score = random.randint(1, 5)
    days  = random.randint(1, 10)
    row = {
        "ticket_id":          ticket_id,
        "status":             "Closed",
        "resolution_days":    days,
        "satisfaction_score": score,
    }
    stars = "⭐" * score
    msg = f"✅ TICKET CLOSE #{ticket_id}  resolved in {days}d  {stars}"
    return ("support_tickets_update", row, msg, None)   # handled specially


def evt_network(customer: dict) -> tuple:
    poor   = random.random() < 0.08
    signal = round(random.uniform(-100, -65) if poor else random.uniform(-80, -50), 1)
    row = {
        "customer_id":    customer["customer_id"],
        "event_date":     date.today(),
        "signal_strength": signal,
        "dropped_calls":  random.randint(2, 8) if poor else random.randint(0, 1),
        "data_speed_mbps": round(random.uniform(1, 10) if poor else random.uniform(15, 100), 2),
        "outage_minutes": random.randint(10, 60) if poor else 0,
    }
    quality = "⚠️  POOR" if poor else "📡 OK  "
    msg = (f"📡 NETWORK     {customer['customer_id']}  "
           f"{quality}  {signal} dBm  {row['data_speed_mbps']:.1f} Mbps")
    return ("network_quality", row, msg, None)


def evt_new_customer(plan_ids: list[int]) -> tuple:
    cid = f"CUST{random.randint(100000, 999999):06d}"   # streamer uses random IDs
    plan_id = random.choice(plan_ids)
    today   = date.today()
    customer_row = {
        "customer_id":  cid,
        "first_name":   fake.first_name(),
        "last_name":    fake.last_name(),
        "email":        fake.unique.email(),
        "phone_number": fake.phone_number()[:20],
        "gender":       random.choice(["Male", "Female", "Other"]),
        "age":          random.randint(18, 70),
        "state":        random.choice(US_STATES),
        "city":         fake.city(),
        "zip_code":     fake.zipcode(),
        "signup_date":  today,
        "churn_date":   None,
        "is_churned":   0,
        "churn_reason": None,
    }
    sub_row = {
        "customer_id":    cid,
        "plan_id":        plan_id,
        "start_date":     today,
        "end_date":       None,
        "is_active":      1,
        "contract_length": random.choice([1, 12, 24]),
        "auto_renew":     1,
    }
    msg = (f"🆕 NEW SIGNUP  {cid}  "
           f"{customer_row['first_name']} {customer_row['last_name']}  "
           f"Plan #{plan_id}")
    return ("new_customer", {"customer": customer_row, "sub": sub_row}, msg, None)


def evt_churn(customer: dict) -> tuple:
    reason = random.choice(CHURN_REASONS)
    today  = date.today()
    row = {
        "customer_id":  customer["customer_id"],
        "churn_date":   today,
        "is_churned":   1,
        "churn_reason": reason,
    }
    msg = (f"❌ CHURN       {customer['customer_id']}  "
           f"\"{reason}\"")
    return ("churn_event", row, msg, None)   # handled specially


# ─────────────────────────────────────────────────────────────────────────────
# DISPATCH — write each event to MySQL
# ─────────────────────────────────────────────────────────────────────────────
def dispatch(cursor, event_type: str, payload: tuple) -> bool:
    table, row, _msg, upsert = payload

    try:
        if table == "support_tickets_update":
            cursor.execute("""
                UPDATE support_tickets
                SET status = %s, resolution_days = %s, satisfaction_score = %s
                WHERE ticket_id = %s
            """, (row["status"], row["resolution_days"],
                  row["satisfaction_score"], row["ticket_id"]))

        elif table == "churn_event":
            cursor.execute("""
                UPDATE customers
                SET is_churned = 1, churn_date = %s, churn_reason = %s
                WHERE customer_id = %s
            """, (row["churn_date"], row["churn_reason"], row["customer_id"]))
            cursor.execute("""
                UPDATE customer_subscriptions
                SET is_active = 0, end_date = %s
                WHERE customer_id = %s AND is_active = 1
            """, (row["churn_date"], row["customer_id"]))

        elif table == "new_customer":
            c = row["customer"]
            s = row["sub"]
            cols_c = ", ".join(c.keys())
            vals_c = ", ".join(["%s"] * len(c))
            cursor.execute(
                f"INSERT IGNORE INTO customers ({cols_c}) VALUES ({vals_c})",
                list(c.values())
            )
            cols_s = ", ".join(s.keys())
            vals_s = ", ".join(["%s"] * len(s))
            cursor.execute(
                f"INSERT IGNORE INTO customer_subscriptions ({cols_s}) VALUES ({vals_s})",
                list(s.values())
            )

        else:
            cols  = ", ".join(row.keys())
            vals  = ", ".join(["%s"] * len(row))
            sql   = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
            if upsert:
                sql += f" {upsert}"
            else:
                sql = sql.replace("INSERT INTO", "INSERT IGNORE INTO")
            cursor.execute(sql, list(row.values()))

        return True

    except mysql.connector.Error as e:
        print(f"  {C['red']}DB error ({table}): {e}{C['reset']}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# STREAMER LOOP
# ─────────────────────────────────────────────────────────────────────────────
class Streamer:
    def __init__(self, args):
        self.args     = args
        self.conn     = None
        self.cursor   = None
        self.running  = False
        self.counters = {e: 0 for e in EVENT_TYPES}
        self.counters["errors"] = 0
        self.tick     = 0
        self.start_ts = None

    def setup(self):
        print(f"\n{C['bold']}{'═'*60}{C['reset']}")
        print(f"  {C['cyan']}Telecom Churn — Live Data Streamer{C['reset']}")
        print(f"  Interval : {self.args.interval}s  |  Events/tick : {self.args.events}")
        print(f"  Target   : {self.args.host}:{self.args.port}/{self.args.database}")
        print(f"{C['bold']}{'═'*60}{C['reset']}\n")

        print("Connecting to MySQL …")
        for attempt in range(1, 6):
            try:
                self.conn   = connect(self.args.host, self.args.port,
                                      self.args.user, self.args.password,
                                      self.args.database)
                self.cursor = self.conn.cursor()
                print(f"{C['green']}Connected ✓{C['reset']}\n")
                return
            except mysql.connector.Error as e:
                print(f"  Attempt {attempt}/5 failed: {e}")
                if attempt < 5:
                    time.sleep(3)
        print(f"{C['red']}Could not connect. Exiting.{C['reset']}")
        sys.exit(1)

    def reconnect(self):
        try:
            self.conn   = connect(self.args.host, self.args.port,
                                  self.args.user, self.args.password,
                                  self.args.database)
            self.cursor = self.conn.cursor()
        except Exception:
            pass

    def stream(self):
        self.running  = True
        self.start_ts = time.time()

        # Register SIGINT / SIGTERM for clean shutdown
        signal.signal(signal.SIGINT,  lambda *_: self.stop())
        signal.signal(signal.SIGTERM, lambda *_: self.stop())

        print(f"{C['grey']}Press Ctrl+C to stop.{C['reset']}\n")

        while self.running:
            tick_start = time.time()
            self.tick += 1

            # ── Refresh DB state every 30 ticks ──────────────────
            if self.tick % 30 == 1:
                try:
                    self.active_customers = fetch_active_customers(self.cursor)
                    self.open_tickets     = fetch_open_tickets(self.cursor)
                    self.plan_ids         = fetch_plan_ids(self.cursor)
                except Exception:
                    self.reconnect()
                    continue

            if not self.active_customers:
                print(f"{C['yellow']}No active customers found — waiting …{C['reset']}")
                time.sleep(self.args.interval)
                continue

            # ── Generate N events this tick ───────────────────────
            chosen_types = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS,
                                          k=self.args.events)

            for etype in chosen_types:
                payload = self._build_event(etype)
                if payload is None:
                    continue

                ok = dispatch(self.cursor, etype, payload)
                if ok:
                    self.counters[etype] += 1
                    if self.args.verbose:
                        color = EVENT_COLORS.get(etype, "")
                        print(f"  {color}{payload[2]}{C['reset']}")
                else:
                    self.counters["errors"] += 1

            # ── Tick summary (always shown) ───────────────────────
            elapsed = time.time() - self.start_ts
            total   = sum(v for k, v in self.counters.items() if k != "errors")
            print(
                f"{C['grey']}[{elapsed:>7.1f}s]  "
                f"tick {self.tick:>5}  │  "
                f"total {total:>6,}  │  "
                f"new {self.counters['new_customer']:>4}  "
                f"churn {self.counters['churn']:>4}  "
                f"usage {self.counters['usage']:>5}  "
                f"billing {self.counters['billing']:>5}  "
                f"tickets {self.counters['support_open']:>4}  "
                f"errors {self.counters['errors']:>3}"
                f"{C['reset']}"
            )

            # ── Sleep for remainder of the interval ───────────────
            elapsed_tick = time.time() - tick_start
            sleep_time   = max(0.0, self.args.interval - elapsed_tick)
            time.sleep(sleep_time)

        self._print_summary()

    def _build_event(self, etype: str) -> Optional[tuple]:
        customer = random.choice(self.active_customers)

        if etype == "usage":
            return evt_usage(customer)
        elif etype == "billing":
            return evt_billing(customer)
        elif etype == "support_open":
            return evt_support_open(customer)
        elif etype == "support_close":
            if not self.open_tickets:
                return None
            tid = random.choice(self.open_tickets)
            self.open_tickets.remove(tid)   # avoid closing same ticket twice
            return evt_support_close(tid)
        elif etype == "network":
            return evt_network(customer)
        elif etype == "new_customer":
            return evt_new_customer(self.plan_ids)
        elif etype == "churn":
            # Remove from active pool immediately
            self.active_customers = [
                c for c in self.active_customers
                if c["customer_id"] != customer["customer_id"]
            ]
            return evt_churn(customer)
        return None

    def stop(self):
        print(f"\n{C['yellow']}Stopping streamer …{C['reset']}")
        self.running = False

    def _print_summary(self):
        elapsed = time.time() - self.start_ts
        total   = sum(v for k, v in self.counters.items() if k != "errors")
        print(f"\n{C['bold']}{'═'*60}{C['reset']}")
        print(f"  {C['cyan']}Stream Summary{C['reset']}")
        print(f"  Runtime : {elapsed:.1f}s  |  Ticks : {self.tick:,}")
        print(f"  Total events inserted : {total:,}")
        print()
        for k, v in self.counters.items():
            if k == "errors":
                continue
            bar = "█" * min(40, v // max(1, total // 40))
            print(f"  {k:<18} {v:>6,}  {C['cyan']}{bar}{C['reset']}")
        if self.counters["errors"]:
            print(f"  {C['red']}errors           {self.counters['errors']:>6}{C['reset']}")
        print(f"{C['bold']}{'═'*60}{C['reset']}\n")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="Stream live telecom churn events to MySQL")
    p.add_argument("--interval",  type=float, default=1.0,           help="Seconds between ticks (default 1.0)")
    p.add_argument("--events",    type=int,   default=3,             help="Events per tick (default 3)")
    p.add_argument("--host",                  default="localhost",   help="MySQL host")
    p.add_argument("--port",      type=int,   default=3306,          help="MySQL port")
    p.add_argument("--user",                  default="pg_user",  help="MySQL user")
    p.add_argument("--password",              default="pg_password_2024",  help="MySQL password")
    p.add_argument("--database",              default="telecom_churn", help="MySQL database")
    p.add_argument("--verbose",   action="store_true",               help="Print every event")
    args = p.parse_args()

    streamer = Streamer(args)
    streamer.setup()
    streamer.stream()


if __name__ == "__main__":
    main()
