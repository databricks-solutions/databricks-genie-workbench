# Databricks notebook source

# MAGIC %md
# MAGIC # SaaS Customer Churn - Synthetic Dataset Generator
# MAGIC
# MAGIC Generates a fictional SaaS customer churn dataset for Databricks AI/BI Genie demos.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---:|---|
# MAGIC | `accounts` | 900 | Customer account dimension |
# MAGIC | `subscriptions` | 900 | Subscription and ARR dimension |
# MAGIC | `product_usage` | variable | Monthly account usage facts |
# MAGIC | `support_tickets` | 2,500 | Customer support ticket facts |
# MAGIC | `invoices` | variable | Monthly invoice and payment facts |
# MAGIC | `churn_events` | variable | Churn facts linked to subscriptions |
# MAGIC
# MAGIC **Setup:** Set the `catalog` and `schema` widgets (or edit defaults below), then **Run All**.
# MAGIC
# MAGIC Seed: `42` - fully reproducible.

# COMMAND ----------

# MAGIC %pip install faker==40.19.1 --quiet
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION - Edit only this section before running
# =============================================================================
DEFAULT_CATALOG = ""  # Unity Catalog name; required
DEFAULT_SCHEMA = "saas_churn"  # Schema / database name

# Widgets let the bulk runner pass values while preserving standalone defaults.
try:
    dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Unity Catalog name")
except Exception:
    pass

try:
    dbutils.widgets.text("schema", DEFAULT_SCHEMA, "Schema / database name")
except Exception:
    pass

try:
    dbutils.widgets.dropdown("overwrite_existing", "false", ["false", "true"], "Overwrite existing tables")
except Exception:
    pass

try:
    CATALOG = dbutils.widgets.get("catalog").strip() or DEFAULT_CATALOG
except Exception:
    CATALOG = DEFAULT_CATALOG

try:
    SCHEMA = dbutils.widgets.get("schema").strip() or DEFAULT_SCHEMA
except Exception:
    SCHEMA = DEFAULT_SCHEMA

try:
    OVERWRITE_EXISTING = dbutils.widgets.get("overwrite_existing").strip().lower() == "true"
except Exception:
    OVERWRITE_EXISTING = False

if not CATALOG:
    raise ValueError("Set the catalog widget to a Unity Catalog name before running this notebook.")

WRITE_MODE = "overwrite" if OVERWRITE_EXISTING else "errorifexists"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports & Global Setup

# COMMAND ----------

# =============================================================================
# IMPORTS & GLOBAL SETUP
# =============================================================================
import random
from calendar import monthrange
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("en_US")
Faker.seed(SEED)

START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 12, 31)
MONTH_LIST = [(y, m) for y in [2023, 2024, 2025] for m in range(1, 13)]

SEGMENTS = ["SMB", "Mid-Market", "Enterprise"]
PLAN_TIERS = ["Starter", "Professional", "Enterprise"]
INDUSTRIES = ["Technology", "Retail", "Financial Services", "Healthcare", "Manufacturing", "Education"]

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Accounts

# COMMAND ----------

# =============================================================================
# TABLE 1: ACCOUNTS (900 rows)
# =============================================================================
accounts_data = []
for i in range(1, 901):
    segment = random.choices(SEGMENTS, weights=[52, 32, 16])[0]
    if segment == "Enterprise":
        employee_count = random.randint(1500, 45000)
        csm_tier = random.choices(["Strategic", "Named", "Pooled"], weights=[70, 25, 5])[0]
    elif segment == "Mid-Market":
        employee_count = random.randint(250, 2500)
        csm_tier = random.choices(["Strategic", "Named", "Pooled"], weights=[10, 65, 25])[0]
    else:
        employee_count = random.randint(10, 400)
        csm_tier = random.choices(["Named", "Pooled", "Tech Touch"], weights=[12, 48, 40])[0]
    ai_adopter = random.random() < {"SMB": 0.28, "Mid-Market": 0.42, "Enterprise": 0.58}[segment]
    accounts_data.append(
        {
            "account_id": f"ACCT-{i:05d}",
            "account_name": fake.company(),
            "segment": segment,
            "industry": random.choice(INDUSTRIES),
            "region": random.choice(["North America", "EMEA", "APAC", "LATAM"]),
            "employee_count": int(employee_count),
            "acquisition_channel": random.choices(
                ["Sales Outbound", "Partner", "Inbound", "Product-Led", "Event"],
                weights=[25, 18, 24, 23, 10],
            )[0],
            "customer_since_date": START_DATE - timedelta(days=random.randint(0, 1200)),
            "csm_tier": csm_tier,
            "ai_feature_adopter": bool(ai_adopter),
            "is_active": True,
        }
    )

df_accounts = pd.DataFrame(accounts_data)
ACCOUNT_BY_ID = {a["account_id"]: a for a in accounts_data}
ACCOUNT_IDS = [a["account_id"] for a in accounts_data]
print(f"accounts: {len(df_accounts)} rows")
print(df_accounts["segment"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Subscriptions

# COMMAND ----------

# =============================================================================
# TABLE 2: SUBSCRIPTIONS (900 rows)
# =============================================================================
subscriptions_data = []
SUBSCRIPTION_BY_ID = {}
for i, account in enumerate(accounts_data, start=1):
    segment = account["segment"]
    if segment == "Enterprise":
        plan = random.choices(PLAN_TIERS, weights=[2, 18, 80])[0]
        seats = random.randint(300, 6000)
        base_mrr_per_seat = random.uniform(58, 115)
    elif segment == "Mid-Market":
        plan = random.choices(PLAN_TIERS, weights=[12, 68, 20])[0]
        seats = random.randint(60, 700)
        base_mrr_per_seat = random.uniform(38, 82)
    else:
        plan = random.choices(PLAN_TIERS, weights=[58, 38, 4])[0]
        seats = random.randint(5, 120)
        base_mrr_per_seat = random.uniform(18, 55)
    billing_term = random.choices(["Monthly", "Annual"], weights=[64, 36] if segment != "Enterprise" else [22, 78])[0]
    discount_pct = random.uniform(0.02, 0.15) if billing_term == "Annual" else random.uniform(0.00, 0.08)
    mrr = seats * base_mrr_per_seat * (1.0 - discount_pct)
    start_date = START_DATE - timedelta(days=random.randint(0, 700))
    row = {
        "subscription_id": f"SUB-{i:06d}",
        "account_id": account["account_id"],
        "plan_tier": plan,
        "billing_term": billing_term,
        "start_date": start_date,
        "end_date": None,
        "subscription_status": "Active",
        "seats_purchased": int(seats),
        "monthly_recurring_revenue_usd": round(mrr, 2),
        "annual_recurring_revenue_usd": round(mrr * 12.0, 2),
        "discount_pct": round(discount_pct * 100.0, 2),
    }
    subscriptions_data.append(row)
    SUBSCRIPTION_BY_ID[row["subscription_id"]] = row

df_subscriptions = pd.DataFrame(subscriptions_data)
SUBSCRIPTION_IDS = [s["subscription_id"] for s in subscriptions_data]
SUB_BY_ACCOUNT = {s["account_id"]: s for s in subscriptions_data}
print(f"subscriptions: {len(df_subscriptions)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Product Usage

# COMMAND ----------

# =============================================================================
# TABLE 3: PRODUCT USAGE (monthly)
# =============================================================================
# Injected patterns:
# 1. Low seat utilization predicts churn.
# 2. AI feature adoption improves health score and reduces churn risk.
# 3. Enterprise accounts have higher ARR and generally stronger retention.

usage_data = []
usage_counter = 1
USAGE_BY_SUB = {}

for sub in subscriptions_data:
    account = ACCOUNT_BY_ID[sub["account_id"]]
    segment = account["segment"]
    seats = sub["seats_purchased"]
    risk_profile = random.choices(["Healthy", "At Risk"], weights=[73, 27])[0]
    base_utilization = {"SMB": 0.54, "Mid-Market": 0.62, "Enterprise": 0.71}[segment]
    if risk_profile == "At Risk":
        base_utilization -= random.uniform(0.18, 0.32)
    if account["ai_feature_adopter"]:
        base_utilization += random.uniform(0.04, 0.10)
    for year, month in MONTH_LIST:
        usage_month = date(year, month, 1)
        if usage_month < date(sub["start_date"].year, sub["start_date"].month, 1):
            continue
        month_idx = (year - 2023) * 12 + month - 1
        trend = 0.002 * month_idx if risk_profile == "Healthy" else -0.003 * month_idx
        seasonal = 0.04 if month in (1, 9, 10) else -0.03 if month in (7, 12) else 0.0
        utilization = max(0.04, min(0.98, base_utilization + trend + seasonal + random.gauss(0, 0.07)))
        active_users = max(1, int(seats * utilization))
        ai_users = int(active_users * random.uniform(0.22, 0.72)) if account["ai_feature_adopter"] else 0
        queries = int(active_users * random.uniform(18, 85) * (1.25 if account["ai_feature_adopter"] else 1.0))
        workflows = int(active_users * random.uniform(0.4, 2.4))
        health_score = max(1, min(100, 42 + utilization * 55 + (8 if account["ai_feature_adopter"] else 0) + random.gauss(0, 7)))
        row = {
            "usage_id": f"USG-{usage_counter:08d}",
            "usage_month": usage_month,
            "usage_year": year,
            "usage_month_num": month,
            "account_id": sub["account_id"],
            "subscription_id": sub["subscription_id"],
            "segment": segment,
            "plan_tier": sub["plan_tier"],
            "seats_purchased": int(seats),
            "active_users": int(active_users),
            "seat_utilization_pct": round(utilization * 100.0, 2),
            "ai_feature_enabled": bool(account["ai_feature_adopter"]),
            "ai_feature_active_users": int(ai_users),
            "queries_run": int(queries),
            "workflows_created": int(workflows),
            "data_exports": int(active_users * random.uniform(0.05, 0.8)),
            "health_score": round(health_score, 1),
        }
        usage_data.append(row)
        USAGE_BY_SUB.setdefault(sub["subscription_id"], []).append(row)
        usage_counter += 1

df_product_usage = pd.DataFrame(usage_data)
print(f"product_usage: {len(df_product_usage)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Support Tickets

# COMMAND ----------

# =============================================================================
# TABLE 4: SUPPORT TICKETS (2,500 rows)
# =============================================================================
support_data = []
ticket_counter = 1

sub_weights = []
for sub in subscriptions_data:
    account = ACCOUNT_BY_ID[sub["account_id"]]
    recent_usage = USAGE_BY_SUB[sub["subscription_id"]][-6:]
    avg_health = sum(u["health_score"] for u in recent_usage) / len(recent_usage)
    weight = 1.0 + max(0, 70 - avg_health) / 25.0
    if account["segment"] == "Enterprise":
        weight *= 1.5
    sub_weights.append(weight)

for _ in range(2500):
    sub = random.choices(subscriptions_data, weights=sub_weights)[0]
    account = ACCOUNT_BY_ID[sub["account_id"]]
    created_year, created_month = random.choice(MONTH_LIST)
    created_date = date(created_year, created_month, random.randint(1, monthrange(created_year, created_month)[1]))
    recent_usage = [
        u for u in USAGE_BY_SUB[sub["subscription_id"]]
        if u["usage_month"] <= date(created_year, created_month, 1)
    ][-3:]
    avg_health = sum(u["health_score"] for u in recent_usage) / len(recent_usage) if recent_usage else 70
    severity_weights = [58, 28, 11, 3]
    if avg_health < 55:
        severity_weights = [38, 32, 21, 9]
    severity = random.choices(["Low", "Medium", "High", "Critical"], weights=severity_weights)[0]
    status = random.choices(["Resolved", "Open", "Escalated"], weights=[82, 12, 6] if severity in ("Low", "Medium") else [64, 18, 18])[0]
    resolution_hours = None
    if status == "Resolved":
        resolution_hours = random.uniform(1, 18) if severity == "Low" else random.uniform(6, 72) if severity == "Medium" else random.uniform(24, 168)
    elif status == "Escalated":
        resolution_hours = random.uniform(48, 240)
    csat = None
    if status == "Resolved":
        csat = random.choices([1, 2, 3, 4, 5], weights=[5, 8, 15, 34, 38] if severity in ("Low", "Medium") else [18, 24, 26, 20, 12])[0]
    support_data.append(
        {
            "ticket_id": f"TCKT-{ticket_counter:07d}",
            "created_date": created_date,
            "ticket_year": created_year,
            "ticket_month": created_month,
            "account_id": sub["account_id"],
            "subscription_id": sub["subscription_id"],
            "severity": severity,
            "category": random.choice(["Bug", "How To", "Integration", "Billing", "Performance", "Feature Request"]),
            "status": status,
            "resolution_time_hours": None if resolution_hours is None else round(resolution_hours, 2),
            "csat_score": csat,
            "is_escalated": status == "Escalated",
        }
    )
    ticket_counter += 1

df_support_tickets = pd.DataFrame(support_data)
TICKETS_BY_SUB = {}
for ticket in support_data:
    TICKETS_BY_SUB.setdefault(ticket["subscription_id"], []).append(ticket)
print(f"support_tickets: {len(df_support_tickets)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Invoices

# COMMAND ----------

# =============================================================================
# TABLE 5: INVOICES
# =============================================================================
invoice_data = []
invoice_counter = 1
INVOICES_BY_SUB = {}

for sub in subscriptions_data:
    account = ACCOUNT_BY_ID[sub["account_id"]]
    start_month_anchor = date(sub["start_date"].year, sub["start_date"].month, 1)
    for year, month in MONTH_LIST:
        invoice_month = date(year, month, 1)
        if invoice_month < date(sub["start_date"].year, sub["start_date"].month, 1):
            continue
        if sub["billing_term"] == "Annual":
            months_since_start = (invoice_month.year - start_month_anchor.year) * 12 + (
                invoice_month.month - start_month_anchor.month
            )
            if months_since_start % 12 != 0:
                continue
        amount_due = sub["monthly_recurring_revenue_usd"] * (12.0 if sub["billing_term"] == "Annual" else 1.0)
        late_prob = 0.07
        if account["segment"] == "SMB":
            late_prob += 0.08
        if sub["billing_term"] == "Monthly":
            late_prob += 0.05
        days_late = 0
        payment_status = "Paid"
        if random.random() < late_prob:
            days_late = random.choices([5, 12, 21, 35, 60], weights=[25, 30, 24, 14, 7])[0]
            payment_status = "Late"
        if random.random() < 0.025:
            payment_status = "Open"
            days_late = random.choice([15, 30, 45, 75])
        row = {
            "invoice_id": f"INV-{invoice_counter:08d}",
            "invoice_month": invoice_month,
            "invoice_year": year,
            "invoice_month_num": month,
            "invoice_date": invoice_month,
            "account_id": sub["account_id"],
            "subscription_id": sub["subscription_id"],
            "billing_term": sub["billing_term"],
            "amount_due_usd": round(amount_due, 2),
            "amount_paid_usd": round(0.0 if payment_status == "Open" else amount_due, 2),
            "payment_status": payment_status,
            "days_late": int(days_late),
        }
        invoice_data.append(row)
        INVOICES_BY_SUB.setdefault(sub["subscription_id"], []).append(row)
        invoice_counter += 1

df_invoices = pd.DataFrame(invoice_data)
print(f"invoices: {len(df_invoices)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Churn Events

# COMMAND ----------

# =============================================================================
# TABLE 6: CHURN EVENTS
# =============================================================================
churn_data = []
churn_counter = 1

for sub in subscriptions_data:
    account = ACCOUNT_BY_ID[sub["account_id"]]
    usage_rows = USAGE_BY_SUB[sub["subscription_id"]]
    avg_util = sum(u["seat_utilization_pct"] for u in usage_rows[-6:]) / len(usage_rows[-6:])
    avg_health = sum(u["health_score"] for u in usage_rows[-6:]) / len(usage_rows[-6:])
    tickets = TICKETS_BY_SUB.get(sub["subscription_id"], [])
    severe_ticket_count = sum(1 for t in tickets if t["severity"] in ("High", "Critical"))
    late_invoice_count = sum(1 for inv in INVOICES_BY_SUB.get(sub["subscription_id"], []) if inv["days_late"] >= 15)
    prob = 0.06
    if avg_util < 35:
        prob += 0.22
    elif avg_util < 50:
        prob += 0.11
    if sub["billing_term"] == "Annual":
        prob -= 0.055
    if account["segment"] == "Enterprise":
        prob -= 0.045
    if account["ai_feature_adopter"]:
        prob -= 0.060
    prob += min(0.14, severe_ticket_count * 0.012)
    prob += min(0.12, late_invoice_count * 0.020)
    prob = max(0.01, min(0.45, prob))
    if random.random() < prob:
        severe_dates = [t["created_date"] for t in tickets if t["severity"] in ("High", "Critical") and t["created_date"] <= date(2025, 10, 1)]
        if severe_dates:
            anchor = random.choice(severe_dates)
            churn_date = min(END_DATE, anchor + timedelta(days=random.randint(21, 90)))
            if churn_date < date(2024, 1, 1):
                churn_date = date(2024, random.randint(1, 12), random.randint(1, 25))
        else:
            churn_date = date(random.choice([2024, 2025]), random.randint(1, 12), random.randint(1, 25))
        reason = random.choices(
            ["Low Adoption", "Product Gaps", "Budget", "Support Experience", "Consolidation", "Competitor"],
            weights=[34 if avg_util < 50 else 15, 18, 18, 18 if severe_ticket_count else 8, 10, 12],
        )[0]
        churn_data.append(
            {
                "churn_event_id": f"CHURN-{churn_counter:07d}",
                "churn_date": churn_date,
                "churn_year": churn_date.year,
                "churn_month": churn_date.month,
                "account_id": sub["account_id"],
                "subscription_id": sub["subscription_id"],
                "segment": account["segment"],
                "billing_term": sub["billing_term"],
                "plan_tier": sub["plan_tier"],
                "churn_reason": reason,
                "churned_arr_usd": sub["annual_recurring_revenue_usd"],
                "prior_6mo_avg_utilization_pct": round(avg_util, 2),
                "prior_6mo_avg_health_score": round(avg_health, 1),
                "severe_ticket_count": int(severe_ticket_count),
                "late_invoice_count": int(late_invoice_count),
                "ai_feature_adopter": bool(account["ai_feature_adopter"]),
            }
        )
        sub["subscription_status"] = "Churned"
        sub["end_date"] = churn_date
        account["is_active"] = False
        churn_counter += 1

df_churn_events = pd.DataFrame(churn_data)
df_subscriptions = pd.DataFrame(subscriptions_data)
df_accounts = pd.DataFrame(accounts_data)
print(f"churn_events: {len(df_churn_events)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Schema & Write Delta Tables

# COMMAND ----------

# =============================================================================
# CREATE SCHEMA & WRITE DELTA TABLES
# =============================================================================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")


def write_table(df_pd, table_name, mode=WRITE_MODE):
    df_spark = spark.createDataFrame(df_pd)
    (
        df_spark.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`")
    )
    print(f"  OK {table_name}: {df_spark.count()} rows written")


TABLES = {
    "accounts": df_accounts,
    "subscriptions": df_subscriptions,
    "product_usage": df_product_usage,
    "support_tickets": df_support_tickets,
    "invoices": df_invoices,
    "churn_events": df_churn_events,
}

print(f"Writing tables to `{CATALOG}`.`{SCHEMA}` with mode={WRITE_MODE}...")
for _table_name, _df in TABLES.items():
    write_table(_df, _table_name)
print("All tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verification

# COMMAND ----------

# =============================================================================
# VERIFICATION
# =============================================================================
C = f"`{CATALOG}`.`{SCHEMA}`"

print("=" * 60)
print("ROW COUNTS")
print("=" * 60)
for tbl in TABLES:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {C}.`{tbl}`").collect()[0]["n"]
    print(f"  {tbl:<24} {n:>8,}")

print()
print("=" * 60)
print("PATTERN VALIDATION")
print("=" * 60)

print("\n1. Churned subscriptions should have lower prior utilization.")
spark.sql(
    f"""
    SELECT s.subscription_status,
           ROUND(AVG(u.seat_utilization_pct), 1) AS avg_recent_utilization_pct,
           COUNT(DISTINCT s.subscription_id) AS subscriptions
    FROM {C}.`subscriptions` s
    JOIN {C}.`product_usage` u ON s.subscription_id = u.subscription_id
    WHERE u.usage_month >= '2025-07-01'
    GROUP BY s.subscription_status
    """
).show()

print("\n2. Annual billing should have lower churn than monthly billing.")
spark.sql(
    f"""
    SELECT billing_term,
           COUNT(*) AS subscriptions,
           SUM(CASE WHEN subscription_status = 'Churned' THEN 1 ELSE 0 END) AS churned,
           ROUND(SUM(CASE WHEN subscription_status = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS churn_rate_pct
    FROM {C}.`subscriptions`
    GROUP BY billing_term
    """
).show()

print("\n3. Enterprise accounts should have higher ARR and lower churn.")
spark.sql(
    f"""
    SELECT a.segment,
           ROUND(AVG(s.annual_recurring_revenue_usd), 2) AS avg_arr,
           ROUND(SUM(CASE WHEN s.subscription_status = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS churn_rate_pct
    FROM {C}.`subscriptions` s
    JOIN {C}.`accounts` a ON s.account_id = a.account_id
    GROUP BY a.segment
    ORDER BY avg_arr DESC
    """
).show()

print("\n4. Churned accounts should have more severe support tickets.")
spark.sql(
    f"""
    SELECT s.subscription_status,
           ROUND(AVG(CASE WHEN t.severity IN ('High', 'Critical') THEN 1 ELSE 0 END), 3) AS severe_ticket_share
    FROM {C}.`subscriptions` s
    LEFT JOIN {C}.`support_tickets` t ON s.subscription_id = t.subscription_id
    GROUP BY s.subscription_status
    """
).show()

print("\n5. AI feature adopters should have lower churn.")
spark.sql(
    f"""
    SELECT a.ai_feature_adopter,
           COUNT(*) AS subscriptions,
           ROUND(SUM(CASE WHEN s.subscription_status = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS churn_rate_pct
    FROM {C}.`subscriptions` s
    JOIN {C}.`accounts` a ON s.account_id = a.account_id
    GROUP BY a.ai_feature_adopter
    """
).show()

print("\n6. Late payment behavior should correlate with churn.")
spark.sql(
    f"""
    WITH invoice_lateness AS (
      SELECT subscription_id,
             SUM(CASE WHEN days_late >= 15 THEN 1 ELSE 0 END) AS late_invoice_count
      FROM {C}.`invoices`
      GROUP BY subscription_id
    )
    SELECT s.subscription_status,
           ROUND(AVG(l.late_invoice_count), 2) AS avg_late_invoice_count
    FROM {C}.`subscriptions` s
    JOIN invoice_lateness l ON s.subscription_id = l.subscription_id
    GROUP BY s.subscription_status
    """
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Create Metric Views

# COMMAND ----------

# =============================================================================
# CREATE METRIC VIEWS
# =============================================================================
print("Creating metric views...")

metric_views = {
    "mv_subscription_revenue": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.subscriptions

joins:
  - name: account
    source: {CATALOG}.{SCHEMA}.accounts
    on: source.account_id = account.account_id

dimensions:
  - name: Segment
    expr: account.segment
  - name: Industry
    expr: account.industry
  - name: Region
    expr: account.region
  - name: Plan Tier
    expr: plan_tier
  - name: Billing Term
    expr: billing_term
  - name: Subscription Status
    expr: subscription_status
  - name: AI Feature Adopter
    expr: account.ai_feature_adopter

measures:
  - name: Subscription Count
    expr: COUNT(1)
  - name: Active Subscription Count
    expr: COUNT(1) FILTER (WHERE subscription_status = 'Active')
  - name: Churned Subscription Count
    expr: COUNT(1) FILTER (WHERE subscription_status = 'Churned')
  - name: ARR
    expr: SUM(annual_recurring_revenue_usd)
  - name: MRR
    expr: SUM(monthly_recurring_revenue_usd)
  - name: Average ARR
    expr: AVG(annual_recurring_revenue_usd)
  - name: Seats Purchased
    expr: SUM(seats_purchased)
  - name: Logo Churn Rate Pct
    expr: COUNT(1) FILTER (WHERE subscription_status = 'Churned') * 100.0 / COUNT(1)
""",
    "mv_product_usage": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.product_usage

joins:
  - name: account
    source: {CATALOG}.{SCHEMA}.accounts
    on: source.account_id = account.account_id
  - name: subscription
    source: {CATALOG}.{SCHEMA}.subscriptions
    on: source.subscription_id = subscription.subscription_id

dimensions:
  - name: Usage Month
    expr: usage_month
  - name: Usage Year
    expr: usage_year
  - name: Segment
    expr: account.segment
  - name: Plan Tier
    expr: subscription.plan_tier
  - name: Billing Term
    expr: subscription.billing_term
  - name: AI Feature Enabled
    expr: ai_feature_enabled

measures:
  - name: Usage Row Count
    expr: COUNT(1)
  - name: Active Users
    expr: SUM(active_users)
  - name: Average Seat Utilization Pct
    expr: AVG(seat_utilization_pct)
  - name: Average Health Score
    expr: AVG(health_score)
  - name: Queries Run
    expr: SUM(queries_run)
  - name: Workflows Created
    expr: SUM(workflows_created)
  - name: AI Active Users
    expr: SUM(ai_feature_active_users)
  - name: AI Adoption Rate Pct
    expr: COUNT(1) FILTER (WHERE ai_feature_enabled = TRUE) * 100.0 / COUNT(1)
""",
    "mv_churn_risk": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.churn_events

joins:
  - name: account
    source: {CATALOG}.{SCHEMA}.accounts
    on: source.account_id = account.account_id
  - name: subscription
    source: {CATALOG}.{SCHEMA}.subscriptions
    on: source.subscription_id = subscription.subscription_id

dimensions:
  - name: Churn Month
    expr: DATE_TRUNC('MONTH', churn_date)
  - name: Churn Year
    expr: churn_year
  - name: Segment
    expr: segment
  - name: Plan Tier
    expr: plan_tier
  - name: Billing Term
    expr: billing_term
  - name: Churn Reason
    expr: churn_reason
  - name: AI Feature Adopter
    expr: ai_feature_adopter

measures:
  - name: Churn Event Count
    expr: COUNT(1)
  - name: Churned ARR
    expr: SUM(churned_arr_usd)
  - name: Average Prior Utilization Pct
    expr: AVG(prior_6mo_avg_utilization_pct)
  - name: Average Prior Health Score
    expr: AVG(prior_6mo_avg_health_score)
  - name: Severe Tickets Before Churn
    expr: SUM(severe_ticket_count)
  - name: Late Invoices Before Churn
    expr: SUM(late_invoice_count)
""",
}

for mv_name, yaml_body in metric_views.items():
    ddl = (
        f"CREATE OR REPLACE VIEW `{CATALOG}`.`{SCHEMA}`.`{mv_name}`\n"
        "WITH METRICS\n"
        "LANGUAGE YAML\n"
        "AS $$"
        + yaml_body
        + "$$"
    )
    spark.sql(ddl)
    print(f"  OK {mv_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Register Constraints & Comments

# COMMAND ----------

# =============================================================================
# REGISTER CONSTRAINTS AND COMMENTS
# =============================================================================
print("Registering constraints and comments...")

PRIMARY_KEYS = {
    "accounts": "account_id",
    "subscriptions": "subscription_id",
    "product_usage": "usage_id",
    "support_tickets": "ticket_id",
    "invoices": "invoice_id",
    "churn_events": "churn_event_id",
}

FOREIGN_KEYS = [
    ("subscriptions", "account_id", "accounts", "account_id"),
    ("product_usage", "account_id", "accounts", "account_id"),
    ("product_usage", "subscription_id", "subscriptions", "subscription_id"),
    ("support_tickets", "account_id", "accounts", "account_id"),
    ("support_tickets", "subscription_id", "subscriptions", "subscription_id"),
    ("invoices", "account_id", "accounts", "account_id"),
    ("invoices", "subscription_id", "subscriptions", "subscription_id"),
    ("churn_events", "account_id", "accounts", "account_id"),
    ("churn_events", "subscription_id", "subscriptions", "subscription_id"),
]

for table, pk in PRIMARY_KEYS.items():
    spark.sql(f"ALTER TABLE {C}.`{table}` ALTER COLUMN `{pk}` SET NOT NULL")
    spark.sql(f"ALTER TABLE {C}.`{table}` ADD CONSTRAINT pk_{table} PRIMARY KEY (`{pk}`)")

for table, col, ref_table, ref_col in FOREIGN_KEYS:
    spark.sql(
        f"ALTER TABLE {C}.`{table}` ADD CONSTRAINT fk_{table}_{col} "
        f"FOREIGN KEY (`{col}`) REFERENCES {C}.`{ref_table}` (`{ref_col}`)"
    )


def sql_text(value):
    return value.replace("'", "''")


TABLE_COMMENTS = {
    "accounts": "Fictional SaaS customer account dimension with segment, industry, region, CSM tier, and AI adoption flag.",
    "subscriptions": "Subscription dimension with plan, billing term, seats, MRR, ARR, status, and churn end date.",
    "product_usage": "Monthly usage facts. Low seat utilization and lower health scores increase synthetic churn risk.",
    "support_tickets": "Support ticket facts with severity, category, status, resolution time, CSAT, and escalation flag.",
    "invoices": "Invoice and payment facts. Late payment behavior is correlated with churn risk.",
    "churn_events": "Churn facts linked to subscriptions with churn reason, churned ARR, prior usage, support, and invoice signals.",
}

for table, comment in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {C}.`{table}` IS '{sql_text(comment)}'")

for table, df_pd in TABLES.items():
    for column in df_pd.columns:
        readable = column.replace("_", " ")
        spark.sql(
            f"ALTER TABLE {C}.`{table}` ALTER COLUMN `{column}` "
            f"COMMENT '{sql_text(readable)} for the synthetic SaaS churn dataset'"
        )

print("  OK constraints and comments registered")

print()
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print("  Tables      : accounts, subscriptions, product_usage, support_tickets, invoices, churn_events")
print("  Metric Views: mv_subscription_revenue, mv_product_usage, mv_churn_risk")
print("  Next    : Create a Genie space from these tables, or see notebooks/demo-data/README.md")
