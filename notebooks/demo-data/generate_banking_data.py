# Databricks notebook source

# MAGIC %md
# MAGIC # Horizon Bank — Synthetic Dataset Generator
# MAGIC
# MAGIC Generates **6 Delta tables** for the Horizon Bank Genie demo space.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---|---|
# MAGIC | `products` | 20 | Product catalog (static) |
# MAGIC | `branches` | 25 | Branch dimension |
# MAGIC | `customers` | 1,000 | Customer dimension |
# MAGIC | `accounts` | ~2,500 | Account dimension (FK → customers, products) |
# MAGIC | `transactions` | 10,000 | Primary fact table (2023–2025) |
# MAGIC | `service_requests` | 3,000 | Secondary fact table (2023–2025) |
# MAGIC
# MAGIC **Setup:** Edit the configuration variables below, then **Run All**.
# MAGIC
# MAGIC Seed: `42` — fully reproducible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION — Edit only this section before running
# =============================================================================
CATALOG = ""  # TODO: set to a Unity Catalog name before running
SCHEMA = "horizon_bank"
OVERWRITE_EXISTING = False

if not CATALOG:
    raise ValueError("Set CATALOG to a Unity Catalog name before running this notebook.")

WRITE_MODE = "overwrite" if OVERWRITE_EXISTING else "errorifexists"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports & Global Setup

# COMMAND ----------

import math

# =============================================================================
# IMPORTS & GLOBAL SETUP
# =============================================================================
import random
from calendar import monthrange
from datetime import date, timedelta

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

FIRST_NAMES = [
    "Alex",
    "Avery",
    "Casey",
    "Drew",
    "Jordan",
    "Morgan",
    "Parker",
    "Quinn",
    "Riley",
    "Taylor",
]
LAST_NAMES = [
    "Anderson",
    "Bennett",
    "Carter",
    "Diaz",
    "Ellis",
    "Foster",
    "Garcia",
    "Hayes",
    "Ibrahim",
    "Jensen",
]


def synthetic_name(index):
    first = FIRST_NAMES[(index - 1) % len(FIRST_NAMES)]
    last = LAST_NAMES[((index - 1) // len(FIRST_NAMES)) % len(LAST_NAMES)]
    return f"{first} {last}"

# Date range for fact tables
START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 12, 31)

# Ordered list of (year, month) tuples covering the 3-year window
MONTH_LIST = [(y, m) for y in [2023, 2024, 2025] for m in range(1, 13)]

# State → Region lookup
STATE_REGION = {
    "NY": "Northeast",
    "MA": "Northeast",
    "NJ": "Northeast",
    "CT": "Northeast",
    "PA": "Northeast",
    "FL": "Southeast",
    "GA": "Southeast",
    "NC": "Southeast",
    "SC": "Southeast",
    "VA": "Southeast",
    "TN": "Southeast",
    "IL": "Midwest",
    "OH": "Midwest",
    "MI": "Midwest",
    "IN": "Midwest",
    "WI": "Midwest",
    "MN": "Midwest",
    "CA": "West",
    "WA": "West",
    "OR": "West",
    "TX": "Southwest",
    "AZ": "Southwest",
    "NV": "Southwest",
    "NM": "Southwest",
    "CO": "Southwest",
}

# State pool for customer assignment (weighted toward NY, CA, TX, FL, IL)
CUSTOMER_STATES = [
    "NY",
    "CA",
    "TX",
    "FL",
    "IL",
    "NJ",
    "MA",
    "GA",
    "NC",
    "WA",
    "OH",
    "MI",
    "PA",
    "VA",
    "AZ",
    "CO",
    "SC",
    "IN",
    "WI",
    "OR",
]
STATE_WEIGHTS = [13, 12, 10, 10, 7, 6, 5, 4, 4, 4, 4, 3, 3, 3, 3, 2, 2, 2, 2, 1]

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table 1: Products

# COMMAND ----------

# =============================================================================
# TABLE 1: PRODUCTS (20 rows)
# =============================================================================

products_data = [
    # ── DEPOSIT ──────────────────────────────────────────────────────────────
    {
        "product_id": "PROD-001",
        "product_name": "Horizon Basic Checking",
        "product_category": "Deposit",
        "product_type": "Checking",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 0.00,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2010, 1, 1),
    },
    {
        "product_id": "PROD-002",
        "product_name": "Horizon Plus Checking",
        "product_category": "Deposit",
        "product_type": "Checking",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 0.01,
        "reward_program": "Cash Back",
        "min_balance_usd": 1500.0,
        "is_active": True,
        "launched_date": date(2012, 6, 1),
    },
    {
        "product_id": "PROD-003",
        "product_name": "Horizon Premier Checking",
        "product_category": "Deposit",
        "product_type": "Checking",
        "annual_fee_usd": 25.0,
        "base_interest_rate_pct": 0.05,
        "reward_program": "Points",
        "min_balance_usd": 5000.0,
        "is_active": True,
        "launched_date": date(2015, 3, 1),
    },
    {
        "product_id": "PROD-004",
        "product_name": "Horizon Business Checking",
        "product_category": "Deposit",
        "product_type": "Checking",
        "annual_fee_usd": 15.0,
        "base_interest_rate_pct": 0.00,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2013, 1, 1),
    },
    {
        "product_id": "PROD-005",
        "product_name": "Horizon Basic Savings",
        "product_category": "Deposit",
        "product_type": "Savings",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 0.50,
        "reward_program": "None",
        "min_balance_usd": 100.0,
        "is_active": True,
        "launched_date": date(2010, 1, 1),
    },
    {
        "product_id": "PROD-006",
        "product_name": "Horizon High-Yield Savings",
        "product_category": "Deposit",
        "product_type": "Savings",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 4.50,
        "reward_program": "None",
        "min_balance_usd": 1000.0,
        "is_active": True,
        "launched_date": date(2019, 7, 15),
    },
    {
        "product_id": "PROD-007",
        "product_name": "Horizon Money Market",
        "product_category": "Deposit",
        "product_type": "Savings",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 3.80,
        "reward_program": "None",
        "min_balance_usd": 2500.0,
        "is_active": True,
        "launched_date": date(2015, 1, 1),
    },
    {
        "product_id": "PROD-008",
        "product_name": "Horizon 12-Month CD",
        "product_category": "Deposit",
        "product_type": "Savings",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 5.10,
        "reward_program": "None",
        "min_balance_usd": 500.0,
        "is_active": True,
        "launched_date": date(2020, 1, 1),
    },
    {
        "product_id": "PROD-009",
        "product_name": "Horizon Student Savings",
        "product_category": "Deposit",
        "product_type": "Savings",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 1.00,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2018, 9, 1),
    },
    # ── CREDIT ───────────────────────────────────────────────────────────────
    {
        "product_id": "PROD-010",
        "product_name": "Horizon Rewards Visa",
        "product_category": "Credit",
        "product_type": "Credit Card",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 19.99,
        "reward_program": "Cash Back",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2011, 4, 1),
    },
    {
        "product_id": "PROD-011",
        "product_name": "Horizon Platinum Card",
        "product_category": "Credit",
        "product_type": "Credit Card",
        "annual_fee_usd": 95.0,
        "base_interest_rate_pct": 17.99,
        "reward_program": "Points",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2013, 1, 1),
    },
    {
        "product_id": "PROD-012",
        "product_name": "Horizon Travel Rewards Card",
        "product_category": "Credit",
        "product_type": "Credit Card",
        "annual_fee_usd": 195.0,
        "base_interest_rate_pct": 20.99,
        "reward_program": "Miles",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2016, 6, 15),
    },
    {
        "product_id": "PROD-013",
        "product_name": "Horizon Business Credit Card",
        "product_category": "Credit",
        "product_type": "Credit Card",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 21.99,
        "reward_program": "Cash Back",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2014, 3, 1),
    },
    # ── LENDING ──────────────────────────────────────────────────────────────
    {
        "product_id": "PROD-014",
        "product_name": "Horizon 30yr Fixed Mortgage",
        "product_category": "Lending",
        "product_type": "Mortgage",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 6.75,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2010, 1, 1),
    },
    {
        "product_id": "PROD-015",
        "product_name": "Horizon 15yr Fixed Mortgage",
        "product_category": "Lending",
        "product_type": "Mortgage",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 6.00,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2010, 1, 1),
    },
    {
        "product_id": "PROD-016",
        "product_name": "Horizon 5/1 ARM Mortgage",
        "product_category": "Lending",
        "product_type": "Mortgage",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 5.50,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2012, 1, 1),
    },
    {
        "product_id": "PROD-017",
        "product_name": "Horizon Auto Loan 48-Month",
        "product_category": "Lending",
        "product_type": "Auto Loan",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 5.25,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2010, 1, 1),
    },
    {
        "product_id": "PROD-018",
        "product_name": "Horizon Auto Loan 60-Month",
        "product_category": "Lending",
        "product_type": "Auto Loan",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 5.75,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2010, 1, 1),
    },
    {
        "product_id": "PROD-019",
        "product_name": "Horizon Home Equity Line",
        "product_category": "Lending",
        "product_type": "HELOC",
        "annual_fee_usd": 50.0,
        "base_interest_rate_pct": 7.50,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2011, 1, 1),
    },
    {
        "product_id": "PROD-020",
        "product_name": "Horizon Home Equity Loan",
        "product_category": "Lending",
        "product_type": "HELOC",
        "annual_fee_usd": 0.0,
        "base_interest_rate_pct": 7.00,
        "reward_program": "None",
        "min_balance_usd": 0.0,
        "is_active": True,
        "launched_date": date(2011, 6, 1),
    },
]

# Product lookup by type (for account → product assignment)
PRODUCTS_BY_TYPE = {}
for p in products_data:
    pt = p["product_type"]
    PRODUCTS_BY_TYPE.setdefault(pt, []).append(p["product_id"])

# Tier-biased product selection:
#   Standard → first product of type, Preferred → middle, Private Client → last
TIER_PRODUCT_IDX = {"Standard": 0, "Preferred": 1, "Private Client": -1}


def pick_product(account_type, tier):
    """Return a product_id for the given account_type and customer tier."""
    # Map account_type to product_type
    atype_to_ptype = {
        "Checking": "Checking",
        "Savings": "Savings",
        "Credit Card": "Credit Card",
        "Mortgage": "Mortgage",
        "Auto Loan": "Auto Loan",
        "Home Equity": "HELOC",
    }
    ptype = atype_to_ptype.get(account_type, "Checking")
    candidates = PRODUCTS_BY_TYPE.get(ptype, ["PROD-001"])
    idx = TIER_PRODUCT_IDX.get(tier, 0)
    # Add small random variation so not all same-tier customers get identical products
    jitter = random.choice([0, 0, 0, 1]) if len(candidates) > 1 else 0
    return candidates[min(idx + jitter, len(candidates) - 1)]


df_products = pd.DataFrame(products_data)
print(f"products: {len(df_products)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table 2: Branches

# COMMAND ----------

# =============================================================================
# TABLE 2: BRANCHES (25 rows)
# =============================================================================


def _branch(bid, name, btype, region, state, city, opened, cost, hc):
    return {
        "branch_id": bid,
        "branch_name": name,
        "branch_type": btype,
        "region": region,
        "state": state,
        "city": city,
        "opened_date": date(*opened),
        "monthly_operating_cost_usd": float(cost),
        "headcount": hc,
        "is_active": True,
    }


branches_data = [
    # ── NORTHEAST (6) ────────────────────────────────────────────────────────
    _branch(
        "BRNCH-001",
        "Manhattan Financial District Branch",
        "Full Service",
        "Northeast",
        "NY",
        "New York",
        (2005, 3, 1),
        145000,
        32,
    ),
    _branch(
        "BRNCH-002",
        "Brooklyn Heights Branch",
        "Full Service",
        "Northeast",
        "NY",
        "Brooklyn",
        (2008, 7, 1),
        118000,
        25,
    ),
    _branch(
        "BRNCH-003",
        "Long Island Branch",
        "Limited Service",
        "Northeast",
        "NY",
        "Garden City",
        (2011, 4, 1),
        72000,
        13,
    ),
    _branch(
        "BRNCH-004",
        "Newark Main Branch",
        "Full Service",
        "Northeast",
        "NJ",
        "Newark",
        (2006, 9, 1),
        105000,
        22,
    ),
    _branch(
        "BRNCH-005",
        "Princeton Branch",
        "Limited Service",
        "Northeast",
        "NJ",
        "Princeton",
        (2014, 2, 1),
        65000,
        11,
    ),
    _branch(
        "BRNCH-006",
        "Boston Financial Branch",
        "Full Service",
        "Northeast",
        "MA",
        "Boston",
        (2007, 5, 1),
        132000,
        28,
    ),
    # ── SOUTHEAST (6) — injected: 20% higher avg txn value ──────────────────
    _branch(
        "BRNCH-007",
        "Miami South Branch",
        "Full Service",
        "Southeast",
        "FL",
        "Miami",
        (2006, 1, 1),
        138000,
        30,
    ),
    _branch(
        "BRNCH-008",
        "Orlando Central Branch",
        "Full Service",
        "Southeast",
        "FL",
        "Orlando",
        (2009, 3, 1),
        112000,
        24,
    ),
    _branch(
        "BRNCH-009",
        "Tampa Bay Branch",
        "Limited Service",
        "Southeast",
        "FL",
        "Tampa",
        (2013, 6, 1),
        68000,
        12,
    ),
    _branch(
        "BRNCH-010",
        "Atlanta Midtown Branch",
        "Full Service",
        "Southeast",
        "GA",
        "Atlanta",
        (2007, 11, 1),
        125000,
        27,
    ),
    _branch(
        "BRNCH-011",
        "Savannah Branch",
        "Drive-Through",
        "Southeast",
        "GA",
        "Savannah",
        (2015, 4, 1),
        32000,
        6,
    ),
    _branch(
        "BRNCH-012",
        "Charlotte Branch",
        "Full Service",
        "Southeast",
        "NC",
        "Charlotte",
        (2010, 8, 1),
        108000,
        23,
    ),
    # ── MIDWEST (5) ──────────────────────────────────────────────────────────
    _branch(
        "BRNCH-013",
        "Chicago Loop Branch",
        "Full Service",
        "Midwest",
        "IL",
        "Chicago",
        (2004, 6, 1),
        150000,
        35,
    ),
    _branch(
        "BRNCH-014",
        "Chicago North Shore Branch",
        "Limited Service",
        "Midwest",
        "IL",
        "Evanston",
        (2010, 1, 1),
        74000,
        14,
    ),
    _branch(
        "BRNCH-015",
        "Chicago South Branch",
        "Drive-Through",
        "Midwest",
        "IL",
        "Oak Park",
        (2012, 9, 1),
        38000,
        7,
    ),
    _branch(
        "BRNCH-016",
        "Columbus Branch",
        "Full Service",
        "Midwest",
        "OH",
        "Columbus",
        (2009, 4, 1),
        98000,
        20,
    ),
    _branch(
        "BRNCH-017",
        "Detroit Branch",
        "Full Service",
        "Midwest",
        "MI",
        "Detroit",
        (2011, 2, 1),
        95000,
        19,
    ),
    # ── WEST (4) ─────────────────────────────────────────────────────────────
    _branch(
        "BRNCH-018",
        "Los Angeles Downtown Branch",
        "Full Service",
        "West",
        "CA",
        "Los Angeles",
        (2005, 9, 1),
        148000,
        33,
    ),
    _branch(
        "BRNCH-019",
        "San Francisco Financial Branch",
        "Full Service",
        "West",
        "CA",
        "San Francisco",
        (2006, 3, 1),
        152000,
        34,
    ),
    _branch(
        "BRNCH-020",
        "San Diego Branch",
        "Limited Service",
        "West",
        "CA",
        "San Diego",
        (2012, 5, 1),
        79000,
        15,
    ),
    _branch(
        "BRNCH-021",
        "Seattle Branch",
        "Full Service",
        "West",
        "WA",
        "Seattle",
        (2010, 7, 1),
        115000,
        24,
    ),
    # ── SOUTHWEST (4) ────────────────────────────────────────────────────────
    _branch(
        "BRNCH-022",
        "Houston Main Branch",
        "Full Service",
        "Southwest",
        "TX",
        "Houston",
        (2006, 2, 1),
        128000,
        27,
    ),
    _branch(
        "BRNCH-023",
        "Dallas Branch",
        "Full Service",
        "Southwest",
        "TX",
        "Dallas",
        (2008, 10, 1),
        122000,
        26,
    ),
    _branch(
        "BRNCH-024",
        "Austin Branch",
        "Limited Service",
        "Southwest",
        "TX",
        "Austin",
        (2014, 6, 1),
        71000,
        13,
    ),
    _branch(
        "BRNCH-025",
        "Phoenix Branch",
        "Full Service",
        "Southwest",
        "AZ",
        "Phoenix",
        (2009, 1, 1),
        103000,
        21,
    ),
]

df_branches = pd.DataFrame(branches_data)

# Branch lookup helpers
BRANCH_IDS = [b["branch_id"] for b in branches_data]
BRANCHES_BY_REGION = {}
for b in branches_data:
    BRANCHES_BY_REGION.setdefault(b["region"], []).append(b["branch_id"])
BRANCH_REGION = {b["branch_id"]: b["region"] for b in branches_data}

print(f"branches: {len(df_branches)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Table 3: Customers

# COMMAND ----------

# =============================================================================
# TABLE 3: CUSTOMERS (1,000 rows)
# =============================================================================

# Tier → credit_score_band weights
CREDIT_SCORE_BANDS = ["Poor", "Fair", "Good", "Very Good", "Exceptional"]
CREDIT_WEIGHTS = {
    "Standard": [5, 20, 35, 30, 10],
    "Preferred": [1, 8, 28, 42, 21],
    "Private Client": [0, 2, 12, 40, 46],
}

# Tier → has_* flag probabilities
HAS_FLAGS = {
    "Standard": {
        "checking": 0.70,
        "savings": 0.62,
        "credit_card": 0.38,
        "mortgage": 0.18,
    },
    "Preferred": {
        "checking": 0.88,
        "savings": 0.78,
        "credit_card": 0.62,
        "mortgage": 0.34,
    },
    "Private Client": {
        "checking": 0.97,
        "savings": 0.90,
        "credit_card": 0.80,
        "mortgage": 0.60,
    },
}

customers_data = []
for i in range(1, 1001):
    cid = f"CUST-{i:04d}"
    tier = random.choices(
        ["Standard", "Preferred", "Private Client"], weights=[60, 30, 10]
    )[0]

    # Segment correlated with tier
    if tier == "Private Client":
        segment = random.choices(
            ["Retail", "Small Business", "Wealth Management"], weights=[15, 30, 55]
        )[0]
    elif tier == "Preferred":
        segment = random.choices(
            ["Retail", "Small Business", "Wealth Management"], weights=[55, 35, 10]
        )[0]
    else:
        segment = random.choices(
            ["Retail", "Small Business", "Wealth Management"], weights=[75, 22, 3]
        )[0]

    state = random.choices(CUSTOMER_STATES, weights=STATE_WEIGHTS)[0]
    region = STATE_REGION[state]

    # Pick branch in same region
    branch_id = random.choice(BRANCHES_BY_REGION[region])

    flags = HAS_FLAGS[tier]
    has_checking = random.random() < flags["checking"]
    has_savings = random.random() < flags["savings"]
    has_credit_card = random.random() < flags["credit_card"]
    has_mortgage = random.random() < flags["mortgage"]

    # Ensure at least one deposit account
    if not has_checking and not has_savings:
        has_checking = True

    # Age band correlated with segment/tier
    age_weights = [8, 20, 22, 20, 17, 13]
    age_band = random.choices(
        ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"], weights=age_weights
    )[0]

    # Income band correlated with tier
    if tier == "Private Client":
        income_band = random.choices(
            ["<$50K", "$50K-$100K", "$100K-$200K", "$200K+"], weights=[2, 10, 35, 53]
        )[0]
    elif tier == "Preferred":
        income_band = random.choices(
            ["<$50K", "$50K-$100K", "$100K-$200K", "$200K+"], weights=[8, 28, 45, 19]
        )[0]
    else:
        income_band = random.choices(
            ["<$50K", "$50K-$100K", "$100K-$200K", "$200K+"], weights=[28, 38, 26, 8]
        )[0]

    credit_score_band = random.choices(
        CREDIT_SCORE_BANDS, weights=CREDIT_WEIGHTS[tier]
    )[0]

    # customer_since_date: 2018–2024
    since_days = random.randint(0, (date(2024, 12, 31) - date(2018, 1, 1)).days)
    since_date = date(2018, 1, 1) + timedelta(days=since_days)

    customers_data.append(
        {
            "customer_id": cid,
            "customer_name": synthetic_name(i),
            "age_band": age_band,
            "income_band": income_band,
            "customer_segment": segment,
            "relationship_tier": tier,
            "state": state,
            "region": region,
            "primary_branch_id": branch_id,
            "acquisition_channel": random.choices(
                ["Branch", "Online", "Mobile", "Referral"], weights=[30, 35, 25, 10]
            )[0],
            "customer_since_date": since_date,
            "is_active": random.random() < 0.94,
            "has_checking": has_checking,
            "has_savings": has_savings,
            "has_credit_card": has_credit_card,
            "has_mortgage": has_mortgage,
            "credit_score_band": credit_score_band,
        }
    )

df_customers = pd.DataFrame(customers_data)
print(f"customers: {len(df_customers)} rows")
print(df_customers["relationship_tier"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Table 4: Accounts

# COMMAND ----------

# =============================================================================
# TABLE 4: ACCOUNTS (~2,500 rows)
# =============================================================================
# Injected pattern: Private Client customers have 3x the avg balance.
# Achieved via tier-scaled lognormal distributions for deposit accounts.
#
# Balance parameters (lognormal: mean = exp(mu + sigma^2/2)):
#   Standard deposit:      mu=7.8, sigma=0.6  → mean ≈ $2,919
#   Preferred deposit:     mu=8.4, sigma=0.6  → mean ≈ $5,297
#   Private Client deposit:mu=8.9, sigma=0.6  → mean ≈ $8,789  (≈3x Standard)

BALANCE_PARAMS = {
    # (mu, sigma) for lognormal draws — deposit accounts
    "Standard": {"deposit": (7.8, 0.6)},
    "Preferred": {"deposit": (8.4, 0.6)},
    "Private Client": {"deposit": (8.9, 0.6)},
}


def deposit_balance(tier):
    mu, sigma = BALANCE_PARAMS[tier]["deposit"]
    return round(float(np.random.lognormal(mu, sigma)), 2)


def credit_card_params(tier):
    """Returns (credit_limit, current_balance)."""
    if tier == "Standard":
        limit = round(random.uniform(3000, 12000), 2)
    elif tier == "Preferred":
        limit = round(random.uniform(10000, 30000), 2)
    else:
        limit = round(random.uniform(25000, 100000), 2)
    utilization = random.uniform(0.05, 0.70)
    balance = round(limit * utilization, 2)
    return limit, balance


def lending_balance(account_type, tier):
    ranges = {
        "Mortgage": {
            "Standard": (100000, 400000),
            "Preferred": (200000, 700000),
            "Private Client": (400000, 2000000),
        },
        "Auto Loan": {
            "Standard": (8000, 30000),
            "Preferred": (20000, 55000),
            "Private Client": (40000, 120000),
        },
        "Home Equity": {
            "Standard": (20000, 80000),
            "Preferred": (50000, 150000),
            "Private Client": (100000, 500000),
        },
    }
    lo, hi = ranges[account_type][tier]
    return round(random.uniform(lo, hi), 2)


accounts_data = []
account_counter = 1

# Track which customers are "high value" (Private Client = top 10%)
# Used later for transaction concentration pattern
HIGH_VALUE_CUSTOMERS = set(
    c["customer_id"]
    for c in customers_data
    if c["relationship_tier"] == "Private Client"
)

for cust in customers_data:
    cid = cust["customer_id"]
    tier = cust["relationship_tier"]
    since = cust["customer_since_date"]

    # Collect account types this customer should have
    account_types = []
    if cust["has_checking"]:
        account_types.append("Checking")
    if cust["has_savings"]:
        account_types.append("Savings")
    if cust["has_credit_card"]:
        account_types.append("Credit Card")
    if cust["has_mortgage"]:
        account_types.append("Mortgage")

    # Extra accounts (Auto Loan, Home Equity) by tier
    auto_prob = {"Standard": 0.15, "Preferred": 0.25, "Private Client": 0.40}
    heloc_prob = {"Standard": 0.10, "Preferred": 0.20, "Private Client": 0.35}
    if random.random() < auto_prob[tier]:
        account_types.append("Auto Loan")
    if random.random() < heloc_prob[tier]:
        account_types.append("Home Equity")

    primary_set = False
    for idx, atype in enumerate(account_types):
        aid = f"ACCT-{account_counter:05d}"
        account_counter += 1

        is_primary = (not primary_set) and (atype == "Checking")
        if atype == "Checking" and not primary_set:
            primary_set = True
        # If no checking, make first account primary
        if idx == 0 and not primary_set:
            is_primary = True
            primary_set = True

        product_id = pick_product(atype, tier)

        # Derive product base_interest_rate
        prod = next(p for p in products_data if p["product_id"] == product_id)
        base_rate = prod["base_interest_rate_pct"]
        rate_noise = round(random.uniform(-0.25, 0.25), 4)
        interest_rate = max(0.0, round(base_rate + rate_noise, 4))

        # Open date: within 6 months after customer_since_date, up to 2024
        max_open = date(2024, 12, 31)
        open_offset = random.randint(0, 180)
        open_date = min(since + timedelta(days=open_offset), max_open)

        # Status: Active(78%), Dormant(10%), Closed(8%), Delinquent(4%)
        status = random.choices(
            ["Active", "Dormant", "Closed", "Delinquent"], weights=[78, 10, 8, 4]
        )[0]

        close_date = None
        if status == "Closed":
            max_offset = max(1, (date(2024, 12, 31) - open_date).days)
            close_offset = random.randint(min(180, max_offset), max_offset)
            close_date = open_date + timedelta(days=close_offset)

        # Balance by account type
        credit_limit = None
        if atype in ("Checking", "Savings"):
            balance = deposit_balance(tier)
        elif atype == "Credit Card":
            credit_limit, balance = credit_card_params(tier)
        else:  # Mortgage, Auto Loan, Home Equity
            balance = lending_balance(atype, tier)

        accounts_data.append(
            {
                "account_id": aid,
                "customer_id": cid,
                "product_id": product_id,
                "account_type": atype,
                "open_date": open_date,
                "close_date": close_date,
                "status": status,
                "current_balance_usd": balance,
                "credit_limit_usd": credit_limit,
                "interest_rate_pct": interest_rate,
                "is_primary_account": is_primary,
            }
        )

df_accounts = pd.DataFrame(accounts_data)
print(f"accounts: {len(df_accounts)} rows")
print(df_accounts["account_type"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Table 5: Transactions

# COMMAND ----------

# =============================================================================
# TABLE 5: TRANSACTIONS (10,000 rows)
# =============================================================================
# Injected patterns:
#  1. Nov/Dec volume spike: +45% transactions vs monthly average
#  2. Q2 2024 deposit dip: deposit amounts -15% in Apr/May/Jun 2024
#  3. Mobile channel growth: 25% share (Jan 2023) → 48% share (Dec 2025)
#  4. Fee revenue growth: wire + overdraft fees +20% YoY
#  5. Top 10% customers (Private Client) = ~35% of deposit volume
#     Achieved by giving them 35% of deposit transaction count.

# ── Month weights (Nov/Dec = 1.45x) ──────────────────────────────────────────
month_weights = []
for y, m in MONTH_LIST:
    month_weights.append(1.45 if m in (11, 12) else 1.0)
total_w = sum(month_weights)  # 38.7
txns_per_month_raw = [w / total_w * 10000 for w in month_weights]
txns_per_month = [int(round(x)) for x in txns_per_month_raw]
diff = 10000 - sum(txns_per_month)
txns_per_month[-1] += diff  # absorb rounding into Dec 2025

# ── Account pools for sampling ────────────────────────────────────────────────
all_account_ids = [a["account_id"] for a in accounts_data]
account_cust_map = {a["account_id"]: a["customer_id"] for a in accounts_data}
account_type_map = {a["account_id"]: a["account_type"] for a in accounts_data}
account_region_map = {}
cust_region_map = {c["customer_id"]: c["region"] for c in customers_data}
cust_branch_map = {c["customer_id"]: c["primary_branch_id"] for c in customers_data}
for a in accounts_data:
    account_region_map[a["account_id"]] = cust_region_map[a["customer_id"]]

# Separate account pools
hv_accounts = [
    a["account_id"]
    for a in accounts_data
    if account_cust_map[a["account_id"]] in HIGH_VALUE_CUSTOMERS
]
reg_accounts = [
    a["account_id"]
    for a in accounts_data
    if account_cust_map[a["account_id"]] not in HIGH_VALUE_CUSTOMERS
]

# ── Transaction type weights by account type ──────────────────────────────────
TXN_TYPES = {
    "Checking": (
        ["Deposit", "Withdrawal", "Transfer", "Payment", "Fee", "Interest"],
        [25, 25, 20, 15, 8, 7],
    ),
    "Savings": (
        ["Deposit", "Withdrawal", "Transfer", "Interest", "Fee"],
        [35, 15, 20, 25, 5],
    ),
    "Credit Card": (["Purchase", "Payment", "Fee", "Interest"], [55, 30, 10, 5]),
    "Mortgage": (["Payment", "Fee", "Interest"], [80, 10, 10]),
    "Auto Loan": (["Payment", "Fee", "Interest"], [80, 10, 10]),
    "Home Equity": (["Payment", "Withdrawal", "Fee", "Interest"], [65, 20, 10, 5]),
}

MERCHANT_CATS = [
    "Groceries",
    "Travel",
    "Dining",
    "Gas",
    "Retail",
    "Healthcare",
    "Entertainment",
]
MERCHANT_WEIGHTS = [20, 12, 18, 10, 17, 13, 10]


def channel_shares(month_idx):
    """Linear interpolation: mobile grows 25%→48%, branch/online/ATM shrink."""
    t = month_idx / 35.0
    mobile = 0.25 + 0.23 * t
    branch = 0.30 - 0.10 * t
    atm = 0.20 - 0.05 * t
    online = 0.20 - 0.08 * t
    wire = 0.05
    return [mobile, branch, atm, online, wire]


CHANNELS = ["Mobile", "Branch", "ATM", "Online", "Wire"]


def fee_amount(txn_type, channel, year):
    """Generate fee_usd with 20% YoY growth starting from 2023 base."""
    year_mult = 1.0 + 0.20 * (year - 2023)  # 1.0 / 1.2 / 1.44
    if channel == "Wire":
        return round(25.0 * year_mult, 2)
    if txn_type in ("Withdrawal", "Transfer") and random.random() < 0.04:
        # Occasional overdraft/service fee
        return round(35.0 * year_mult, 2)
    if txn_type == "Fee":
        # Explicit fee transaction
        base = random.choice([10.0, 15.0, 25.0, 35.0])
        return round(base * year_mult, 2)
    return 0.0


def amount_for(txn_type, account_type, is_high_value, year, month):
    """Generate amount_usd (always positive)."""
    if account_type == "Mortgage":
        # Monthly payment ~$1,500–$5,000
        return round(random.uniform(1200, 5000), 2)
    if account_type == "Auto Loan":
        return round(random.uniform(300, 800), 2)
    if account_type == "Home Equity":
        return round(random.uniform(500, 3000), 2)
    if account_type == "Credit Card":
        if txn_type == "Purchase":
            return round(random.uniform(10, 500), 2)
        if txn_type == "Payment":
            return round(random.uniform(50, 2000), 2)
        if txn_type == "Interest":
            return round(random.uniform(20, 200), 2)
        return round(random.uniform(10, 100), 2)

    # Checking / Savings
    if txn_type == "Deposit":
        if is_high_value:
            amt = round(float(np.random.lognormal(8.3, 0.7)), 2)  # mean ≈ $6,000
        else:
            amt = round(float(np.random.lognormal(6.8, 0.7)), 2)  # mean ≈ $1,200
        # Q2 2024 deposit dip: -15%
        if year == 2024 and month in (4, 5, 6):
            amt *= 0.85
        return round(amt, 2)
    if txn_type == "Withdrawal":
        return round(float(np.random.lognormal(6.2, 0.8)), 2)
    if txn_type == "Transfer":
        return round(float(np.random.lognormal(7.0, 0.7)), 2)
    if txn_type == "Payment":
        return round(random.uniform(50, 1500), 2)
    if txn_type == "Interest":
        return round(random.uniform(1, 80), 2)
    if txn_type == "Fee":
        return 0.0  # fee captured in fee_usd column
    return round(random.uniform(5, 200), 2)


# ── Running balance tracker per account ───────────────────────────────────────
# Initialize each account with a starting balance slightly above current_balance_usd
account_running_balance = {}
for a in accounts_data:
    account_running_balance[a["account_id"]] = a[
        "current_balance_usd"
    ] * random.uniform(0.8, 1.2)

# ── Generate transactions ─────────────────────────────────────────────────────
transactions_data = []
txn_counter = 1

# Pre-compute high-value deposit allocation:
# Top 10% (Private Client) accounts for ~35% of deposit transactions.
# We track this by biasing account selection for Deposit transactions.

for month_idx, (year, month) in enumerate(MONTH_LIST):
    n_txns = txns_per_month[month_idx]
    shares = channel_shares(month_idx)
    max_day = monthrange(year, month)[1]

    for _ in range(n_txns):
        # Pick transaction date within month
        txn_date = date(year, month, random.randint(1, max_day))

        # Select account — for deposits: 35% chance of drawing from high-value pool
        # For other types: draw proportionally from all accounts
        if hv_accounts and random.random() < 0.35:
            acct_id = random.choice(hv_accounts)
        else:
            acct_id = random.choice(reg_accounts if reg_accounts else all_account_ids)

        cust_id = account_cust_map[acct_id]
        atype = account_type_map[acct_id]
        region = account_region_map[acct_id]
        is_hv = cust_id in HIGH_VALUE_CUSTOMERS

        # Transaction type
        types_list, type_weights = TXN_TYPES.get(atype, TXN_TYPES["Checking"])
        txn_type = random.choices(types_list, weights=type_weights)[0]

        # Channel — weighted by time, restricted by transaction type
        if txn_type in ("Interest", "Fee") and atype in ("Mortgage", "Auto Loan"):
            channel = "Online"
        else:
            channel = random.choices(CHANNELS, weights=shares)[0]

        # Branch ID — NULL for digital channels
        branch_id = None
        if channel == "Branch":
            branch_id = cust_branch_map.get(cust_id)

        # Amount
        amt = amount_for(txn_type, atype, is_hv, year, month)
        if amt < 0:
            amt = 0.01

        # Southeast 20% transaction value premium (branch transactions only)
        if region == "Southeast" and branch_id is not None:
            amt = round(amt * 1.20, 2)

        # Fee
        fee = fee_amount(txn_type, channel, year)

        # Running balance
        prev_bal = account_running_balance[acct_id]
        if txn_type in ("Deposit", "Interest"):
            new_bal = prev_bal + amt
        elif txn_type == "Fee":
            new_bal = prev_bal - fee
        else:
            new_bal = prev_bal - amt - fee
        account_running_balance[acct_id] = new_bal
        bal_after = round(new_bal, 2)

        # Merchant category (credit card purchases only)
        merchant_cat = None
        if atype == "Credit Card" and txn_type == "Purchase":
            merchant_cat = random.choices(MERCHANT_CATS, weights=MERCHANT_WEIGHTS)[0]

        # International: rare, higher for travel
        intl_prob = 0.08 if merchant_cat == "Travel" else 0.015
        is_intl = random.random() < intl_prob

        # Status & flag
        status = random.choices(["Posted", "Pending", "Reversed"], weights=[95, 3, 2])[
            0
        ]
        is_flagged = random.random() < 0.005

        transactions_data.append(
            {
                "transaction_id": f"TXN-{txn_counter:07d}",
                "transaction_date": txn_date,
                "transaction_year": year,
                "transaction_month": month,
                "transaction_quarter": (month - 1) // 3 + 1,
                "account_id": acct_id,
                "customer_id": cust_id,
                "branch_id": branch_id,
                "transaction_type": txn_type,
                "channel": channel,
                "amount_usd": max(0.0, amt),
                "fee_usd": max(0.0, fee),
                "balance_after_usd": bal_after,
                "merchant_category": merchant_cat,
                "is_international": is_intl,
                "status": status,
                "is_flagged": is_flagged,
            }
        )
        txn_counter += 1

# Update accounts.current_balance_usd to reflect final running balance
final_balance_map = dict(account_running_balance)
for a in accounts_data:
    a["current_balance_usd"] = round(final_balance_map[a["account_id"]], 2)
df_accounts = pd.DataFrame(accounts_data)  # rebuild with updated balances

df_transactions = pd.DataFrame(transactions_data)
print(f"transactions: {len(df_transactions)} rows")
print(df_transactions["transaction_type"].value_counts().to_dict())
print(df_transactions["channel"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Table 6: Service Requests

# COMMAND ----------

# =============================================================================
# TABLE 6: SERVICE REQUESTS (3,000 rows)
# =============================================================================
# Injected pattern: Complaint category spikes in Jan 2024 (+80% overall requests)
# to simulate a system outage narrative.

# Month weights: Jan 2024 = 1.80x, all others = 1.0x
sr_month_weights = [1.80 if (y == 2024 and m == 1) else 1.0 for (y, m) in MONTH_LIST]
total_sr_w = sum(sr_month_weights)  # 36.8
sr_per_month_raw = [w / total_sr_w * 3000 for w in sr_month_weights]
sr_per_month = [int(round(x)) for x in sr_per_month_raw]
sr_diff = 3000 - sum(sr_per_month)
sr_per_month[-1] += sr_diff

SR_CHANNELS = ["Phone", "Chat", "Branch", "App"]
SR_CATEGORIES = [
    "Account Inquiry",
    "Dispute",
    "Complaint",
    "Product Inquiry",
    "Technical Issue",
]
SR_STATUSES = ["Resolved", "Open", "Escalated"]

all_customer_ids = [c["customer_id"] for c in customers_data]

service_requests_data = []
sr_counter = 1

for month_idx, (year, month) in enumerate(MONTH_LIST):
    n_srs = sr_per_month[month_idx]
    is_jan2024 = year == 2024 and month == 1
    max_day = monthrange(year, month)[1]

    for _ in range(n_srs):
        req_date = date(year, month, random.randint(1, max_day))
        cust_id = random.choice(all_customer_ids)

        # Jan 2024 complaint spike: complaints elevated to 55% vs normal 15%
        if is_jan2024:
            cat_weights = [10, 12, 55, 13, 10]
        else:
            cat_weights = [35, 15, 15, 20, 15]

        category = random.choices(SR_CATEGORIES, weights=cat_weights)[0]
        channel = random.choices(SR_CHANNELS, weights=[35, 30, 15, 20])[0]

        # Branch for branch-channel requests
        branch_id = None
        if channel == "Branch":
            region = cust_region_map.get(cust_id, "Northeast")
            branch_id = random.choice(BRANCHES_BY_REGION[region])

        status = random.choices(SR_STATUSES, weights=[72, 18, 10])[0]

        # Resolution time: NULL if open/escalated
        res_days = None
        if status == "Resolved":
            if category == "Complaint":
                res_days = random.randint(3, 21)
            elif category == "Dispute":
                res_days = random.randint(5, 30)
            else:
                res_days = random.randint(0, 5)
        elif status == "Escalated":
            res_days = random.randint(10, 45)

        # Satisfaction score: NULL if not resolved; lower for complaints
        sat_score = None
        if status == "Resolved":
            if category == "Complaint":
                sat_score = random.choices(
                    [1, 2, 3, 4, 5], weights=[20, 30, 25, 15, 10]
                )[0]
            else:
                sat_score = random.choices(
                    [1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30]
                )[0]
        elif status == "Escalated" and random.random() < 0.3:
            sat_score = random.choices([1, 2, 3], weights=[50, 35, 15])[0]

        service_requests_data.append(
            {
                "request_id": f"SR-{sr_counter:06d}",
                "request_date": req_date,
                "request_year": year,
                "request_month": month,
                "customer_id": cust_id,
                "branch_id": branch_id,
                "channel": channel,
                "category": category,
                "status": status,
                "resolution_time_days": res_days,
                "satisfaction_score": sat_score,
                "is_resolved": status == "Resolved",
            }
        )
        sr_counter += 1

df_service_requests = pd.DataFrame(service_requests_data)
print(f"service_requests: {len(df_service_requests)} rows")
print(df_service_requests["category"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Create Schema & Write Delta Tables

# COMMAND ----------

# =============================================================================
# CREATE SCHEMA & WRITE DELTA TABLES
# =============================================================================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")


def write_table(df_pd, table_name, mode=WRITE_MODE):
    """Convert pandas → Spark and write as Delta table."""
    df_spark = spark.createDataFrame(df_pd)
    (
        df_spark.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`")
    )
    print(f"  ✓ {table_name}: {df_spark.count()} rows written")


print(f"Writing tables to `{CATALOG}`.`{SCHEMA}` with mode={WRITE_MODE}...")
write_table(df_products, "products")
write_table(df_branches, "branches")
write_table(df_customers, "customers")
write_table(df_accounts, "accounts")
write_table(df_transactions, "transactions")
write_table(df_service_requests, "service_requests")
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
for tbl in [
    "products",
    "branches",
    "customers",
    "accounts",
    "transactions",
    "service_requests",
]:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {C}.`{tbl}`").collect()[0]["n"]
    print(f"  {tbl:<20} {n:>6,}")

print()
print("=" * 60)
print("PATTERN VALIDATION")
print("=" * 60)

# 1. Nov/Dec spike
q1 = spark.sql(
    f"""
    SELECT
        CASE WHEN transaction_month IN (11,12) THEN 'Nov/Dec' ELSE 'Other' END AS period,
        COUNT(*) AS txn_count,
        ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS share
    FROM {C}.`transactions`
    GROUP BY 1
"""
)
print("\n1. Nov/Dec volume share (target: ~23% of all transactions):")
q1.show()

# 2. Q2 2024 deposit dip
q2 = spark.sql(
    f"""
    SELECT transaction_year AS yr, transaction_quarter AS qtr,
           ROUND(AVG(amount_usd),2) AS avg_deposit_amt,
           COUNT(*) AS cnt
    FROM {C}.`transactions`
    WHERE transaction_type = 'Deposit'
      AND transaction_year IN (2023,2024)
    GROUP BY 1,2
    ORDER BY 1,2
"""
)
print(
    "\n2. Avg deposit amount by year/quarter (Q2 2024 should be ~15% below Q1/Q3 2024):"
)
q2.show()

# 3. Mobile channel growth
q3 = spark.sql(
    f"""
    SELECT transaction_year AS yr,
           ROUND(SUM(CASE WHEN channel='Mobile' THEN 1 ELSE 0 END)*100.0/COUNT(*),1)
               AS mobile_pct
    FROM {C}.`transactions`
    GROUP BY 1
    ORDER BY 1
"""
)
print("\n3. Mobile share by year (target: 25%→~35%→~48% trend):")
q3.show()

# 4. Top 10% customer deposit concentration
q4 = spark.sql(
    f"""
    WITH cust_deposits AS (
        SELECT customer_id,
               SUM(amount_usd) AS total_deposits
        FROM {C}.`transactions`
        WHERE transaction_type = 'Deposit'
        GROUP BY customer_id
    ),
    ranked AS (
        SELECT *,
               NTILE(10) OVER (ORDER BY total_deposits DESC) AS decile
        FROM cust_deposits
    )
    SELECT
        CASE WHEN decile = 1 THEN 'Top 10%' ELSE 'Other 90%' END AS segment,
        ROUND(SUM(total_deposits),2)                              AS deposit_volume,
        ROUND(SUM(total_deposits)*100.0/SUM(SUM(total_deposits)) OVER (),1) AS pct
    FROM ranked
    GROUP BY 1
"""
)
print("\n4. Deposit concentration (target: top 10% ≈ 35% of volume):")
q4.show()

# 5. Jan 2024 complaint spike
q5 = spark.sql(
    f"""
    SELECT request_year AS yr, request_month AS mo,
           SUM(CASE WHEN category='Complaint' THEN 1 ELSE 0 END) AS complaints,
           COUNT(*) AS total_requests
    FROM {C}.`service_requests`
    WHERE (request_year = 2024 AND request_month BETWEEN 1 AND 3)
       OR (request_year = 2023 AND request_month = 12)
    GROUP BY 1,2
    ORDER BY 1,2
"""
)
print("\n5. Jan 2024 complaint spike (should be markedly higher than adjacent months):")
q5.show()

# 6. FK integrity check
fk_checks = [
    (
        "transactions → accounts",
        f"SELECT COUNT(*) FROM {C}.`transactions` t "
        f"LEFT JOIN {C}.`accounts` a USING(account_id) WHERE a.account_id IS NULL",
    ),
    (
        "transactions → customers",
        f"SELECT COUNT(*) FROM {C}.`transactions` t "
        f"LEFT JOIN {C}.`customers` c USING(customer_id) WHERE c.customer_id IS NULL",
    ),
    (
        "accounts → customers",
        f"SELECT COUNT(*) FROM {C}.`accounts` a "
        f"LEFT JOIN {C}.`customers` c USING(customer_id) WHERE c.customer_id IS NULL",
    ),
    (
        "accounts → products",
        f"SELECT COUNT(*) FROM {C}.`accounts` a "
        f"LEFT JOIN {C}.`products` p USING(product_id) WHERE p.product_id IS NULL",
    ),
    (
        "customers → branches",
        f"SELECT COUNT(*) FROM {C}.`customers` c "
        f"LEFT JOIN {C}.`branches` b ON c.primary_branch_id = b.branch_id WHERE b.branch_id IS NULL",
    ),
]
print("\n6. FK integrity (all should be 0):")
for label, q in fk_checks:
    bad = spark.sql(q).collect()[0][0]
    status = "✓" if bad == 0 else "✗ VIOLATION"
    print(f"  {status}  {label}: {bad} orphans")

print("\nData generation complete.")

print()
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog : {CATALOG}")
print(f"  Schema  : {SCHEMA}")
print(
    "  Tables  : products, branches, customers, accounts,transactions, service_requests"
)
print("  Next    : Create a Genie Agent from these tables, or see notebooks/demo-data/README.md")
