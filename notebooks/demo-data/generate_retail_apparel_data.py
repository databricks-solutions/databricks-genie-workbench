# Databricks notebook source

# MAGIC %md
# MAGIC # Retail Apparel - Synthetic Dataset Generator
# MAGIC
# MAGIC Generates a fictional retail apparel dataset for Databricks AI/BI Genie demos.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---:|---|
# MAGIC | `products` | 80 | Apparel product catalog |
# MAGIC | `stores` | 12 | Store and ecommerce locations |
# MAGIC | `customers` | 1,500 | Loyalty customer dimension |
# MAGIC | `inventory_snapshots` | 34,560 | Product/store/month inventory health |
# MAGIC | `sales` | 15,000 | Item-level sales facts from 2023-2025 |
# MAGIC | `returns` | variable | Return facts linked to sales |
# MAGIC
# MAGIC **Setup:** Edit the configuration variables below, then **Run All**.
# MAGIC
# MAGIC Seed: `42` - fully reproducible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION - Edit only this section before running
# =============================================================================
CATALOG = ""  # TODO: set to a Unity Catalog name before running
SCHEMA = "retail_apparel"
OVERWRITE_EXISTING = False

if not CATALOG:
    raise ValueError("Set CATALOG to a Unity Catalog name before running this notebook.")

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

START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 12, 31)
MONTH_LIST = [(y, m) for y in [2023, 2024, 2025] for m in range(1, 13)]

REGIONS = ["Northeast", "Southeast", "Midwest", "West"]
REGION_STATES = {
    "Northeast": ["NY", "MA", "NJ", "PA"],
    "Southeast": ["FL", "GA", "NC", "TN"],
    "Midwest": ["IL", "OH", "MI", "MN"],
    "West": ["CA", "WA", "OR", "CO"],
}

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Products

# COMMAND ----------

# =============================================================================
# TABLE 1: PRODUCTS (80 rows)
# =============================================================================
CATEGORY_CONFIG = {
    "Tops": {"base_price": (24, 78), "return_weight": 0.09, "season": "All Season"},
    "Bottoms": {"base_price": (38, 110), "return_weight": 0.11, "season": "All Season"},
    "Dresses": {"base_price": (58, 180), "return_weight": 0.14, "season": "Spring"},
    "Outerwear": {"base_price": (88, 260), "return_weight": 0.08, "season": "Winter"},
    "Footwear": {"base_price": (55, 210), "return_weight": 0.16, "season": "All Season"},
    "Accessories": {"base_price": (12, 85), "return_weight": 0.05, "season": "All Season"},
    "Activewear": {"base_price": (28, 120), "return_weight": 0.10, "season": "Summer"},
}
CATEGORY_WEIGHTS = {
    "Tops": 18,
    "Bottoms": 14,
    "Dresses": 10,
    "Outerwear": 9,
    "Footwear": 12,
    "Accessories": 20,
    "Activewear": 17,
}
BRAND_LINES = ["Aster", "Summit", "Kindred", "Northline", "Vista", "Everyday"]
CATEGORY_LABELS = {
    "Tops": "Top",
    "Bottoms": "Bottom",
    "Dresses": "Dress",
    "Outerwear": "Outerwear",
    "Footwear": "Footwear",
    "Accessories": "Accessory",
    "Activewear": "Activewear",
}

products_data = []
categories = list(CATEGORY_CONFIG.keys())
for i in range(1, 81):
    category = random.choices(categories, weights=[CATEGORY_WEIGHTS[c] for c in categories])[0]
    cfg = CATEGORY_CONFIG[category]
    list_price = round(random.uniform(*cfg["base_price"]), 2)
    unit_cost = round(list_price * random.uniform(0.34, 0.58), 2)
    brand_line = random.choice(BRAND_LINES)
    products_data.append(
        {
            "product_id": f"PROD-{i:04d}",
            "sku": f"SKU-{100000 + i}",
            "product_name": f"{brand_line} {CATEGORY_LABELS[category]} {i:02d}",
            "category": category,
            "brand_line": brand_line,
            "season": cfg["season"],
            "list_price_usd": list_price,
            "unit_cost_usd": unit_cost,
            "target_margin_pct": round((list_price - unit_cost) * 100.0 / list_price, 2),
            "launch_date": START_DATE + timedelta(days=random.randint(0, 650)),
            "is_active": random.random() < 0.92,
        }
    )

df_products = pd.DataFrame(products_data)
PRODUCT_LOOKUP = {p["product_id"]: p for p in products_data}
print(f"products: {len(df_products)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Stores

# COMMAND ----------

# =============================================================================
# TABLE 2: STORES (12 rows)
# =============================================================================
STORE_SEEDS = [
    ("STOR-001", "Manhattan Flagship", "Northeast", "NY", "New York", "Flagship"),
    ("STOR-002", "Boston Back Bay", "Northeast", "MA", "Boston", "Mall"),
    ("STOR-003", "Philadelphia Center", "Northeast", "PA", "Philadelphia", "Outlet"),
    ("STOR-004", "Miami Lincoln", "Southeast", "FL", "Miami", "Flagship"),
    ("STOR-005", "Atlanta Midtown", "Southeast", "GA", "Atlanta", "Mall"),
    ("STOR-006", "Charlotte SouthPark", "Southeast", "NC", "Charlotte", "Mall"),
    ("STOR-007", "Chicago Loop", "Midwest", "IL", "Chicago", "Flagship"),
    ("STOR-008", "Columbus Easton", "Midwest", "OH", "Columbus", "Mall"),
    ("STOR-009", "Detroit Somerset", "Midwest", "MI", "Troy", "Outlet"),
    ("STOR-010", "Los Angeles Grove", "West", "CA", "Los Angeles", "Flagship"),
    ("STOR-011", "Seattle Bellevue", "West", "WA", "Bellevue", "Mall"),
    ("STOR-012", "Online Flagship", "West", "CA", "San Francisco", "Ecommerce"),
]

stores_data = []
for sid, name, region, state, city, store_type in STORE_SEEDS:
    stores_data.append(
        {
            "store_id": sid,
            "store_name": name,
            "store_type": store_type,
            "region": region,
            "state": state,
            "city": city,
            "opened_date": date(2015 + random.randint(0, 8), random.randint(1, 12), 1),
            "selling_sqft": 0 if store_type == "Ecommerce" else random.choice([3800, 5200, 7400, 9800]),
            "monthly_rent_usd": 0.0 if store_type == "Ecommerce" else float(random.randint(22000, 115000)),
            "is_active": True,
        }
    )

df_stores = pd.DataFrame(stores_data)
STORE_IDS = [s["store_id"] for s in stores_data]
PHYSICAL_STORE_IDS = [s["store_id"] for s in stores_data if s["store_type"] != "Ecommerce"]
ONLINE_STORE_ID = "STOR-012"
print(f"stores: {len(df_stores)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Customers

# COMMAND ----------

# =============================================================================
# TABLE 3: CUSTOMERS (1,500 rows)
# =============================================================================
LOYALTY_TIERS = ["Member", "Silver", "Gold", "VIP"]
LOYALTY_WEIGHTS = [55, 25, 15, 5]
LOYALTY_DISCOUNT = {"Member": 0.00, "Silver": 0.03, "Gold": 0.06, "VIP": 0.10}

customers_data = []
for i in range(1, 1501):
    tier = random.choices(LOYALTY_TIERS, weights=LOYALTY_WEIGHTS)[0]
    region = random.choice(REGIONS)
    state = random.choice(REGION_STATES[region])
    customers_data.append(
        {
            "customer_id": f"CUST-{i:05d}",
            "customer_name": synthetic_name(i),
            "loyalty_tier": tier,
            "age_band": random.choices(
                ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"],
                weights=[14, 28, 24, 18, 11, 5],
            )[0],
            "home_region": region,
            "home_state": state,
            "acquisition_channel": random.choices(
                ["Store", "Paid Search", "Social", "Referral", "Email"],
                weights=[35, 20, 18, 12, 15],
            )[0],
            "customer_since_date": START_DATE - timedelta(days=random.randint(0, 1500)),
            "is_active": random.random() < 0.95,
        }
    )

df_customers = pd.DataFrame(customers_data)
CUSTOMER_IDS = [c["customer_id"] for c in customers_data]
CUSTOMER_TIER = {c["customer_id"]: c["loyalty_tier"] for c in customers_data}
print(f"customers: {len(df_customers)} rows")
print(df_customers["loyalty_tier"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inventory Snapshots

# COMMAND ----------

# =============================================================================
# TABLE 4: INVENTORY SNAPSHOTS (product x store x month)
# =============================================================================
inventory_data = []
stockout_lookup = {}
snapshot_counter = 1

for year, month in MONTH_LIST:
    for store in stores_data:
        for product in products_data:
            category = product["category"]
            is_q4 = month in (10, 11, 12)
            base_on_hand = random.randint(8, 85)
            if store["store_type"] == "Ecommerce":
                base_on_hand = int(base_on_hand * 2.3)
            if is_q4 and category in ("Outerwear", "Footwear"):
                stockout_days = random.choices([0, 1, 2, 4, 7, 10, 14], weights=[12, 15, 20, 20, 15, 12, 6])[0]
            else:
                stockout_days = random.choices([0, 1, 2, 4, 7, 10], weights=[30, 28, 20, 12, 7, 3])[0]
            if stockout_days >= 7:
                on_hand_units = random.randint(0, 8)
            else:
                on_hand_units = base_on_hand
            lost_sales = max(0, stockout_days - 2) * random.uniform(1.5, 5.0)
            stockout_lookup[(product["product_id"], store["store_id"], year, month)] = stockout_days
            inventory_data.append(
                {
                    "snapshot_id": f"INV-{snapshot_counter:07d}",
                    "snapshot_month": date(year, month, 1),
                    "snapshot_year": year,
                    "snapshot_month_num": month,
                    "product_id": product["product_id"],
                    "store_id": store["store_id"],
                    "on_hand_units": int(on_hand_units),
                    "reorder_point_units": int(random.randint(10, 40)),
                    "stockout_days": int(stockout_days),
                    "lost_sales_estimate_units": round(lost_sales, 2),
                    "inventory_value_usd": round(on_hand_units * product["unit_cost_usd"], 2),
                }
            )
            snapshot_counter += 1

df_inventory_snapshots = pd.DataFrame(inventory_data)
print(f"inventory_snapshots: {len(df_inventory_snapshots)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Sales

# COMMAND ----------

# =============================================================================
# TABLE 5: SALES (15,000 rows)
# =============================================================================
# Injected patterns:
# 1. Holiday Nov/Dec sales spike.
# 2. Online channel share grows from 28% to 52%.
# 3. January and July clearance periods have deeper discounts.
# 4. High-stockout product/store/month combinations have lower quantities.
# 5. VIP and Gold loyalty tiers are a small customer share but large revenue share.

month_weights = [1.65 if m in (11, 12) else 1.20 if m in (1, 7) else 1.0 for _, m in MONTH_LIST]
sales_per_month_raw = [w / sum(month_weights) * 15000 for w in month_weights]
sales_per_month = [int(round(x)) for x in sales_per_month_raw]
sales_per_month[-1] += 15000 - sum(sales_per_month)


def online_share(month_idx):
    return 0.28 + (0.52 - 0.28) * (month_idx / 35.0)


def pick_customer_for_sale():
    # Revenue concentration: Gold/VIP customers are more likely to purchase.
    tier = random.choices(LOYALTY_TIERS, weights=[35, 28, 25, 12])[0]
    candidates = [c["customer_id"] for c in customers_data if c["loyalty_tier"] == tier]
    return random.choice(candidates)


sales_data = []
sale_counter = 1
product_weights = [CATEGORY_WEIGHTS[p["category"]] for p in products_data]

for month_idx, (year, month) in enumerate(MONTH_LIST):
    n_sales = sales_per_month[month_idx]
    max_day = monthrange(year, month)[1]
    o_share = online_share(month_idx)
    is_clearance_month = month in (1, 7)

    for _ in range(n_sales):
        sale_date = date(year, month, random.randint(1, max_day))
        customer_id = pick_customer_for_sale()
        loyalty_tier = CUSTOMER_TIER[customer_id]
        channel = random.choices(["Online", "Store"], weights=[o_share, 1.0 - o_share])[0]
        store_id = ONLINE_STORE_ID if channel == "Online" else random.choice(PHYSICAL_STORE_IDS)
        product = random.choices(products_data, weights=product_weights)[0]
        stockout_days = stockout_lookup[(product["product_id"], store_id, year, month)]
        quantity = random.choices([1, 2, 3, 4], weights=[68, 22, 7, 3])[0]
        if stockout_days >= 7:
            quantity = 1

        base_discount = LOYALTY_DISCOUNT[loyalty_tier]
        promo_discount = random.choice([0.00, 0.05, 0.10, 0.15])
        clearance_discount = random.uniform(0.20, 0.45) if is_clearance_month else 0.0
        discount_pct = min(0.65, base_discount + promo_discount + clearance_discount)
        gross_sales = product["list_price_usd"] * quantity
        discount_amount = gross_sales * discount_pct
        net_sales = gross_sales - discount_amount

        sales_data.append(
            {
                "sale_id": f"SALE-{sale_counter:07d}",
                "sale_date": sale_date,
                "sale_year": year,
                "sale_month": month,
                "sale_quarter": (month - 1) // 3 + 1,
                "customer_id": customer_id,
                "product_id": product["product_id"],
                "store_id": store_id,
                "channel": channel,
                "quantity": int(quantity),
                "gross_sales_usd": round(gross_sales, 2),
                "discount_pct": round(discount_pct * 100, 2),
                "discount_amount_usd": round(discount_amount, 2),
                "net_sales_usd": round(net_sales, 2),
                "unit_cost_usd": product["unit_cost_usd"],
                "gross_margin_usd": round(net_sales - product["unit_cost_usd"] * quantity, 2),
                "loyalty_tier": loyalty_tier,
                "is_clearance": bool(is_clearance_month),
                "stockout_days_at_sale": int(stockout_days),
            }
        )
        sale_counter += 1

df_sales = pd.DataFrame(sales_data)
print(f"sales: {len(df_sales)} rows")
print(df_sales["channel"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Returns

# COMMAND ----------

# =============================================================================
# TABLE 6: RETURNS
# =============================================================================
RETURN_REASONS = ["Fit", "Changed Mind", "Damaged", "Late Delivery", "Wrong Item"]
returns_data = []
return_counter = 1

for sale in sales_data:
    product = PRODUCT_LOOKUP[sale["product_id"]]
    base_prob = CATEGORY_CONFIG[product["category"]]["return_weight"]
    channel_mult = 1.55 if sale["channel"] == "Online" else 0.82
    clearance_mult = 0.75 if sale["is_clearance"] else 1.0
    return_prob = min(0.35, base_prob * channel_mult * clearance_mult)
    if random.random() < return_prob:
        reason = random.choices(RETURN_REASONS, weights=[33, 28, 12, 18, 9])[0]
        return_date = min(END_DATE, sale["sale_date"] + timedelta(days=random.randint(3, 45)))
        return_qty = random.randint(1, sale["quantity"])
        unit_net_price = sale["net_sales_usd"] / sale["quantity"]
        returns_data.append(
            {
                "return_id": f"RTRN-{return_counter:07d}",
                "return_date": return_date,
                "return_year": return_date.year,
                "return_month": return_date.month,
                "sale_id": sale["sale_id"],
                "customer_id": sale["customer_id"],
                "product_id": sale["product_id"],
                "store_id": sale["store_id"],
                "channel": sale["channel"],
                "return_reason": reason,
                "return_quantity": return_qty,
                "return_amount_usd": round(unit_net_price * return_qty, 2),
                "days_to_return": (return_date - sale["sale_date"]).days,
            }
        )
        return_counter += 1

df_returns = pd.DataFrame(returns_data)
print(f"returns: {len(df_returns)} rows")

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
    "products": df_products,
    "stores": df_stores,
    "customers": df_customers,
    "inventory_snapshots": df_inventory_snapshots,
    "sales": df_sales,
    "returns": df_returns,
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

print("\n1. Holiday sales spike: Nov/Dec should have higher monthly order counts.")
spark.sql(
    f"""
    SELECT CASE WHEN sale_month IN (11, 12) THEN 'Nov/Dec' ELSE 'Other' END AS period,
           COUNT(*) AS orders,
           ROUND(SUM(net_sales_usd), 2) AS revenue,
           COUNT(DISTINCT sale_month) AS months,
           ROUND(COUNT(*) / COUNT(DISTINCT sale_month), 1) AS avg_orders_per_month,
           ROUND(SUM(net_sales_usd) / COUNT(DISTINCT sale_month), 2) AS avg_revenue_per_month
    FROM {C}.`sales`
    GROUP BY 1
    """
).show()

print("\n2. Online channel growth: online share should rise from 2023 to 2025.")
spark.sql(
    f"""
    SELECT sale_year,
           ROUND(SUM(CASE WHEN channel = 'Online' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS online_order_pct
    FROM {C}.`sales`
    GROUP BY sale_year
    ORDER BY sale_year
    """
).show()

print("\n3. Clearance months should have deeper discounts.")
spark.sql(
    f"""
    SELECT is_clearance,
           ROUND(AVG(discount_pct), 1) AS avg_discount_pct,
           COUNT(*) AS orders
    FROM {C}.`sales`
    GROUP BY is_clearance
    """
).show()

print("\n4. Stockout impact: high stockout sales should have lower units per order.")
spark.sql(
    f"""
    SELECT CASE WHEN stockout_days_at_sale >= 7 THEN 'High Stockout' ELSE 'Normal' END AS stockout_group,
           ROUND(AVG(quantity), 2) AS avg_units_per_order,
           COUNT(*) AS orders
    FROM {C}.`sales`
    GROUP BY 1
    """
).show()

print("\n5. Higher online return rate.")
spark.sql(
    f"""
    SELECT s.channel,
           COUNT(DISTINCT r.return_id) AS returns,
           COUNT(DISTINCT s.sale_id) AS sales,
           ROUND(COUNT(DISTINCT r.return_id) * 100.0 / COUNT(DISTINCT s.sale_id), 1) AS return_rate_pct
    FROM {C}.`sales` s
    LEFT JOIN {C}.`returns` r ON s.sale_id = r.sale_id
    GROUP BY s.channel
    """
).show()

print()
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print("  Tables      : products, stores, customers, inventory_snapshots, sales, returns")
print("  Next    : Create a Genie space from these tables, or see notebooks/demo-data/README.md")
