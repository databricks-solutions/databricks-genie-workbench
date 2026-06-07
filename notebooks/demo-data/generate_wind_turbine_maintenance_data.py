# Databricks notebook source

# MAGIC %md
# MAGIC # Wind Turbine Predictive Maintenance - Synthetic Dataset Generator
# MAGIC
# MAGIC Generates a fictional wind turbine operations dataset for Databricks AI/BI Genie demos.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---:|---|
# MAGIC | `wind_farms` | 8 | Wind farm dimension |
# MAGIC | `turbines` | 96 | Turbine asset dimension |
# MAGIC | `components` | 384 | Major component dimension |
# MAGIC | `sensor_readings` | 3,456 | Monthly turbine telemetry from 2023-2025 |
# MAGIC | `maintenance_events` | variable | Preventive, inspection, and corrective work orders |
# MAGIC | `failure_events` | variable | Component failure facts |
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
DEFAULT_SCHEMA = "wind_turbine_maintenance"  # Schema / database name

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

MODEL_CONFIG = {
    "WT-2.5A": {"capacity_mw": 2.5, "failure_mult": 0.85, "hub_height_m": 90},
    "WT-3.0B": {"capacity_mw": 3.0, "failure_mult": 1.00, "hub_height_m": 105},
    "WT-4.2C": {"capacity_mw": 4.2, "failure_mult": 1.45, "hub_height_m": 120},
}
COMPONENT_TYPES = ["Gearbox", "Generator", "Blade Set", "Yaw System"]

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Wind Farms

# COMMAND ----------

# =============================================================================
# TABLE 1: WIND FARMS (8 rows)
# =============================================================================
FARM_SEEDS = [
    ("FARM-001", "Prairie Ridge", "Midwest", "IA", 8.7, "Plains"),
    ("FARM-002", "Lone Mesa", "Southwest", "TX", 9.1, "Mesa"),
    ("FARM-003", "Columbia Gorge", "West", "WA", 10.2, "Ridge"),
    ("FARM-004", "High Desert", "West", "CA", 8.4, "Desert"),
    ("FARM-005", "Great Lakes Shore", "Midwest", "MI", 8.9, "Coastal"),
    ("FARM-006", "Dakota Plains", "Midwest", "ND", 9.5, "Plains"),
    ("FARM-007", "Appalachian Ridge", "Northeast", "PA", 7.8, "Ridge"),
    ("FARM-008", "Gulf Coast Bend", "Southeast", "LA", 8.2, "Coastal"),
]

wind_farms_data = []
for farm_id, name, region, state, wind_speed, terrain in FARM_SEEDS:
    wind_farms_data.append(
        {
            "farm_id": farm_id,
            "farm_name": name,
            "region": region,
            "state": state,
            "terrain_type": terrain,
            "mean_wind_speed_mps": float(wind_speed),
            "grid_interconnect": random.choice(["North", "South", "East", "West"]),
            "commissioned_date": date(2013 + random.randint(0, 7), random.randint(1, 12), 1),
            "is_active": True,
        }
    )

df_wind_farms = pd.DataFrame(wind_farms_data)
FARM_BY_ID = {f["farm_id"]: f for f in wind_farms_data}
print(f"wind_farms: {len(df_wind_farms)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Turbines

# COMMAND ----------

# =============================================================================
# TABLE 2: TURBINES (96 rows)
# =============================================================================
turbines_data = []
turbine_counter = 1

for farm in wind_farms_data:
    for _ in range(12):
        model = random.choices(list(MODEL_CONFIG.keys()), weights=[36, 42, 22])[0]
        cfg = MODEL_CONFIG[model]
        install_date = farm["commissioned_date"] + timedelta(days=random.randint(0, 540))
        turbines_data.append(
            {
                "turbine_id": f"TURB-{turbine_counter:04d}",
                "farm_id": farm["farm_id"],
                "turbine_model": model,
                "rated_capacity_mw": cfg["capacity_mw"],
                "hub_height_m": cfg["hub_height_m"],
                "rotor_diameter_m": random.choice([100, 112, 126, 136]),
                "install_date": install_date,
                "commissioning_year": install_date.year,
                "status": random.choices(["Operating", "Derated", "Offline"], weights=[92, 6, 2])[0],
            }
        )
        turbine_counter += 1

df_turbines = pd.DataFrame(turbines_data)
TURBINE_BY_ID = {t["turbine_id"]: t for t in turbines_data}
TURBINE_IDS = [t["turbine_id"] for t in turbines_data]
print(f"turbines: {len(df_turbines)} rows")
print(df_turbines["turbine_model"].value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Components

# COMMAND ----------

# =============================================================================
# TABLE 3: COMPONENTS (384 rows)
# =============================================================================
components_data = []
component_counter = 1
COMPONENTS_BY_TURBINE = {}

for turbine in turbines_data:
    COMPONENTS_BY_TURBINE[turbine["turbine_id"]] = []
    for component_type in COMPONENT_TYPES:
        component_id = f"COMP-{component_counter:06d}"
        component_counter += 1
        expected_life = {
            "Gearbox": 12,
            "Generator": 15,
            "Blade Set": 20,
            "Yaw System": 10,
        }[component_type]
        row = {
            "component_id": component_id,
            "turbine_id": turbine["turbine_id"],
            "component_type": component_type,
            "manufacturer": random.choice(["Apex Rotor", "Northwind Systems", "HelioWorks", "Vector Drive"]),
            "install_date": turbine["install_date"],
            "expected_life_years": expected_life,
            "status": random.choices(["In Service", "Watchlist", "Replaced"], weights=[88, 10, 2])[0],
        }
        components_data.append(row)
        COMPONENTS_BY_TURBINE[turbine["turbine_id"]].append(row)

df_components = pd.DataFrame(components_data)
COMPONENT_BY_ID = {c["component_id"]: c for c in components_data}
print(f"components: {len(df_components)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preventive Maintenance

# COMMAND ----------

# =============================================================================
# TABLE 4: MAINTENANCE EVENTS (base preventive and inspection events)
# =============================================================================
maintenance_data = []
maintenance_counter = 1

for turbine in turbines_data:
    for year in [2023, 2024, 2025]:
        for quarter_start in [1, 4, 7, 10]:
            if random.random() < 0.74:
                event_date = date(year, quarter_start, 1) + timedelta(days=random.randint(0, 70))
                component = random.choice(COMPONENTS_BY_TURBINE[turbine["turbine_id"]])
                event_type = random.choices(["Preventive", "Inspection"], weights=[72, 28])[0]
                duration = random.uniform(2.0, 8.0) if event_type == "Inspection" else random.uniform(6.0, 18.0)
                maintenance_data.append(
                    {
                        "maintenance_id": f"MAINT-{maintenance_counter:07d}",
                        "maintenance_date": event_date,
                        "maintenance_year": event_date.year,
                        "maintenance_month": event_date.month,
                        "farm_id": turbine["farm_id"],
                        "turbine_id": turbine["turbine_id"],
                        "component_id": component["component_id"],
                        "maintenance_type": event_type,
                        "work_order_priority": random.choices(["Low", "Medium", "High"], weights=[45, 45, 10])[0],
                        "duration_hours": round(duration, 2),
                        "cost_usd": round(duration * random.uniform(180, 420), 2),
                        "technician_team": random.choice(["Team A", "Team B", "Team C", "Vendor"]),
                    }
                )
                maintenance_counter += 1


def had_recent_preventive(turbine_id, event_date, days=90):
    lower = event_date - timedelta(days=days)
    for event in maintenance_data:
        if (
            event["turbine_id"] == turbine_id
            and event["maintenance_type"] == "Preventive"
            and lower <= event["maintenance_date"] <= event_date
        ):
            return True
    return False


print(f"base maintenance_events: {len(maintenance_data)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Failure Events

# COMMAND ----------

# =============================================================================
# TABLE 5: FAILURE EVENTS
# =============================================================================
# Injected patterns:
# 1. WT-4.2C turbines fail more often than other models.
# 2. Gearbox temperature and vibration rise before failures.
# 3. Winter icing increases blade-related failures and reduces capacity factor.
# 4. Recent preventive maintenance reduces downtime.

failure_data = []
failure_counter = 1

for turbine in turbines_data:
    model_mult = MODEL_CONFIG[turbine["turbine_model"]]["failure_mult"]
    farm = FARM_BY_ID[turbine["farm_id"]]
    base_failures = random.choices([0, 1, 2, 3], weights=[50, 34, 12, 4])[0]
    if random.random() < (model_mult - 1.0) * 0.22:
        base_failures += 1
    for _ in range(base_failures):
        year = random.choice([2023, 2024, 2025])
        month = random.choices(list(range(1, 13)), weights=[13, 12, 10, 8, 7, 7, 11, 12, 8, 6, 3, 3])[0]
        max_day = monthrange(year, month)[1]
        failure_date = date(year, month, random.randint(1, max_day))
        if month in (1, 2, 12):
            component_type = random.choices(COMPONENT_TYPES, weights=[24, 18, 44, 14])[0]
            failure_type = random.choice(["Icing Damage", "Pitch Fault", "Bearing Wear"])
        elif month in (6, 7, 8):
            component_type = random.choices(COMPONENT_TYPES, weights=[46, 23, 18, 13])[0]
            failure_type = random.choice(["Thermal Alarm", "Gearbox Wear", "Oil Pressure"])
        else:
            component_type = random.choices(COMPONENT_TYPES, weights=[34, 26, 24, 16])[0]
            failure_type = random.choice(["Bearing Wear", "Electrical Fault", "Pitch Fault", "Sensor Fault"])
        component = random.choice([c for c in COMPONENTS_BY_TURBINE[turbine["turbine_id"]] if c["component_type"] == component_type])
        recent_pm = had_recent_preventive(turbine["turbine_id"], failure_date)
        severity = random.choices(["Minor", "Major", "Critical"], weights=[42, 43, 15])[0]
        severity_mult = {"Minor": 0.7, "Major": 1.0, "Critical": 1.8}[severity]
        downtime = random.uniform(10, 72) * severity_mult
        if recent_pm:
            downtime *= 0.62
        repair_cost = downtime * random.uniform(800, 1800)
        failure_data.append(
            {
                "failure_id": f"FAIL-{failure_counter:07d}",
                "failure_date": failure_date,
                "failure_year": failure_date.year,
                "failure_month": failure_date.month,
                "farm_id": turbine["farm_id"],
                "turbine_id": turbine["turbine_id"],
                "component_id": component["component_id"],
                "component_type": component_type,
                "failure_type": failure_type,
                "severity": severity,
                "had_recent_preventive": recent_pm,
                "downtime_hours": round(downtime, 2),
                "repair_cost_usd": round(repair_cost, 2),
            }
        )
        # Corrective maintenance work order tied to the failure.
        corrective_date = min(END_DATE, failure_date + timedelta(days=random.randint(0, 4)))
        maintenance_data.append(
            {
                "maintenance_id": f"MAINT-{maintenance_counter:07d}",
                "maintenance_date": corrective_date,
                "maintenance_year": corrective_date.year,
                "maintenance_month": corrective_date.month,
                "farm_id": turbine["farm_id"],
                "turbine_id": turbine["turbine_id"],
                "component_id": component["component_id"],
                "maintenance_type": "Corrective",
                "work_order_priority": "High" if severity != "Minor" else "Medium",
                "duration_hours": round(downtime, 2),
                "cost_usd": round(repair_cost, 2),
                "technician_team": random.choice(["Team A", "Team B", "Team C", "Vendor"]),
            }
        )
        maintenance_counter += 1
        failure_counter += 1

df_failure_events = pd.DataFrame(failure_data)
df_maintenance_events = pd.DataFrame(maintenance_data)
FAILURES_BY_TURBINE = {}
for failure in failure_data:
    FAILURES_BY_TURBINE.setdefault(failure["turbine_id"], []).append(failure)
print(f"failure_events: {len(df_failure_events)} rows")
print(f"maintenance_events: {len(df_maintenance_events)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sensor Readings

# COMMAND ----------

# =============================================================================
# TABLE 6: SENSOR READINGS (monthly turbine telemetry)
# =============================================================================


def days_to_next_failure(turbine_id, reading_date):
    candidates = [
        (f["failure_date"] - reading_date).days
        for f in FAILURES_BY_TURBINE.get(turbine_id, [])
        if f["failure_date"] >= reading_date
    ]
    return min(candidates) if candidates else None


sensor_data = []
reading_counter = 1

for turbine in turbines_data:
    farm = FARM_BY_ID[turbine["farm_id"]]
    farm_wind = farm["mean_wind_speed_mps"]
    model = turbine["turbine_model"]
    capacity_kw = turbine["rated_capacity_mw"] * 1000.0
    farm_cf_bias = (farm_wind - 8.5) * 2.2
    for year, month in MONTH_LIST:
        reading_date = date(year, month, min(15, monthrange(year, month)[1]))
        ambient_temp = random.uniform(-8, 8) if month in (1, 2, 12) else random.uniform(18, 35) if month in (6, 7, 8) else random.uniform(5, 24)
        icing_flag = month in (1, 2, 12) and random.random() < 0.34
        wind_speed = max(2.0, random.gauss(farm_wind, 1.6))
        capacity_factor = max(8.0, min(58.0, 31.0 + farm_cf_bias + (wind_speed - farm_wind) * 3.0 + random.gauss(0, 4.2)))
        if icing_flag:
            capacity_factor *= random.uniform(0.68, 0.86)
        gearbox_temp = random.gauss(66, 7)
        if month in (6, 7, 8):
            gearbox_temp += random.uniform(6, 13)
        vibration = max(0.5, random.gauss(2.4, 0.55))
        days_next = days_to_next_failure(turbine["turbine_id"], reading_date)
        anomaly_score = max(0.0, random.gauss(18, 8))
        if days_next is not None and days_next <= 60:
            anomaly_score += 45 - (days_next / 2.0)
            vibration += random.uniform(1.4, 3.2)
            gearbox_temp += random.uniform(8, 18)
        elif days_next is not None and days_next <= 120:
            anomaly_score += 16
            vibration += random.uniform(0.4, 1.2)
        power_output = capacity_kw * capacity_factor / 100.0
        sensor_data.append(
            {
                "reading_id": f"SENS-{reading_counter:07d}",
                "reading_date": reading_date,
                "reading_year": year,
                "reading_month": month,
                "farm_id": turbine["farm_id"],
                "turbine_id": turbine["turbine_id"],
                "turbine_model": model,
                "wind_speed_mps": round(wind_speed, 2),
                "ambient_temp_c": round(ambient_temp, 2),
                "power_output_kw": round(power_output, 2),
                "capacity_factor_pct": round(capacity_factor, 2),
                "gearbox_temp_c": round(gearbox_temp, 2),
                "vibration_mm_s": round(vibration, 2),
                "blade_pitch_deg": round(random.uniform(2, 18), 2),
                "icing_flag": bool(icing_flag),
                "anomaly_score": round(min(100.0, anomaly_score), 2),
                "days_to_next_failure": days_next,
            }
        )
        reading_counter += 1

df_sensor_readings = pd.DataFrame(sensor_data)
print(f"sensor_readings: {len(df_sensor_readings)} rows")

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
    "wind_farms": df_wind_farms,
    "turbines": df_turbines,
    "components": df_components,
    "sensor_readings": df_sensor_readings,
    "maintenance_events": df_maintenance_events,
    "failure_events": df_failure_events,
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

print("\n1. Anomaly scores should rise within 60 days of failure.")
spark.sql(
    f"""
    SELECT CASE WHEN days_to_next_failure BETWEEN 0 AND 60 THEN 'Within 60 Days'
                WHEN days_to_next_failure BETWEEN 61 AND 120 THEN '61-120 Days'
                ELSE 'No Near Failure' END AS failure_window,
           ROUND(AVG(anomaly_score), 1) AS avg_anomaly_score,
           ROUND(AVG(vibration_mm_s), 2) AS avg_vibration,
           COUNT(*) AS readings
    FROM {C}.`sensor_readings`
    GROUP BY 1
    ORDER BY avg_anomaly_score DESC
    """
).show()

print("\n2. Winter icing should reduce capacity factor.")
spark.sql(
    f"""
    SELECT icing_flag,
           ROUND(AVG(capacity_factor_pct), 1) AS avg_capacity_factor_pct,
           COUNT(*) AS readings
    FROM {C}.`sensor_readings`
    GROUP BY icing_flag
    """
).show()

print("\n3. Summer months should have higher gearbox temperatures.")
spark.sql(
    f"""
    SELECT CASE WHEN reading_month IN (6, 7, 8) THEN 'Summer' ELSE 'Other' END AS period,
           ROUND(AVG(gearbox_temp_c), 1) AS avg_gearbox_temp_c
    FROM {C}.`sensor_readings`
    GROUP BY 1
    """
).show()

print("\n4. WT-4.2C should show a higher failure rate.")
spark.sql(
    f"""
    SELECT t.turbine_model,
           COUNT(DISTINCT f.failure_id) AS failures,
           COUNT(DISTINCT t.turbine_id) AS turbines,
           ROUND(COUNT(DISTINCT f.failure_id) * 1.0 / COUNT(DISTINCT t.turbine_id), 2) AS failures_per_turbine
    FROM {C}.`turbines` t
    LEFT JOIN {C}.`failure_events` f ON t.turbine_id = f.turbine_id
    GROUP BY t.turbine_model
    ORDER BY failures_per_turbine DESC
    """
).show()

print("\n5. Recent preventive maintenance should reduce failure downtime.")
spark.sql(
    f"""
    SELECT had_recent_preventive,
           ROUND(AVG(downtime_hours), 1) AS avg_downtime_hours,
           COUNT(*) AS failures
    FROM {C}.`failure_events`
    GROUP BY had_recent_preventive
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
    "mv_turbine_performance": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.sensor_readings

joins:
  - name: turbine
    source: {CATALOG}.{SCHEMA}.turbines
    on: source.turbine_id = turbine.turbine_id
  - name: farm
    source: {CATALOG}.{SCHEMA}.wind_farms
    on: source.farm_id = farm.farm_id

dimensions:
  - name: Reading Month
    expr: DATE_TRUNC('MONTH', reading_date)
  - name: Reading Year
    expr: reading_year
  - name: Farm Name
    expr: farm.farm_name
  - name: Region
    expr: farm.region
  - name: Turbine Model
    expr: turbine.turbine_model
  - name: Icing Flag
    expr: icing_flag

measures:
  - name: Reading Count
    expr: COUNT(1)
  - name: Average Capacity Factor Pct
    expr: AVG(capacity_factor_pct)
  - name: Average Power Output KW
    expr: AVG(power_output_kw)
  - name: Average Anomaly Score
    expr: AVG(anomaly_score)
  - name: High Anomaly Count
    expr: COUNT(1) FILTER (WHERE anomaly_score >= 70)
  - name: Average Gearbox Temp C
    expr: AVG(gearbox_temp_c)
  - name: Average Vibration
    expr: AVG(vibration_mm_s)
""",
    "mv_maintenance_reliability": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.maintenance_events

joins:
  - name: turbine
    source: {CATALOG}.{SCHEMA}.turbines
    on: source.turbine_id = turbine.turbine_id
  - name: farm
    source: {CATALOG}.{SCHEMA}.wind_farms
    on: source.farm_id = farm.farm_id
  - name: component
    source: {CATALOG}.{SCHEMA}.components
    on: source.component_id = component.component_id

dimensions:
  - name: Maintenance Month
    expr: DATE_TRUNC('MONTH', maintenance_date)
  - name: Maintenance Year
    expr: maintenance_year
  - name: Maintenance Type
    expr: maintenance_type
  - name: Component Type
    expr: component.component_type
  - name: Farm Name
    expr: farm.farm_name
  - name: Turbine Model
    expr: turbine.turbine_model

measures:
  - name: Maintenance Count
    expr: COUNT(1)
  - name: Preventive Count
    expr: COUNT(1) FILTER (WHERE maintenance_type = 'Preventive')
  - name: Corrective Count
    expr: COUNT(1) FILTER (WHERE maintenance_type = 'Corrective')
  - name: Total Maintenance Cost
    expr: SUM(cost_usd)
  - name: Average Duration Hours
    expr: AVG(duration_hours)
""",
    "mv_failure_events": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.failure_events

joins:
  - name: turbine
    source: {CATALOG}.{SCHEMA}.turbines
    on: source.turbine_id = turbine.turbine_id
  - name: farm
    source: {CATALOG}.{SCHEMA}.wind_farms
    on: source.farm_id = farm.farm_id

dimensions:
  - name: Failure Month
    expr: DATE_TRUNC('MONTH', failure_date)
  - name: Failure Year
    expr: failure_year
  - name: Component Type
    expr: component_type
  - name: Failure Type
    expr: failure_type
  - name: Severity
    expr: severity
  - name: Turbine Model
    expr: turbine.turbine_model
  - name: Farm Name
    expr: farm.farm_name
  - name: Recent Preventive Flag
    expr: had_recent_preventive

measures:
  - name: Failure Count
    expr: COUNT(1)
  - name: Total Downtime Hours
    expr: SUM(downtime_hours)
  - name: Average Downtime Hours
    expr: AVG(downtime_hours)
  - name: Total Repair Cost
    expr: SUM(repair_cost_usd)
  - name: Recent Preventive Failure Count
    expr: COUNT(1) FILTER (WHERE had_recent_preventive = TRUE)
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
    "wind_farms": "farm_id",
    "turbines": "turbine_id",
    "components": "component_id",
    "sensor_readings": "reading_id",
    "maintenance_events": "maintenance_id",
    "failure_events": "failure_id",
}

FOREIGN_KEYS = [
    ("turbines", "farm_id", "wind_farms", "farm_id"),
    ("components", "turbine_id", "turbines", "turbine_id"),
    ("sensor_readings", "farm_id", "wind_farms", "farm_id"),
    ("sensor_readings", "turbine_id", "turbines", "turbine_id"),
    ("maintenance_events", "farm_id", "wind_farms", "farm_id"),
    ("maintenance_events", "turbine_id", "turbines", "turbine_id"),
    ("maintenance_events", "component_id", "components", "component_id"),
    ("failure_events", "farm_id", "wind_farms", "farm_id"),
    ("failure_events", "turbine_id", "turbines", "turbine_id"),
    ("failure_events", "component_id", "components", "component_id"),
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
    "wind_farms": "Wind farm dimension with region, state, terrain, and wind resource attributes.",
    "turbines": "Turbine asset dimension. WT-4.2C turbines have an intentionally higher synthetic failure rate.",
    "components": "Major turbine components tracked for maintenance and failure analysis.",
    "sensor_readings": "Monthly turbine telemetry. Anomaly score, vibration, and gearbox temperature rise before failures.",
    "maintenance_events": "Preventive, inspection, and corrective maintenance work orders with duration and cost.",
    "failure_events": "Component failure facts with severity, downtime, repair cost, and recent preventive maintenance flag.",
}

for table, comment in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {C}.`{table}` IS '{sql_text(comment)}'")

for table, df_pd in TABLES.items():
    for column in df_pd.columns:
        readable = column.replace("_", " ")
        spark.sql(
            f"ALTER TABLE {C}.`{table}` ALTER COLUMN `{column}` "
            f"COMMENT '{sql_text(readable)} for the synthetic wind turbine maintenance dataset'"
        )

print("  OK constraints and comments registered")

print()
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print("  Tables      : wind_farms, turbines, components, sensor_readings, maintenance_events, failure_events")
print("  Metric Views: mv_turbine_performance, mv_maintenance_reliability, mv_failure_events")
print("  Next    : Create a Genie space from these tables, or see notebooks/demo-data/README.md")
