# Databricks notebook source

# MAGIC %md
# MAGIC # Talent Advisory - Synthetic Dataset Generator
# MAGIC
# MAGIC Generates a fictional HR talent advisory dataset for Databricks AI/BI Genie demos.
# MAGIC The data is designed for multi-Genie demos where focused spaces answer workforce
# MAGIC planning, hiring, retention, mobility, compensation, and succession questions.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---:|---|
# MAGIC | `locations` | 10 | Work locations and regions |
# MAGIC | `org_units` | 18 | Organizational hierarchy |
# MAGIC | `job_roles` | 44 | Role catalog with compensation bands and criticality |
# MAGIC | `skills` | 36 | Skill taxonomy |
# MAGIC | `employees` | ~2,850 | Synthetic employee dimension, ~2,500 active |
# MAGIC | `employee_skills` | ~17,000 | Employee skill proficiency and gap facts |
# MAGIC | `learning_activity` | variable | Learning completions and mobility program activity |
# MAGIC | `talent_events` | variable | Hires, exits, promotions, and transfers |
# MAGIC | `performance_reviews` | variable | Annual performance and potential reviews |
# MAGIC | `compensation_snapshots` | variable | Annual salary, bonus, and compa-ratio facts |
# MAGIC | `engagement_pulses` | variable | Quarterly engagement pulse facts |
# MAGIC | `retention_risk_snapshots` | variable | Quarterly retention risk snapshots |
# MAGIC | `requisitions` | ~640 | Hiring requisitions |
# MAGIC | `applications` | variable | Candidate funnel facts |
# MAGIC | `succession_plans` | variable | Critical role successor coverage |
# MAGIC
# MAGIC The notebook also creates six curated mart tables for
# MAGIC focused Genie Agents.
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
SCHEMA = "talent_advisory"
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
import math
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
YEARS = [2023, 2024, 2025]
MONTH_LIST = [(y, m) for y in YEARS for m in range(1, 13)]
QUARTER_ENDS = [
    date(2023, 3, 31),
    date(2023, 6, 30),
    date(2023, 9, 30),
    date(2023, 12, 31),
    date(2024, 3, 31),
    date(2024, 6, 30),
    date(2024, 9, 30),
    date(2024, 12, 31),
    date(2025, 3, 31),
    date(2025, 6, 30),
    date(2025, 9, 30),
    date(2025, 12, 31),
]


def clamp(value, low, high):
    return max(low, min(high, value))


def date_between(start_date, end_date):
    days = (end_date - start_date).days
    return start_date + timedelta(days=random.randint(0, max(0, days)))


def end_of_month(year, month):
    return date(year, month, monthrange(year, month)[1])


def quarter_label(d):
    return f"{d.year}-Q{((d.month - 1) // 3) + 1}"


def is_employed_on(employee, as_of_date):
    term_date = employee.get("termination_date")
    return employee["hire_date"] <= as_of_date and (term_date is None or term_date > as_of_date)


def active_during(employee, period_start, period_end):
    term_date = employee.get("termination_date")
    return employee["hire_date"] <= period_end and (term_date is None or term_date >= period_start)


print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Locations

# COMMAND ----------

# =============================================================================
# TABLE 1: LOCATIONS (10 rows)
# =============================================================================
locations_data = [
    {
        "location_id": "LOC-001",
        "location_name": "San Francisco HQ",
        "region": "West",
        "country": "United States",
        "state": "CA",
        "city": "San Francisco",
        "time_zone": "America/Los_Angeles",
        "work_model": "Hybrid Hub",
        "cost_index": 1.28,
        "is_active": True,
    },
    {
        "location_id": "LOC-002",
        "location_name": "Seattle Engineering Hub",
        "region": "West",
        "country": "United States",
        "state": "WA",
        "city": "Seattle",
        "time_zone": "America/Los_Angeles",
        "work_model": "Hybrid Hub",
        "cost_index": 1.18,
        "is_active": True,
    },
    {
        "location_id": "LOC-003",
        "location_name": "Austin GTM Hub",
        "region": "Central",
        "country": "United States",
        "state": "TX",
        "city": "Austin",
        "time_zone": "America/Chicago",
        "work_model": "Hybrid Hub",
        "cost_index": 0.98,
        "is_active": True,
    },
    {
        "location_id": "LOC-004",
        "location_name": "New York Sales Office",
        "region": "East",
        "country": "United States",
        "state": "NY",
        "city": "New York",
        "time_zone": "America/New_York",
        "work_model": "Hybrid Hub",
        "cost_index": 1.24,
        "is_active": True,
    },
    {
        "location_id": "LOC-005",
        "location_name": "Atlanta Success Hub",
        "region": "East",
        "country": "United States",
        "state": "GA",
        "city": "Atlanta",
        "time_zone": "America/New_York",
        "work_model": "Hybrid Hub",
        "cost_index": 0.94,
        "is_active": True,
    },
    {
        "location_id": "LOC-006",
        "location_name": "Chicago Operations Hub",
        "region": "Central",
        "country": "United States",
        "state": "IL",
        "city": "Chicago",
        "time_zone": "America/Chicago",
        "work_model": "Hybrid Hub",
        "cost_index": 1.02,
        "is_active": True,
    },
    {
        "location_id": "LOC-007",
        "location_name": "London EMEA Hub",
        "region": "EMEA",
        "country": "United Kingdom",
        "state": None,
        "city": "London",
        "time_zone": "Europe/London",
        "work_model": "Hybrid Hub",
        "cost_index": 1.12,
        "is_active": True,
    },
    {
        "location_id": "LOC-008",
        "location_name": "Dublin Support Center",
        "region": "EMEA",
        "country": "Ireland",
        "state": None,
        "city": "Dublin",
        "time_zone": "Europe/Dublin",
        "work_model": "Office First",
        "cost_index": 1.03,
        "is_active": True,
    },
    {
        "location_id": "LOC-009",
        "location_name": "Bangalore Product Center",
        "region": "APAC",
        "country": "India",
        "state": None,
        "city": "Bangalore",
        "time_zone": "Asia/Kolkata",
        "work_model": "Office First",
        "cost_index": 0.62,
        "is_active": True,
    },
    {
        "location_id": "LOC-010",
        "location_name": "Remote - Americas",
        "region": "Americas Remote",
        "country": "United States",
        "state": None,
        "city": "Remote",
        "time_zone": "Mixed",
        "work_model": "Remote",
        "cost_index": 0.96,
        "is_active": True,
    },
]

df_locations = pd.DataFrame(locations_data)
LOCATION_BY_ID = {row["location_id"]: row for row in locations_data}
LOCATION_IDS = [row["location_id"] for row in locations_data]
print(f"locations: {len(df_locations)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Org Units

# COMMAND ----------

# =============================================================================
# TABLE 2: ORG_UNITS (18 rows)
# =============================================================================
org_units_data = [
    ("ORG-001", "Executive Leadership", None, "Corporate", "Corporate", "Global", 10, False),
    ("ORG-002", "Engineering", "ORG-001", "Engineering", "Product & Technology", "Global", 320, True),
    ("ORG-003", "Data & AI Platform", "ORG-002", "Engineering", "Product & Technology", "Global", 190, True),
    ("ORG-004", "Core Platform", "ORG-002", "Engineering", "Product & Technology", "Global", 230, False),
    ("ORG-005", "Product Management", "ORG-001", "Product", "Product & Technology", "Global", 210, True),
    ("ORG-006", "Design & Research", "ORG-005", "Product", "Product & Technology", "Global", 90, False),
    ("ORG-007", "Sales", "ORG-001", "Sales", "Go To Market", "Global", 260, True),
    ("ORG-008", "Sales East", "ORG-007", "Sales", "Go To Market", "East", 230, True),
    ("ORG-009", "Sales West", "ORG-007", "Sales", "Go To Market", "West", 210, True),
    ("ORG-010", "Solutions Consulting", "ORG-007", "Sales", "Go To Market", "Global", 150, True),
    ("ORG-011", "Customer Success", "ORG-001", "Customer Success", "Go To Market", "Global", 260, True),
    ("ORG-012", "Implementation Services", "ORG-011", "Customer Success", "Go To Market", "Global", 130, False),
    ("ORG-013", "Support Operations", "ORG-011", "Customer Success", "Go To Market", "Global", 160, False),
    ("ORG-014", "Marketing", "ORG-001", "Marketing", "Go To Market", "Global", 140, False),
    ("ORG-015", "People & Culture", "ORG-001", "People", "Corporate", "Global", 90, False),
    ("ORG-016", "Finance", "ORG-001", "Finance", "Corporate", "Global", 90, False),
    ("ORG-017", "Legal & Compliance", "ORG-001", "Legal", "Corporate", "Global", 60, False),
    ("ORG-018", "Business Operations", "ORG-001", "Operations", "Corporate", "Global", 130, False),
]

org_units_data = [
    {
        "org_unit_id": org_id,
        "org_unit_name": name,
        "parent_org_unit_id": parent_id,
        "function": function,
        "business_unit": business_unit,
        "region_scope": region_scope,
        "target_headcount_weight": int(weight),
        "is_critical_org": bool(is_critical),
    }
    for org_id, name, parent_id, function, business_unit, region_scope, weight, is_critical in org_units_data
]

df_org_units = pd.DataFrame(org_units_data)
ORG_BY_ID = {row["org_unit_id"]: row for row in org_units_data}
ORG_IDS = [row["org_unit_id"] for row in org_units_data]
ORG_WEIGHTS = [row["target_headcount_weight"] for row in org_units_data]
print(f"org_units: {len(df_org_units)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Job Roles

# COMMAND ----------

# =============================================================================
# TABLE 3: JOB_ROLES (44 rows)
# =============================================================================
role_specs = [
    ("Chief Executive Officer", "Corporate", "Management", 7, "Executive", 330000, True, 8, "Run"),
    ("VP Engineering", "Engineering", "Management", 7, "Executive", 305000, True, 8, "Scale"),
    ("Engineering Director", "Engineering", "Management", 6, "Director", 250000, True, 7, "Scale"),
    ("Engineering Manager", "Engineering", "Management", 5, "Manager", 205000, True, 6, "Scale"),
    ("Staff Software Engineer", "Engineering", "IC", 5, "Staff", 218000, True, 8, "Scale"),
    ("Senior Software Engineer", "Engineering", "IC", 4, "Senior", 178000, False, 6, "Scale"),
    ("Software Engineer", "Engineering", "IC", 3, "Professional", 142000, False, 5, "Maintain"),
    ("Data Platform Engineer", "Data & AI", "IC", 4, "Senior", 188000, True, 8, "Invest"),
    ("ML Engineer", "Data & AI", "IC", 4, "Senior", 196000, True, 9, "Invest"),
    ("AI Platform Architect", "Data & AI", "IC", 5, "Staff", 235000, True, 10, "Invest"),
    ("Data Scientist", "Data & AI", "IC", 3, "Professional", 158000, False, 8, "Invest"),
    ("Analytics Engineer", "Data & AI", "IC", 3, "Professional", 148000, False, 7, "Invest"),
    ("Data Science Manager", "Data & AI", "Management", 5, "Manager", 215000, True, 8, "Invest"),
    ("VP Product", "Product", "Management", 7, "Executive", 295000, True, 8, "Invest"),
    ("Group Product Manager", "Product", "Management", 5, "Manager", 212000, True, 8, "Invest"),
    ("Senior Product Manager", "Product", "IC", 4, "Senior", 180000, False, 7, "Invest"),
    ("Product Manager", "Product", "IC", 3, "Professional", 145000, False, 6, "Maintain"),
    ("Product Operations Manager", "Product", "IC", 3, "Professional", 132000, False, 5, "Maintain"),
    ("Product Designer", "Product", "IC", 3, "Professional", 138000, False, 6, "Maintain"),
    ("Design Manager", "Product", "Management", 5, "Manager", 188000, False, 6, "Maintain"),
    ("VP Sales", "Sales", "Management", 7, "Executive", 285000, True, 8, "Scale"),
    ("Regional Sales Director", "Sales", "Management", 6, "Director", 230000, True, 8, "Scale"),
    ("Sales Manager", "Sales", "Management", 5, "Manager", 175000, True, 7, "Scale"),
    ("Account Executive", "Sales", "IC", 3, "Professional", 128000, False, 6, "Scale"),
    ("Senior Account Executive", "Sales", "IC", 4, "Senior", 158000, False, 7, "Scale"),
    ("Revenue Operations Manager", "Sales", "IC", 4, "Senior", 155000, True, 7, "Maintain"),
    ("Solutions Consultant", "Sales", "IC", 4, "Senior", 168000, True, 8, "Scale"),
    ("VP Customer Success", "Customer Success", "Management", 7, "Executive", 260000, True, 7, "Scale"),
    ("Customer Success Director", "Customer Success", "Management", 6, "Director", 205000, True, 7, "Scale"),
    ("Customer Success Manager", "Customer Success", "IC", 3, "Professional", 118000, False, 5, "Maintain"),
    ("Senior Customer Success Manager", "Customer Success", "IC", 4, "Senior", 145000, False, 6, "Maintain"),
    ("Implementation Consultant", "Customer Success", "IC", 3, "Professional", 124000, False, 6, "Maintain"),
    ("Support Engineer", "Customer Success", "IC", 3, "Professional", 112000, False, 5, "Maintain"),
    ("Demand Generation Manager", "Marketing", "IC", 4, "Senior", 142000, False, 5, "Maintain"),
    ("Product Marketing Manager", "Marketing", "IC", 4, "Senior", 152000, False, 6, "Maintain"),
    ("Marketing Operations Analyst", "Marketing", "IC", 3, "Professional", 108000, False, 4, "Maintain"),
    ("People Business Partner", "People", "IC", 4, "Senior", 138000, False, 5, "Maintain"),
    ("Talent Acquisition Partner", "People", "IC", 3, "Professional", 115000, False, 5, "Maintain"),
    ("Learning Program Manager", "People", "IC", 4, "Senior", 132000, False, 5, "Invest"),
    ("Finance Manager", "Finance", "Management", 5, "Manager", 165000, False, 5, "Maintain"),
    ("Financial Analyst", "Finance", "IC", 3, "Professional", 105000, False, 4, "Maintain"),
    ("Legal Counsel", "Legal", "IC", 4, "Senior", 172000, False, 5, "Maintain"),
    ("Compliance Manager", "Legal", "IC", 4, "Senior", 145000, False, 5, "Maintain"),
    ("Business Operations Manager", "Operations", "IC", 4, "Senior", 142000, False, 5, "Maintain"),
]

job_roles_data = []
for i, spec in enumerate(role_specs, start=1):
    title, family, track, level_num, level_name, midpoint, critical, scarcity, growth_priority = spec
    premium = 0.12 if family == "Data & AI" else 0.08 if family == "Product" else 0.04 if family == "Engineering" else 0.0
    job_roles_data.append(
        {
            "role_id": f"ROLE-{i:03d}",
            "role_title": title,
            "job_family": family,
            "job_track": track,
            "job_level": level_name,
            "job_level_num": int(level_num),
            "base_salary_midpoint_usd": int(midpoint),
            "salary_range_min_usd": int(midpoint * 0.80),
            "salary_range_max_usd": int(midpoint * 1.20),
            "market_premium_pct": round(premium * 100.0, 1),
            "scarcity_score": int(scarcity),
            "is_critical_role": bool(critical),
            "growth_priority": growth_priority,
        }
    )

df_job_roles = pd.DataFrame(job_roles_data)
ROLE_BY_ID = {row["role_id"]: row for row in job_roles_data}
ROLES_BY_FAMILY = {}
for row in job_roles_data:
    ROLES_BY_FAMILY.setdefault(row["job_family"], []).append(row)
print(f"job_roles: {len(df_job_roles)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Skills

# COMMAND ----------

# =============================================================================
# TABLE 4: SKILLS (36 rows)
# =============================================================================
skill_specs = [
    ("Python", "Technical", False, 6),
    ("SQL", "Technical", False, 6),
    ("Spark", "Technical", False, 7),
    ("Cloud Architecture", "Technical", False, 7),
    ("API Design", "Technical", False, 5),
    ("Data Modeling", "Data & AI", False, 6),
    ("Machine Learning", "Data & AI", True, 8),
    ("MLOps", "Data & AI", True, 9),
    ("Generative AI", "Data & AI", True, 10),
    ("Experimentation", "Data & AI", False, 7),
    ("Data Governance", "Data & AI", False, 7),
    ("Product Strategy", "Product", False, 7),
    ("Roadmap Planning", "Product", False, 6),
    ("User Research", "Product", False, 6),
    ("Design Systems", "Product", False, 5),
    ("Pricing Strategy", "GTM", False, 6),
    ("Enterprise Selling", "GTM", False, 7),
    ("Pipeline Management", "GTM", False, 6),
    ("Solution Discovery", "GTM", False, 7),
    ("Customer Health", "GTM", False, 6),
    ("Renewal Strategy", "GTM", False, 6),
    ("Implementation Planning", "GTM", False, 5),
    ("Technical Troubleshooting", "GTM", False, 5),
    ("Demand Generation", "Marketing", False, 5),
    ("Product Messaging", "Marketing", False, 6),
    ("Marketing Analytics", "Marketing", False, 5),
    ("Workforce Planning", "People", False, 6),
    ("Talent Advising", "People", False, 6),
    ("Compensation Analysis", "People", False, 7),
    ("Financial Planning", "Finance", False, 5),
    ("Forecasting", "Finance", False, 5),
    ("Contract Review", "Legal", False, 5),
    ("Risk Management", "Legal", False, 6),
    ("Change Management", "Leadership", False, 6),
    ("People Management", "Leadership", False, 7),
    ("Executive Communication", "Leadership", False, 6),
]

skills_data = [
    {
        "skill_id": f"SKILL-{i:03d}",
        "skill_name": name,
        "skill_category": category,
        "is_emerging_skill": bool(is_emerging),
        "scarcity_score": int(scarcity),
    }
    for i, (name, category, is_emerging, scarcity) in enumerate(skill_specs, start=1)
]

df_skills = pd.DataFrame(skills_data)
SKILL_BY_ID = {row["skill_id"]: row for row in skills_data}
SKILL_ID_BY_NAME = {row["skill_name"]: row["skill_id"] for row in skills_data}
print(f"skills: {len(df_skills)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Employees

# COMMAND ----------

# =============================================================================
# TABLE 5: EMPLOYEES (~2,850 rows, ~2,500 active)
# =============================================================================
ORG_ROLE_FAMILIES = {
    "Engineering": ["Engineering", "Data & AI"],
    "Product": ["Product"],
    "Sales": ["Sales"],
    "Customer Success": ["Customer Success", "Sales"],
    "Marketing": ["Marketing"],
    "People": ["People"],
    "Finance": ["Finance"],
    "Legal": ["Legal"],
    "Operations": ["Operations", "Finance", "People"],
    "Corporate": ["Corporate", "Finance", "People", "Legal"],
}

FUNCTION_LOCATION_WEIGHTS = {
    "Engineering": (["LOC-001", "LOC-002", "LOC-009", "LOC-010"], [24, 30, 28, 18]),
    "Product": (["LOC-001", "LOC-002", "LOC-009", "LOC-010"], [36, 18, 28, 18]),
    "Sales": (["LOC-003", "LOC-004", "LOC-005", "LOC-007", "LOC-010"], [24, 32, 18, 12, 14]),
    "Customer Success": (["LOC-003", "LOC-005", "LOC-008", "LOC-010"], [25, 28, 26, 21]),
    "Marketing": (["LOC-001", "LOC-004", "LOC-010"], [35, 30, 35]),
    "People": (["LOC-001", "LOC-006", "LOC-010"], [35, 25, 40]),
    "Finance": (["LOC-001", "LOC-006", "LOC-010"], [32, 38, 30]),
    "Legal": (["LOC-001", "LOC-004", "LOC-010"], [40, 30, 30]),
    "Operations": (["LOC-003", "LOC-006", "LOC-010"], [30, 35, 35]),
    "Corporate": (["LOC-001", "LOC-004"], [70, 30]),
}


def choose_role_for_org(org):
    families = ORG_ROLE_FAMILIES[org["function"]]
    if org["org_unit_name"] == "Data & AI Platform":
        families = ["Data & AI", "Engineering"]
    if org["org_unit_name"] == "Solutions Consulting":
        families = ["Sales", "Data & AI"]
    role_pool = []
    role_weights = []
    for family in families:
        for role in ROLES_BY_FAMILY.get(family, []):
            role_pool.append(role)
            weight = {3: 42, 4: 30, 5: 15, 6: 7, 7: 2}.get(role["job_level_num"], 4)
            if role["job_track"] == "Management":
                weight *= 0.65
            if org["org_unit_name"] == "Data & AI Platform" and role["job_family"] == "Data & AI":
                weight *= 2.6
            if org["function"] == "Corporate" and role["job_level_num"] < 5:
                weight *= 0.5
            role_weights.append(weight)
    return random.choices(role_pool, weights=role_weights)[0]


def choose_hire_date(role_family):
    if role_family == "Data & AI":
        period = random.choices(
            ["before", "2023", "2024", "2025"], weights=[38, 16, 22, 24]
        )[0]
    elif role_family == "Product":
        period = random.choices(
            ["before", "2023", "2024", "2025"], weights=[50, 17, 18, 15]
        )[0]
    else:
        period = random.choices(
            ["before", "2023", "2024", "2025"], weights=[66, 13, 11, 10]
        )[0]

    if period == "before":
        return date_between(date(2016, 1, 1), date(2022, 12, 31))
    return date_between(date(int(period), 1, 1), date(int(period), 11, 15))


def choose_location(function):
    location_ids, weights = FUNCTION_LOCATION_WEIGHTS[function]
    return random.choices(location_ids, weights=weights)[0]


employees_data = []
for i in range(1, 2851):
    org = random.choices(org_units_data, weights=ORG_WEIGHTS)[0]
    role = choose_role_for_org(org)
    hire_date = choose_hire_date(role["job_family"])
    location_id = choose_location(org["function"])
    location = LOCATION_BY_ID[location_id]
    performance_profile = random.choices(
        ["Developing", "Solid", "Strong", "Exceptional"],
        weights=[9, 54, 29, 8],
    )[0]
    is_high_performer_profile = performance_profile in ("Strong", "Exceptional") and random.random() < (
        0.78 if performance_profile == "Strong" else 0.96
    )
    compa_ratio_profile = random.gauss(0.98, 0.08)
    if is_high_performer_profile and random.random() < 0.28:
        compa_ratio_profile = random.uniform(0.82, 0.89)
    if role["job_family"] == "Product" and random.random() < 0.18:
        compa_ratio_profile -= random.uniform(0.03, 0.08)
    compa_ratio_profile = round(clamp(compa_ratio_profile, 0.72, 1.22), 3)

    mobility_program_participant = bool(
        random.random()
        < (
            0.24
            if is_high_performer_profile
            else 0.10 if role["job_family"] in ("Data & AI", "Product", "Sales") else 0.07
        )
    )

    exit_prob = 0.070
    if org["org_unit_name"] == "Sales East":
        exit_prob += 0.075
    if is_high_performer_profile and compa_ratio_profile < 0.90:
        exit_prob += 0.120
    if role["job_family"] == "Data & AI":
        exit_prob -= 0.020
    if mobility_program_participant:
        exit_prob -= 0.040
    if hire_date > date(2025, 6, 30):
        exit_prob *= 0.25
    exit_prob = clamp(exit_prob, 0.015, 0.32)

    termination_date = None
    termination_type = None
    employment_status = "Active"
    if hire_date < date(2025, 6, 30) and random.random() < exit_prob:
        min_exit = max(hire_date + timedelta(days=180), date(2024, 1, 1))
        if org["org_unit_name"] == "Sales East":
            min_exit = max(min_exit, date(2024, 7, 1))
        if min_exit <= date(2025, 12, 15):
            termination_date = date_between(min_exit, date(2025, 12, 15))
            termination_type = random.choices(
                ["Voluntary", "Involuntary", "Reduction in Force"], weights=[74, 18, 8]
            )[0]
            employment_status = "Inactive"

    employee = {
        "employee_id": f"EMP-{i:05d}",
        "employee_name": synthetic_name(i),
        "hire_date": hire_date,
        "termination_date": termination_date,
        "employment_status": employment_status,
        "termination_type": termination_type,
        "org_unit_id": org["org_unit_id"],
        "location_id": location_id,
        "role_id": role["role_id"],
        "job_family": role["job_family"],
        "job_level": role["job_level"],
        "job_level_num": int(role["job_level_num"]),
        "worker_type": random.choices(["Full Time", "Part Time"], weights=[96, 4])[0],
        "work_model": location["work_model"],
        "current_manager_employee_id": None,
        "mobility_program_participant": mobility_program_participant,
        "performance_profile": performance_profile,
        "high_performer_profile": bool(is_high_performer_profile),
        "compa_ratio_profile": compa_ratio_profile,
        "is_active": employment_status == "Active",
    }
    employees_data.append(employee)

# Assign managers after all employees exist.
EMP_BY_ID = {row["employee_id"]: row for row in employees_data}
employees_by_org = {}
for employee in employees_data:
    employees_by_org.setdefault(employee["org_unit_id"], []).append(employee)

for employee in employees_data:
    if employee["job_level_num"] >= 7:
        continue
    candidates = [
        manager
        for manager in employees_by_org.get(employee["org_unit_id"], [])
        if manager["employee_id"] != employee["employee_id"]
        and manager["job_level_num"] > employee["job_level_num"]
        and ROLE_BY_ID[manager["role_id"]]["job_track"] == "Management"
    ]
    if not candidates:
        parent_id = ORG_BY_ID[employee["org_unit_id"]]["parent_org_unit_id"]
        candidates = [
            manager
            for manager in employees_by_org.get(parent_id, [])
            if manager["job_level_num"] >= max(5, employee["job_level_num"] + 1)
        ]
    if candidates:
        employee["current_manager_employee_id"] = random.choice(candidates)["employee_id"]

df_employees = pd.DataFrame(employees_data)
EMPLOYEE_IDS = [row["employee_id"] for row in employees_data]
ACTIVE_EMPLOYEES = [row for row in employees_data if row["is_active"]]
print(f"employees: {len(df_employees)} rows")
print(f"active employees: {len(ACTIVE_EMPLOYEES)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Employee Skills

# COMMAND ----------

# =============================================================================
# TABLE 6: EMPLOYEE_SKILLS
# =============================================================================
FAMILY_SKILL_NAMES = {
    "Engineering": ["Python", "SQL", "Spark", "Cloud Architecture", "API Design", "Change Management"],
    "Data & AI": ["Python", "SQL", "Spark", "Data Modeling", "Machine Learning", "MLOps", "Generative AI", "Data Governance"],
    "Product": ["Product Strategy", "Roadmap Planning", "User Research", "Experimentation", "Executive Communication"],
    "Sales": ["Enterprise Selling", "Pipeline Management", "Solution Discovery", "Pricing Strategy", "Executive Communication"],
    "Customer Success": ["Customer Health", "Renewal Strategy", "Implementation Planning", "Technical Troubleshooting", "Executive Communication"],
    "Marketing": ["Demand Generation", "Product Messaging", "Marketing Analytics", "Executive Communication"],
    "People": ["Workforce Planning", "Talent Advising", "Compensation Analysis", "Change Management"],
    "Finance": ["Financial Planning", "Forecasting", "SQL", "Executive Communication"],
    "Legal": ["Contract Review", "Risk Management", "Executive Communication"],
    "Operations": ["Workforce Planning", "Forecasting", "Change Management", "Executive Communication"],
    "Corporate": ["Executive Communication", "People Management", "Risk Management", "Forecasting"],
}

employee_skills_data = []
skill_counter = 1
for employee in employees_data:
    role = ROLE_BY_ID[employee["role_id"]]
    family_skills = FAMILY_SKILL_NAMES.get(role["job_family"], ["Executive Communication"])
    selected_names = list(dict.fromkeys(random.sample(family_skills, min(5, len(family_skills))) + ["Change Management"]))
    if role["job_track"] == "Management":
        selected_names.append("People Management")
    selected_names = list(dict.fromkeys(selected_names))[:7]

    for skill_name in selected_names:
        skill = SKILL_BY_ID[SKILL_ID_BY_NAME[skill_name]]
        target_level = clamp(role["job_level_num"] - 1 + (1 if skill["scarcity_score"] >= 8 else 0), 2, 5)
        if role["job_family"] == "Data & AI" and skill["is_emerging_skill"]:
            current_level = random.choices([1, 2, 3, 4], weights=[18, 38, 32, 12])[0]
        else:
            current_level = random.choices([1, 2, 3, 4, 5], weights=[6, 18, 36, 30, 10])[0]
        current_level = int(clamp(current_level + (1 if employee["high_performer_profile"] and random.random() < 0.22 else 0), 1, 5))
        gap = max(0, int(target_level) - current_level)
        employee_skills_data.append(
            {
                "employee_skill_id": f"ESKILL-{skill_counter:08d}",
                "employee_id": employee["employee_id"],
                "skill_id": skill["skill_id"],
                "assessed_date": date(2025, 6, 30),
                "current_proficiency_level": int(current_level),
                "target_proficiency_level": int(target_level),
                "skill_gap": int(gap),
                "is_critical_gap": bool(gap >= 2 and skill["scarcity_score"] >= 7),
            }
        )
        skill_counter += 1

df_employee_skills = pd.DataFrame(employee_skills_data)
SKILL_ROWS_BY_EMP = {}
for row in employee_skills_data:
    SKILL_ROWS_BY_EMP.setdefault(row["employee_id"], []).append(row)
print(f"employee_skills: {len(df_employee_skills)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Learning Activity

# COMMAND ----------

# =============================================================================
# TABLE 7: LEARNING_ACTIVITY
# =============================================================================
learning_activity_data = []
learning_counter = 1
PROGRAM_BY_CATEGORY = {
    "Data & AI": ["GenAI Builder Academy", "MLOps Practitioner Path", "Data Governance Bootcamp"],
    "GTM": ["Enterprise Advisory Selling", "Strategic Renewal Lab", "Solution Discovery Intensive"],
    "Product": ["Product Strategy Studio", "Experimentation Lab", "Research Foundations"],
    "Leadership": ["Manager Essentials", "Leading Through Change", "Executive Communication Lab"],
    "People": ["Talent Advisor Certification", "Workforce Planning Lab", "Compensation Fundamentals"],
}

for employee in employees_data:
    if employee["termination_date"] and employee["termination_date"] < START_DATE:
        continue
    role = ROLE_BY_ID[employee["role_id"]]
    base_activity_count = random.choices([0, 1, 2, 3], weights=[42, 34, 18, 6])[0]
    if employee["mobility_program_participant"]:
        base_activity_count += random.choices([1, 2], weights=[65, 35])[0]
    if role["job_family"] == "Data & AI":
        base_activity_count += random.choice([0, 1])
    skill_rows = sorted(SKILL_ROWS_BY_EMP[employee["employee_id"]], key=lambda row: row["skill_gap"], reverse=True)
    for activity_idx in range(base_activity_count):
        skill_row = skill_rows[min(activity_idx, len(skill_rows) - 1)]
        skill = SKILL_BY_ID[skill_row["skill_id"]]
        category = skill["skill_category"]
        program_names = PROGRAM_BY_CATEGORY.get(category, PROGRAM_BY_CATEGORY.get(role["job_family"], ["Talent Marketplace Accelerator"]))
        if employee["mobility_program_participant"] and activity_idx == 0:
            program_name = "Talent Marketplace Accelerator"
            program_type = "Mobility Program"
            activity_date = date_between(date(2025, 1, 15), date(2025, 11, 15))
        else:
            program_name = random.choice(program_names)
            program_type = random.choice(["Course", "Workshop", "Certification", "Coaching"])
            activity_date = date_between(START_DATE, END_DATE)
        if activity_date < employee["hire_date"]:
            activity_date = employee["hire_date"] + timedelta(days=random.randint(15, 180))
        if employee["termination_date"] and activity_date > employee["termination_date"]:
            continue
        completion_status = random.choices(["Completed", "In Progress", "Dropped"], weights=[82, 13, 5])[0]
        learning_activity_data.append(
            {
                "learning_activity_id": f"LRN-{learning_counter:08d}",
                "employee_id": employee["employee_id"],
                "skill_id": skill["skill_id"],
                "activity_date": activity_date,
                "activity_year": activity_date.year,
                "program_name": program_name,
                "program_type": program_type,
                "skill_category": category,
                "hours_spent": round(random.uniform(2.0, 32.0), 1),
                "completion_status": completion_status,
                "mobility_program_activity": bool(program_name == "Talent Marketplace Accelerator"),
            }
        )
        learning_counter += 1

df_learning_activity = pd.DataFrame(learning_activity_data)
LEARNING_BY_EMP = {}
for row in learning_activity_data:
    LEARNING_BY_EMP.setdefault(row["employee_id"], []).append(row)
print(f"learning_activity: {len(df_learning_activity)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Talent Events

# COMMAND ----------

# =============================================================================
# TABLE 8: TALENT_EVENTS
# =============================================================================
talent_events_data = []
event_counter = 1


def add_talent_event(employee, event_type, event_date, reason, from_org=None, to_org=None, from_role=None, to_role=None):
    global event_counter
    role = ROLE_BY_ID[employee["role_id"]]
    org = ORG_BY_ID[employee["org_unit_id"]]
    is_exit = event_type == "Exit"
    is_regretted_loss = bool(
        is_exit
        and employee["termination_type"] == "Voluntary"
        and (employee["high_performer_profile"] or role["is_critical_role"])
    )
    talent_events_data.append(
        {
            "talent_event_id": f"TEVENT-{event_counter:08d}",
            "event_date": event_date,
            "event_year": event_date.year,
            "event_month": event_date.month,
            "employee_id": employee["employee_id"],
            "event_type": event_type,
            "event_reason": reason,
            "from_org_unit_id": from_org,
            "to_org_unit_id": to_org,
            "from_role_id": from_role,
            "to_role_id": to_role,
            "org_unit_id": employee["org_unit_id"],
            "role_id": employee["role_id"],
            "job_family": role["job_family"],
            "business_unit": org["business_unit"],
            "is_internal_move": bool(event_type in ("Promotion", "Transfer")),
            "is_regretted_loss": is_regretted_loss,
            "mobility_program_related": bool(employee["mobility_program_participant"] and event_date.year == 2025),
        }
    )
    event_counter += 1


for employee in employees_data:
    if START_DATE <= employee["hire_date"] <= END_DATE:
        add_talent_event(
            employee,
            "Hire",
            employee["hire_date"],
            "Growth hire" if ROLE_BY_ID[employee["role_id"]]["growth_priority"] == "Invest" else "Backfill",
            from_org=None,
            to_org=employee["org_unit_id"],
            from_role=None,
            to_role=employee["role_id"],
        )

    move_prob = 0.115
    if employee["mobility_program_participant"]:
        move_prob += 0.190
    if employee["high_performer_profile"]:
        move_prob += 0.035
    if random.random() < move_prob and employee["hire_date"] < date(2025, 6, 1):
        if employee["mobility_program_participant"]:
            move_date = date_between(max(employee["hire_date"] + timedelta(days=180), date(2025, 1, 1)), date(2025, 11, 30))
        else:
            move_date = date_between(max(employee["hire_date"] + timedelta(days=180), START_DATE), date(2025, 11, 30))
        if employee["termination_date"] and move_date > employee["termination_date"]:
            move_date = employee["termination_date"] - timedelta(days=random.randint(30, 120))
        if START_DATE <= move_date <= END_DATE:
            role = ROLE_BY_ID[employee["role_id"]]
            event_type = random.choices(["Promotion", "Transfer"], weights=[58, 42])[0]
            if event_type == "Promotion":
                higher_roles = [
                    candidate
                    for candidate in ROLES_BY_FAMILY.get(role["job_family"], [])
                    if candidate["job_level_num"] == role["job_level_num"] + 1
                ]
                to_role = random.choice(higher_roles)["role_id"] if higher_roles else employee["role_id"]
                to_org = employee["org_unit_id"]
                reason = "Promotion after sustained performance"
            else:
                possible_orgs = [
                    candidate_org
                    for candidate_org in org_units_data
                    if candidate_org["org_unit_id"] != employee["org_unit_id"]
                    and candidate_org["function"] in (ORG_BY_ID[employee["org_unit_id"]]["function"], "Engineering", "Product", "Sales")
                ]
                to_org = random.choice(possible_orgs)["org_unit_id"] if possible_orgs else employee["org_unit_id"]
                to_role = employee["role_id"]
                reason = "Internal mobility transfer"
            add_talent_event(
                employee,
                event_type,
                move_date,
                reason,
                from_org=employee["org_unit_id"],
                to_org=to_org,
                from_role=employee["role_id"],
                to_role=to_role,
            )

    if employee["termination_date"] and START_DATE <= employee["termination_date"] <= END_DATE:
        add_talent_event(
            employee,
            "Exit",
            employee["termination_date"],
            employee["termination_type"],
            from_org=employee["org_unit_id"],
            to_org=None,
            from_role=employee["role_id"],
            to_role=None,
        )

df_talent_events = pd.DataFrame(talent_events_data)
print(f"talent_events: {len(df_talent_events)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Performance Reviews

# COMMAND ----------

# =============================================================================
# TABLE 9: PERFORMANCE_REVIEWS
# =============================================================================
performance_reviews_data = []
review_counter = 1
PERFORMANCE_BY_EMP_YEAR = {}

for employee in employees_data:
    for review_year in YEARS:
        period_start = date(review_year, 1, 1)
        period_end = date(review_year, 12, 31)
        if not active_during(employee, period_start, period_end):
            continue
        base_rating = {
            "Developing": 2.5,
            "Solid": 3.35,
            "Strong": 4.05,
            "Exceptional": 4.65,
        }[employee["performance_profile"]]
        rating = clamp(base_rating + random.gauss(0, 0.28), 1.0, 5.0)
        if employee["mobility_program_participant"] and review_year == 2025:
            rating = clamp(rating + 0.10, 1.0, 5.0)
        potential = random.choices([1, 2, 3], weights=[25, 52, 23])[0]
        if employee["high_performer_profile"]:
            potential = random.choices([2, 3], weights=[38, 62])[0]
        goal_attainment = clamp(72 + rating * 8 + random.gauss(0, 8), 45, 145)
        row = {
            "review_id": f"REV-{review_counter:08d}",
            "employee_id": employee["employee_id"],
            "review_year": review_year,
            "review_date": date(review_year, 12, 15),
            "org_unit_id": employee["org_unit_id"],
            "role_id": employee["role_id"],
            "performance_rating": round(rating, 2),
            "performance_bucket": "High" if rating >= 4.1 else "Solid" if rating >= 3.0 else "Developing",
            "potential_rating": int(potential),
            "goal_attainment_pct": round(goal_attainment, 1),
            "is_high_performer": bool(rating >= 4.1),
        }
        performance_reviews_data.append(row)
        PERFORMANCE_BY_EMP_YEAR[(employee["employee_id"], review_year)] = row
        review_counter += 1

df_performance_reviews = pd.DataFrame(performance_reviews_data)
print(f"performance_reviews: {len(df_performance_reviews)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Compensation Snapshots

# COMMAND ----------

# =============================================================================
# TABLE 10: COMPENSATION_SNAPSHOTS
# =============================================================================
compensation_snapshots_data = []
comp_counter = 1
COMP_BY_EMP_YEAR = {}

for employee in employees_data:
    role = ROLE_BY_ID[employee["role_id"]]
    location = LOCATION_BY_ID[employee["location_id"]]
    market_midpoint = role["base_salary_midpoint_usd"] * (1 + role["market_premium_pct"] / 100.0) * location["cost_index"]
    for comp_year in YEARS:
        snapshot_date = date(comp_year, 12, 31)
        if not active_during(employee, date(comp_year, 1, 1), snapshot_date):
            continue
        merit_progression = 1.0 + 0.018 * (comp_year - 2023)
        if employee["high_performer_profile"]:
            merit_progression += 0.012 * (comp_year - 2023)
        compa_ratio = clamp(employee["compa_ratio_profile"] * merit_progression + random.gauss(0, 0.015), 0.70, 1.25)
        salary = market_midpoint * compa_ratio
        bonus_target = 0.10 + 0.025 * max(0, role["job_level_num"] - 3)
        if role["job_family"] == "Sales":
            bonus_target += 0.10
        review = PERFORMANCE_BY_EMP_YEAR.get((employee["employee_id"], comp_year), {})
        perf_rating = review.get("performance_rating", 3.3)
        bonus_actual = salary * bonus_target * clamp(0.72 + perf_rating / 5.0 * 0.56 + random.gauss(0, 0.08), 0.35, 1.55)
        row = {
            "compensation_snapshot_id": f"COMP-{comp_counter:08d}",
            "employee_id": employee["employee_id"],
            "snapshot_year": comp_year,
            "snapshot_date": snapshot_date,
            "org_unit_id": employee["org_unit_id"],
            "role_id": employee["role_id"],
            "job_family": role["job_family"],
            "job_level": role["job_level"],
            "base_salary_usd": round(salary, 2),
            "bonus_target_pct": round(bonus_target * 100.0, 1),
            "bonus_actual_usd": round(bonus_actual, 2),
            "market_midpoint_usd": round(market_midpoint, 2),
            "salary_range_min_usd": round(market_midpoint * 0.80, 2),
            "salary_range_max_usd": round(market_midpoint * 1.20, 2),
            "compa_ratio": round(compa_ratio, 3),
            "range_penetration_pct": round(clamp((salary - market_midpoint * 0.80) / (market_midpoint * 0.40), 0, 1.25) * 100.0, 1),
            "pay_band": f"{role['job_level']} {role['job_family']}",
        }
        compensation_snapshots_data.append(row)
        COMP_BY_EMP_YEAR[(employee["employee_id"], comp_year)] = row
        comp_counter += 1

df_compensation_snapshots = pd.DataFrame(compensation_snapshots_data)
print(f"compensation_snapshots: {len(df_compensation_snapshots)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Engagement Pulses and Retention Risk

# COMMAND ----------

# =============================================================================
# TABLE 11: ENGAGEMENT_PULSES
# TABLE 12: RETENTION_RISK_SNAPSHOTS
# =============================================================================
engagement_pulses_data = []
retention_risk_data = []
pulse_counter = 1
risk_counter = 1

for employee in employees_data:
    role = ROLE_BY_ID[employee["role_id"]]
    org = ORG_BY_ID[employee["org_unit_id"]]
    base_engagement = random.gauss(3.75, 0.42)
    if employee["high_performer_profile"]:
        base_engagement += 0.12
    if org["org_unit_name"] == "Sales East":
        base_engagement -= 0.08
    for q_end in QUARTER_ENDS:
        if not is_employed_on(employee, q_end):
            continue
        year = q_end.year
        quarter = ((q_end.month - 1) // 3) + 1
        engagement = base_engagement + random.gauss(0, 0.22)
        manager_effectiveness = random.gauss(3.82, 0.38)
        workload = random.gauss(3.18, 0.45)
        if org["org_unit_name"] == "Sales East" and year == 2024:
            engagement -= 0.72
            manager_effectiveness -= 0.38
            workload += 0.62
        if employee["mobility_program_participant"] and year == 2025:
            engagement += 0.28
            manager_effectiveness += 0.10
            workload -= 0.12
        if role["job_family"] == "Data & AI":
            workload += 0.18
        engagement = round(clamp(engagement, 1.0, 5.0), 2)
        manager_effectiveness = round(clamp(manager_effectiveness, 1.0, 5.0), 2)
        workload = round(clamp(workload, 1.0, 5.0), 2)
        burnout_risk = round(clamp(1.2 + workload * 0.72 + random.gauss(0, 0.28), 1.0, 5.0), 2)
        row = {
            "engagement_pulse_id": f"PULSE-{pulse_counter:08d}",
            "pulse_date": q_end,
            "pulse_year": year,
            "pulse_quarter": quarter,
            "pulse_quarter_label": quarter_label(q_end),
            "employee_id": employee["employee_id"],
            "org_unit_id": employee["org_unit_id"],
            "role_id": employee["role_id"],
            "job_family": role["job_family"],
            "engagement_score": engagement,
            "manager_effectiveness_score": manager_effectiveness,
            "workload_score": workload,
            "growth_opportunity_score": round(clamp(random.gauss(3.55, 0.48) + (0.25 if employee["mobility_program_participant"] and year == 2025 else 0), 1.0, 5.0), 2),
            "burnout_risk_score": burnout_risk,
        }
        engagement_pulses_data.append(row)
        pulse_counter += 1

        comp = COMP_BY_EMP_YEAR.get((employee["employee_id"], year), {})
        review = PERFORMANCE_BY_EMP_YEAR.get((employee["employee_id"], year), {})
        compa_ratio = comp.get("compa_ratio", employee["compa_ratio_profile"])
        is_high_performer = bool(review.get("is_high_performer", employee["high_performer_profile"]))
        risk_score = 22 + (5.0 - engagement) * 9.5 + burnout_risk * 5.0
        risk_driver = "Engagement"
        if is_high_performer and compa_ratio < 0.90:
            risk_score += 23
            risk_driver = "Below-market compensation for high performer"
        if org["org_unit_name"] == "Sales East" and year in (2024, 2025):
            risk_score += 14
            risk_driver = "Sales East engagement dip"
        if role["job_family"] == "Data & AI" and role["scarcity_score"] >= 8:
            risk_score += 7
            risk_driver = "Scarce external market"
        if employee["mobility_program_participant"] and year == 2025:
            risk_score -= 16
            if risk_driver == "Engagement":
                risk_driver = "Mobility program mitigated risk"
        learning_rows = [
            learning
            for learning in LEARNING_BY_EMP.get(employee["employee_id"], [])
            if learning["activity_year"] == year and learning["completion_status"] == "Completed"
        ]
        if learning_rows:
            risk_score -= 4
        risk_score = round(clamp(risk_score + random.gauss(0, 4), 1, 99), 1)
        risk_band = "High" if risk_score >= 70 else "Medium" if risk_score >= 45 else "Low"
        retention_risk_data.append(
            {
                "retention_risk_snapshot_id": f"RISK-{risk_counter:08d}",
                "snapshot_date": q_end,
                "snapshot_year": year,
                "snapshot_quarter": quarter,
                "snapshot_quarter_label": quarter_label(q_end),
                "employee_id": employee["employee_id"],
                "org_unit_id": employee["org_unit_id"],
                "role_id": employee["role_id"],
                "job_family": role["job_family"],
                "risk_score": risk_score,
                "risk_band": risk_band,
                "risk_driver": risk_driver,
                "compa_ratio": round(compa_ratio, 3),
                "is_high_performer": is_high_performer,
                "mobility_program_participant": bool(employee["mobility_program_participant"]),
                "recommended_action": "Retention conversation" if risk_band == "High" else "Monitor" if risk_band == "Medium" else "No immediate action",
            }
        )
        risk_counter += 1

df_engagement_pulses = pd.DataFrame(engagement_pulses_data)
df_retention_risk_snapshots = pd.DataFrame(retention_risk_data)
print(f"engagement_pulses: {len(df_engagement_pulses)} rows")
print(f"retention_risk_snapshots: {len(df_retention_risk_snapshots)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Requisitions

# COMMAND ----------

# =============================================================================
# TABLE 13: REQUISITIONS
# =============================================================================
requisitions_data = []
req_counter = 1


def choose_org_for_family(job_family):
    candidates = []
    weights = []
    for org in org_units_data:
        families = ORG_ROLE_FAMILIES[org["function"]]
        if org["org_unit_name"] == "Data & AI Platform":
            families = ["Data & AI", "Engineering"]
        if org["org_unit_name"] == "Solutions Consulting":
            families = ["Sales", "Data & AI"]
        if job_family in families:
            candidates.append(org)
            weights.append(org["target_headcount_weight"])
    return random.choices(candidates, weights=weights)[0]


for year, req_count in [(2023, 170), (2024, 210), (2025, 260)]:
    family_weights = {
        "Engineering": 20,
        "Data & AI": 8 + (year - 2023) * 8,
        "Product": 12 + (year - 2023) * 2,
        "Sales": 24,
        "Customer Success": 18,
        "Marketing": 7,
        "People": 4,
        "Finance": 3,
        "Legal": 2,
        "Operations": 4,
    }
    families = list(family_weights.keys())
    weights = list(family_weights.values())
    for _ in range(req_count):
        job_family = random.choices(families, weights=weights)[0]
        role = random.choice(ROLES_BY_FAMILY[job_family])
        org = choose_org_for_family(job_family)
        location_id = choose_location(org["function"])
        open_date = date_between(date(year, 1, 1), date(year, 12, 10))
        status = random.choices(["Filled", "Open", "Cancelled"], weights=[78, 15, 7])[0]
        base_ttf = random.randint(28, 72)
        if job_family == "Data & AI":
            base_ttf += random.randint(20, 38)
        if job_family == "Product":
            base_ttf += random.randint(8, 24)
        if role["is_critical_role"]:
            base_ttf += random.randint(8, 22)
        close_date = None
        time_to_fill_days = None
        if status == "Filled":
            time_to_fill_days = int(base_ttf)
            close_date = min(END_DATE, open_date + timedelta(days=time_to_fill_days))
        elif status == "Cancelled":
            close_date = min(END_DATE, open_date + timedelta(days=random.randint(20, 100)))
        internal_fill_prob = 0.16 if year < 2025 else 0.34
        if job_family in ("Product", "Data & AI"):
            internal_fill_prob += 0.06
        is_internal_fill = bool(status == "Filled" and random.random() < internal_fill_prob)
        eligible_internal = [
            emp
            for emp in ACTIVE_EMPLOYEES
            if emp["job_family"] == job_family
            and emp["employee_id"] not in (None, "")
            and (emp["mobility_program_participant"] or random.random() < 0.18)
        ]
        filled_by_employee_id = random.choice(eligible_internal)["employee_id"] if is_internal_fill and eligible_internal else None
        hiring_managers = [
            emp
            for emp in ACTIVE_EMPLOYEES
            if emp["org_unit_id"] == org["org_unit_id"] and ROLE_BY_ID[emp["role_id"]]["job_track"] == "Management"
        ]
        requisitions_data.append(
            {
                "requisition_id": f"REQ-{req_counter:06d}",
                "opened_date": open_date,
                "opened_year": year,
                "opened_month": open_date.month,
                "closed_date": close_date,
                "status": status,
                "org_unit_id": org["org_unit_id"],
                "location_id": location_id,
                "role_id": role["role_id"],
                "job_family": job_family,
                "hiring_manager_employee_id": random.choice(hiring_managers)["employee_id"] if hiring_managers else None,
                "requisition_reason": "Growth" if job_family == "Data & AI" or random.random() < 0.58 else "Backfill",
                "priority": "Critical" if role["is_critical_role"] else random.choices(["High", "Standard"], weights=[35, 65])[0],
                "approved_headcount": 1,
                "time_to_fill_days": time_to_fill_days,
                "is_internal_fill": is_internal_fill,
                "filled_by_employee_id": filled_by_employee_id,
                "candidate_market": "Scarce" if role["scarcity_score"] >= 8 else "Competitive" if role["scarcity_score"] >= 6 else "Balanced",
            }
        )
        req_counter += 1

df_requisitions = pd.DataFrame(requisitions_data)
REQ_BY_ID = {row["requisition_id"]: row for row in requisitions_data}
print(f"requisitions: {len(df_requisitions)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Applications

# COMMAND ----------

# =============================================================================
# TABLE 14: APPLICATIONS
# =============================================================================
applications_data = []
app_counter = 1

for req in requisitions_data:
    role = ROLE_BY_ID[req["role_id"]]
    app_count = random.randint(9, 24)
    if role["job_family"] == "Data & AI":
        app_count = random.randint(5, 15)
    if req["candidate_market"] == "Scarce":
        app_count = max(4, app_count - random.randint(2, 6))
    accepted_created = False
    for app_idx in range(app_count):
        source = random.choices(
            ["Inbound", "Referral", "Sourcing", "Agency", "Internal"],
            weights=[34, 22, 24, 8, 12 if req["opened_year"] < 2025 else 22],
        )[0]
        candidate_employee_id = None
        if source == "Internal":
            internal_candidates = [
                emp
                for emp in ACTIVE_EMPLOYEES
                if emp["job_family"] == role["job_family"] and emp["employee_id"] != req.get("filled_by_employee_id")
            ]
            if internal_candidates:
                candidate_employee_id = random.choice(internal_candidates)["employee_id"]
        stage = random.choices(
            ["Applied", "Recruiter Screen", "Interview", "Final Interview", "Offer", "Rejected"],
            weights=[38, 20, 22, 8, 5, 7],
        )[0]
        offer_salary = None
        offer_date = None
        offer_accepted = None
        current_status = "In Process" if req["status"] == "Open" and stage != "Rejected" else "Rejected"
        interview_count = 0
        decline_reason = None
        if stage in ("Interview", "Final Interview", "Offer"):
            interview_count = random.randint(1, 4)
        if stage == "Offer":
            offer_date = req["opened_date"] + timedelta(days=random.randint(18, max(24, req["time_to_fill_days"] or 90)))
            market_midpoint = role["base_salary_midpoint_usd"] * (1 + role["market_premium_pct"] / 100.0)
            offer_salary = market_midpoint * random.uniform(0.86, 1.10)
            below_midpoint = offer_salary < market_midpoint
            accept_prob = 0.58
            if role["job_family"] == "Product" and below_midpoint:
                accept_prob = 0.27
            elif below_midpoint:
                accept_prob = 0.43
            if source == "Internal":
                accept_prob += 0.16
            offer_accepted = bool(req["status"] == "Filled" and random.random() < accept_prob)
            current_status = "Offer Accepted" if offer_accepted else "Offer Declined"
            if offer_accepted:
                accepted_created = True
            elif below_midpoint:
                decline_reason = "Compensation below market midpoint"
            else:
                decline_reason = random.choice(["Accepted competing offer", "Role scope mismatch", "Timing"])
        applications_data.append(
            {
                "application_id": f"APP-{app_counter:08d}",
                "requisition_id": req["requisition_id"],
                "application_date": max(req["opened_date"], req["opened_date"] + timedelta(days=random.randint(0, 45))),
                "candidate_source": source,
                "candidate_employee_id": candidate_employee_id,
                "candidate_stage": stage,
                "current_status": current_status,
                "interview_count": int(interview_count),
                "offer_date": offer_date,
                "offer_salary_usd": None if offer_salary is None else round(offer_salary, 2),
                "offer_to_market_midpoint_pct": None if offer_salary is None else round(offer_salary * 100.0 / (role["base_salary_midpoint_usd"] * (1 + role["market_premium_pct"] / 100.0)), 1),
                "offer_accepted": offer_accepted,
                "decline_reason": decline_reason,
                "candidate_quality_score": round(clamp(random.gauss(72, 13), 25, 99), 1),
            }
        )
        app_counter += 1

    if req["status"] == "Filled" and not accepted_created:
        source = "Internal" if req["is_internal_fill"] else random.choice(["Referral", "Sourcing", "Inbound"])
        role_mid = role["base_salary_midpoint_usd"] * (1 + role["market_premium_pct"] / 100.0)
        offer_salary = role_mid * random.uniform(0.95, 1.12)
        applications_data.append(
            {
                "application_id": f"APP-{app_counter:08d}",
                "requisition_id": req["requisition_id"],
                "application_date": req["opened_date"] + timedelta(days=random.randint(2, 30)),
                "candidate_source": source,
                "candidate_employee_id": req["filled_by_employee_id"] if source == "Internal" else None,
                "candidate_stage": "Offer",
                "current_status": "Offer Accepted",
                "interview_count": random.randint(2, 5),
                "offer_date": req["closed_date"] or req["opened_date"] + timedelta(days=req["time_to_fill_days"] or 60),
                "offer_salary_usd": round(offer_salary, 2),
                "offer_to_market_midpoint_pct": round(offer_salary * 100.0 / role_mid, 1),
                "offer_accepted": True,
                "decline_reason": None,
                "candidate_quality_score": round(clamp(random.gauss(84, 7), 55, 99), 1),
            }
        )
        app_counter += 1

df_applications = pd.DataFrame(applications_data)
print(f"applications: {len(df_applications)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Succession Plans

# COMMAND ----------

# =============================================================================
# TABLE 15: SUCCESSION_PLANS
# =============================================================================
succession_plans_data = []
succession_counter = 1

critical_incumbents = [
    employee
    for employee in ACTIVE_EMPLOYEES
    if ROLE_BY_ID[employee["role_id"]]["is_critical_role"] and employee["job_level_num"] >= 4
]
random.shuffle(critical_incumbents)

for employee in critical_incumbents[:520]:
    role = ROLE_BY_ID[employee["role_id"]]
    org = ORG_BY_ID[employee["org_unit_id"]]
    weak_coverage_org = org["business_unit"] == "Go To Market" or org["org_unit_name"] == "Data & AI Platform"
    coverage_prob = 0.54 if weak_coverage_org else 0.78
    has_successor = random.random() < coverage_prob
    successor_rows = []
    if has_successor:
        successor_pool = [
            candidate
            for candidate in ACTIVE_EMPLOYEES
            if candidate["employee_id"] != employee["employee_id"]
            and candidate["job_family"] == role["job_family"]
            and candidate["job_level_num"] >= max(3, role["job_level_num"] - 2)
            and candidate["job_level_num"] <= role["job_level_num"]
        ]
        successor_count = random.choices([1, 2], weights=[72, 28])[0]
        successor_rows = random.sample(successor_pool, min(successor_count, len(successor_pool))) if successor_pool else []
    if not successor_rows:
        succession_plans_data.append(
            {
                "succession_plan_id": f"SUCC-{succession_counter:07d}",
                "plan_review_date": date(2025, 9, 30),
                "org_unit_id": employee["org_unit_id"],
                "role_id": employee["role_id"],
                "incumbent_employee_id": employee["employee_id"],
                "successor_employee_id": None,
                "criticality_tier": "Tier 1" if role["scarcity_score"] >= 8 else "Tier 2",
                "readiness_level": "No Named Successor",
                "readiness_months": None,
                "coverage_status": "No Ready Successor",
                "successor_retention_risk_band": None,
                "weak_coverage_flag": True,
            }
        )
        succession_counter += 1
    else:
        for successor in successor_rows:
            readiness = random.choices(
                ["Ready Now", "Ready <12 Months", "Ready 12-24 Months", "Ready 24+ Months"],
                weights=[18, 34, 34, 14] if not weak_coverage_org else [8, 24, 44, 24],
            )[0]
            readiness_months = {
                "Ready Now": 0,
                "Ready <12 Months": random.randint(3, 11),
                "Ready 12-24 Months": random.randint(12, 24),
                "Ready 24+ Months": random.randint(25, 36),
            }[readiness]
            succession_plans_data.append(
                {
                    "succession_plan_id": f"SUCC-{succession_counter:07d}",
                    "plan_review_date": date(2025, 9, 30),
                    "org_unit_id": employee["org_unit_id"],
                    "role_id": employee["role_id"],
                    "incumbent_employee_id": employee["employee_id"],
                    "successor_employee_id": successor["employee_id"],
                    "criticality_tier": "Tier 1" if role["scarcity_score"] >= 8 else "Tier 2",
                    "readiness_level": readiness,
                    "readiness_months": readiness_months,
                    "coverage_status": "Covered" if readiness in ("Ready Now", "Ready <12 Months") else "Developing",
                    "successor_retention_risk_band": random.choices(["Low", "Medium", "High"], weights=[58, 32, 10])[0],
                    "weak_coverage_flag": bool(weak_coverage_org and readiness not in ("Ready Now", "Ready <12 Months")),
                }
            )
            succession_counter += 1

df_succession_plans = pd.DataFrame(succession_plans_data)
print(f"succession_plans: {len(df_succession_plans)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Curated Topic Marts

# COMMAND ----------

# =============================================================================
# CURATED TOPIC MARTS
# =============================================================================
df_emp = df_employees.copy()
df_emp["hire_month"] = df_emp["hire_date"].apply(lambda d: date(d.year, d.month, 1))
df_emp["termination_month"] = df_emp["termination_date"].apply(
    lambda d: None if pd.isna(d) or d is None else date(d.year, d.month, 1)
)

# Workforce planning mart: monthly by org, job family, and location.
workforce_rows = []
workforce_counter = 1
employee_combos = df_emp[["org_unit_id", "job_family", "location_id"]].drop_duplicates().to_dict("records")
for year, month in MONTH_LIST:
    month_start = date(year, month, 1)
    month_end = end_of_month(year, month)
    for combo in employee_combos:
        org_id = combo["org_unit_id"]
        job_family = combo["job_family"]
        location_id = combo["location_id"]
        emp_mask = (
            (df_emp["org_unit_id"] == org_id)
            & (df_emp["job_family"] == job_family)
            & (df_emp["location_id"] == location_id)
        )
        active_mask = emp_mask & (df_emp["hire_date"] <= month_end) & (
            df_emp["termination_date"].isna() | (df_emp["termination_date"] > month_end)
        )
        start_headcount_mask = emp_mask & (df_emp["hire_date"] < month_start) & (
            df_emp["termination_date"].isna() | (df_emp["termination_date"] >= month_start)
        )
        hires = int((emp_mask & (df_emp["hire_month"] == month_start)).sum())
        exits = int((emp_mask & (df_emp["termination_month"] == month_start)).sum())
        active_headcount = int(active_mask.sum())
        start_headcount = int(start_headcount_mask.sum())
        req_mask = (
            (df_requisitions["org_unit_id"] == org_id)
            & (df_requisitions["job_family"] == job_family)
            & (df_requisitions["location_id"] == location_id)
        )
        open_reqs = int(
            (
                req_mask
                & (df_requisitions["opened_date"] <= month_end)
                & (
                    df_requisitions["closed_date"].isna()
                    | (df_requisitions["closed_date"] > month_end)
                )
                & (df_requisitions["status"] == "Open")
            ).sum()
        )
        filled_reqs = int(
            (
                req_mask
                & (df_requisitions["status"] == "Filled")
                & df_requisitions["closed_date"].notna()
                & (df_requisitions["closed_date"] >= month_start)
                & (df_requisitions["closed_date"] <= month_end)
            ).sum()
        )
        if active_headcount + hires + exits + open_reqs + filled_reqs == 0:
            continue
        org = ORG_BY_ID[org_id]
        workforce_rows.append(
            {
                "mart_workforce_planning_id": f"MWP-{workforce_counter:08d}",
                "snapshot_month": month_start,
                "snapshot_year": year,
                "snapshot_month_num": month,
                "org_unit_id": org_id,
                "org_unit_name": org["org_unit_name"],
                "business_unit": org["business_unit"],
                "function": org["function"],
                "job_family": job_family,
                "location_id": location_id,
                "region": LOCATION_BY_ID[location_id]["region"],
                "active_headcount": active_headcount,
                "start_headcount": start_headcount,
                "hires": hires,
                "exits": exits,
                "open_requisitions": open_reqs,
                "filled_requisitions": filled_reqs,
                "attrition_rate_pct": round(exits * 100.0 / start_headcount, 2) if start_headcount else 0.0,
                "vacancy_rate_pct": round(open_reqs * 100.0 / (active_headcount + open_reqs), 2) if active_headcount + open_reqs else 0.0,
            }
        )
        workforce_counter += 1

df_mart_workforce_planning = pd.DataFrame(workforce_rows)

# Hiring funnel mart: one row per requisition.
app_group = df_applications.groupby("requisition_id")
hiring_rows = []
for req in requisitions_data:
    role = ROLE_BY_ID[req["role_id"]]
    org = ORG_BY_ID[req["org_unit_id"]]
    apps = app_group.get_group(req["requisition_id"]) if req["requisition_id"] in app_group.groups else pd.DataFrame()
    offers = apps[apps["candidate_stage"] == "Offer"] if len(apps) else pd.DataFrame()
    accepted = apps[apps["current_status"] == "Offer Accepted"] if len(apps) else pd.DataFrame()
    declined = apps[apps["current_status"] == "Offer Declined"] if len(apps) else pd.DataFrame()
    hiring_rows.append(
        {
            "mart_hiring_funnel_id": f"MHF-{len(hiring_rows) + 1:08d}",
            "requisition_id": req["requisition_id"],
            "opened_date": req["opened_date"],
            "opened_year": req["opened_year"],
            "status": req["status"],
            "org_unit_id": req["org_unit_id"],
            "org_unit_name": org["org_unit_name"],
            "business_unit": org["business_unit"],
            "role_id": req["role_id"],
            "role_title": role["role_title"],
            "job_family": role["job_family"],
            "priority": req["priority"],
            "candidate_market": req["candidate_market"],
            "requisition_reason": req["requisition_reason"],
            "applications_count": int(len(apps)),
            "internal_applications": int((apps["candidate_source"] == "Internal").sum()) if len(apps) else 0,
            "interview_count": int((apps["interview_count"] > 0).sum()) if len(apps) else 0,
            "offer_count": int(len(offers)),
            "accepted_offer_count": int(len(accepted)),
            "declined_offer_count": int(len(declined)),
            "acceptance_rate_pct": round(len(accepted) * 100.0 / len(offers), 2) if len(offers) else 0.0,
            "avg_offer_to_market_midpoint_pct": round(float(offers["offer_to_market_midpoint_pct"].mean()), 2) if len(offers) else None,
            "time_to_fill_days": req["time_to_fill_days"],
            "is_internal_fill": bool(req["is_internal_fill"]),
        }
    )

df_mart_hiring_funnel = pd.DataFrame(hiring_rows)

# Retention and engagement mart: quarterly by org and job family.
risk_enriched = df_retention_risk_snapshots.merge(
    df_org_units[["org_unit_id", "org_unit_name", "business_unit", "function"]],
    on="org_unit_id",
    how="left",
)
retention_rows = []
for keys, group in risk_enriched.groupby(["snapshot_year", "snapshot_quarter", "snapshot_quarter_label", "org_unit_id", "org_unit_name", "business_unit", "function", "job_family"]):
    snapshot_year, snapshot_quarter, snapshot_quarter_label, org_id, org_name, business_unit, function, job_family = keys
    exits = df_talent_events[
        (df_talent_events["event_type"] == "Exit")
        & (df_talent_events["event_year"] == snapshot_year)
        & (((df_talent_events["event_month"] - 1) // 3 + 1) == snapshot_quarter)
        & (df_talent_events["org_unit_id"] == org_id)
        & (df_talent_events["job_family"] == job_family)
    ]
    pulses = df_engagement_pulses[
        (df_engagement_pulses["pulse_year"] == snapshot_year)
        & (df_engagement_pulses["pulse_quarter"] == snapshot_quarter)
        & (df_engagement_pulses["org_unit_id"] == org_id)
        & (df_engagement_pulses["job_family"] == job_family)
    ]
    retention_rows.append(
        {
            "mart_retention_engagement_id": f"MRE-{len(retention_rows) + 1:08d}",
            "snapshot_year": int(snapshot_year),
            "snapshot_quarter": int(snapshot_quarter),
            "snapshot_quarter_label": snapshot_quarter_label,
            "org_unit_id": org_id,
            "org_unit_name": org_name,
            "business_unit": business_unit,
            "function": function,
            "job_family": job_family,
            "employee_count": int(group["employee_id"].nunique()),
            "avg_engagement_score": round(float(pulses["engagement_score"].mean()), 2) if len(pulses) else None,
            "avg_manager_effectiveness_score": round(float(pulses["manager_effectiveness_score"].mean()), 2) if len(pulses) else None,
            "avg_burnout_risk_score": round(float(pulses["burnout_risk_score"].mean()), 2) if len(pulses) else None,
            "avg_retention_risk_score": round(float(group["risk_score"].mean()), 2),
            "high_risk_employee_count": int((group["risk_band"] == "High").sum()),
            "regretted_exit_count": int(exits["is_regretted_loss"].sum()) if len(exits) else 0,
            "total_exit_count": int(len(exits)),
            "mobility_program_participants": int(group[group["mobility_program_participant"]]["employee_id"].nunique()),
        }
    )

df_mart_retention_engagement = pd.DataFrame(retention_rows)

# Internal mobility mart: annual by org and job family.
mobility_rows = []
skill_df = df_employee_skills.merge(df_employees[["employee_id", "org_unit_id", "job_family", "mobility_program_participant"]], on="employee_id")
learning_df = df_learning_activity.merge(df_employees[["employee_id", "org_unit_id", "job_family"]], on="employee_id")
for year in YEARS:
    active_year = df_emp[
        (df_emp["hire_date"] <= date(year, 12, 31))
        & (df_emp["termination_date"].isna() | (df_emp["termination_date"] >= date(year, 1, 1)))
    ]
    for keys, group in active_year.groupby(["org_unit_id", "job_family"]):
        org_id, job_family = keys
        org = ORG_BY_ID[org_id]
        events = df_talent_events[
            (df_talent_events["event_year"] == year)
            & (df_talent_events["org_unit_id"] == org_id)
            & (df_talent_events["job_family"] == job_family)
        ]
        learning = learning_df[
            (learning_df["activity_year"] == year)
            & (learning_df["org_unit_id"] == org_id)
            & (learning_df["job_family"] == job_family)
        ]
        skills = skill_df[
            (skill_df["org_unit_id"] == org_id)
            & (skill_df["job_family"] == job_family)
        ]
        reqs = df_requisitions[
            (df_requisitions["opened_year"] == year)
            & (df_requisitions["org_unit_id"] == org_id)
            & (df_requisitions["job_family"] == job_family)
        ]
        mobility_rows.append(
            {
                "mart_internal_mobility_id": f"MIM-{len(mobility_rows) + 1:08d}",
                "snapshot_year": year,
                "org_unit_id": org_id,
                "org_unit_name": org["org_unit_name"],
                "business_unit": org["business_unit"],
                "function": org["function"],
                "job_family": job_family,
                "employee_count": int(group["employee_id"].nunique()),
                "mobility_program_participants": int(group[group["mobility_program_participant"]]["employee_id"].nunique()),
                "promotion_count": int((events["event_type"] == "Promotion").sum()) if len(events) else 0,
                "transfer_count": int((events["event_type"] == "Transfer").sum()) if len(events) else 0,
                "internal_fill_count": int(reqs["is_internal_fill"].sum()) if len(reqs) else 0,
                "learning_completion_count": int((learning["completion_status"] == "Completed").sum()) if len(learning) else 0,
                "avg_skill_gap": round(float(skills["skill_gap"].mean()), 2) if len(skills) else None,
                "critical_skill_gap_count": int(skills["is_critical_gap"].sum()) if len(skills) else 0,
            }
        )

df_mart_internal_mobility = pd.DataFrame(mobility_rows)

# Compensation and performance mart: annual by org, family, and level.
comp_perf_df = df_compensation_snapshots.merge(
    df_performance_reviews[
        ["employee_id", "review_year", "performance_rating", "is_high_performer", "potential_rating"]
    ],
    left_on=["employee_id", "snapshot_year"],
    right_on=["employee_id", "review_year"],
    how="left",
)
comp_perf_df = comp_perf_df.merge(
    df_org_units[["org_unit_id", "org_unit_name", "business_unit", "function"]],
    on="org_unit_id",
    how="left",
)
latest_high_risk = (
    df_retention_risk_snapshots.groupby(["employee_id", "snapshot_year"])["risk_score"]
    .max()
    .reset_index()
    .rename(columns={"risk_score": "max_retention_risk_score"})
)
comp_perf_df = comp_perf_df.merge(
    latest_high_risk,
    left_on=["employee_id", "snapshot_year"],
    right_on=["employee_id", "snapshot_year"],
    how="left",
)
comp_rows = []
for keys, group in comp_perf_df.groupby(["snapshot_year", "org_unit_id", "org_unit_name", "business_unit", "function", "job_family", "job_level"]):
    snapshot_year, org_id, org_name, business_unit, function, job_family, job_level = keys
    high_perf_low_comp = group[(group["is_high_performer"] == True) & (group["compa_ratio"] < 0.90)]
    comp_rows.append(
        {
            "mart_comp_performance_id": f"MCP-{len(comp_rows) + 1:08d}",
            "snapshot_year": int(snapshot_year),
            "org_unit_id": org_id,
            "org_unit_name": org_name,
            "business_unit": business_unit,
            "function": function,
            "job_family": job_family,
            "job_level": job_level,
            "employee_count": int(group["employee_id"].nunique()),
            "avg_base_salary_usd": round(float(group["base_salary_usd"].mean()), 2),
            "avg_bonus_actual_usd": round(float(group["bonus_actual_usd"].mean()), 2),
            "avg_compa_ratio": round(float(group["compa_ratio"].mean()), 3),
            "avg_range_penetration_pct": round(float(group["range_penetration_pct"].mean()), 1),
            "avg_performance_rating": round(float(group["performance_rating"].mean()), 2),
            "high_performer_count": int((group["is_high_performer"] == True).sum()),
            "high_performer_below_90_compa_count": int(len(high_perf_low_comp)),
            "high_risk_employee_count": int((group["max_retention_risk_score"] >= 70).sum()),
        }
    )

df_mart_comp_performance = pd.DataFrame(comp_rows)

# Succession planning mart: by org, family, and criticality.
succession_df = df_succession_plans.merge(df_job_roles[["role_id", "job_family", "role_title"]], on="role_id", how="left")
succession_df = succession_df.merge(df_org_units[["org_unit_id", "org_unit_name", "business_unit", "function"]], on="org_unit_id", how="left")
succession_rows = []
for keys, group in succession_df.groupby(["org_unit_id", "org_unit_name", "business_unit", "function", "job_family", "criticality_tier"]):
    org_id, org_name, business_unit, function, job_family, criticality_tier = keys
    critical_positions = int(group["incumbent_employee_id"].nunique())
    ready_successors = int(group[group["readiness_level"].isin(["Ready Now", "Ready <12 Months"])]["successor_employee_id"].nunique())
    covered_positions = int(group[group["coverage_status"] == "Covered"]["incumbent_employee_id"].nunique())
    succession_rows.append(
        {
            "mart_succession_planning_id": f"MSP-{len(succession_rows) + 1:08d}",
            "plan_review_date": date(2025, 9, 30),
            "org_unit_id": org_id,
            "org_unit_name": org_name,
            "business_unit": business_unit,
            "function": function,
            "job_family": job_family,
            "criticality_tier": criticality_tier,
            "critical_position_count": critical_positions,
            "ready_successor_count": ready_successors,
            "covered_position_count": covered_positions,
            "weak_coverage_position_count": int(group[group["weak_coverage_flag"]]["incumbent_employee_id"].nunique()),
            "coverage_rate_pct": round(covered_positions * 100.0 / critical_positions, 2) if critical_positions else 0.0,
            "avg_readiness_months": round(float(group["readiness_months"].dropna().mean()), 1) if group["readiness_months"].notna().any() else None,
        }
    )

df_mart_succession_planning = pd.DataFrame(succession_rows)

print(f"mart_workforce_planning: {len(df_mart_workforce_planning)} rows")
print(f"mart_hiring_funnel: {len(df_mart_hiring_funnel)} rows")
print(f"mart_retention_engagement: {len(df_mart_retention_engagement)} rows")
print(f"mart_internal_mobility: {len(df_mart_internal_mobility)} rows")
print(f"mart_comp_performance: {len(df_mart_comp_performance)} rows")
print(f"mart_succession_planning: {len(df_mart_succession_planning)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Create Schema & Write Delta Tables

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
    "locations": df_locations,
    "org_units": df_org_units,
    "job_roles": df_job_roles,
    "skills": df_skills,
    "employees": df_employees,
    "employee_skills": df_employee_skills,
    "learning_activity": df_learning_activity,
    "talent_events": df_talent_events,
    "performance_reviews": df_performance_reviews,
    "compensation_snapshots": df_compensation_snapshots,
    "engagement_pulses": df_engagement_pulses,
    "retention_risk_snapshots": df_retention_risk_snapshots,
    "requisitions": df_requisitions,
    "applications": df_applications,
    "succession_plans": df_succession_plans,
    "mart_workforce_planning": df_mart_workforce_planning,
    "mart_hiring_funnel": df_mart_hiring_funnel,
    "mart_retention_engagement": df_mart_retention_engagement,
    "mart_internal_mobility": df_mart_internal_mobility,
    "mart_comp_performance": df_mart_comp_performance,
    "mart_succession_planning": df_mart_succession_planning,
}

print(f"Writing tables to `{CATALOG}`.`{SCHEMA}` with mode={WRITE_MODE}...")
for _table_name, _df in TABLES.items():
    write_table(_df, _table_name)
print("All tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Verification

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
    print(f"  {tbl:<32} {n:>8,}")

print()
print("=" * 60)
print("PATTERN VALIDATION")
print("=" * 60)

print("\n1. 2025 mobility program participants should have lower average retention risk.")
spark.sql(
    f"""
    SELECT mobility_program_participant,
           ROUND(AVG(risk_score), 1) AS avg_risk_score,
           COUNT(DISTINCT employee_id) AS employees
    FROM {C}.`retention_risk_snapshots`
    WHERE snapshot_year = 2025
    GROUP BY mobility_program_participant
    ORDER BY mobility_program_participant DESC
    """
).show()

print("\n2. Sales East engagement should dip in 2024 and show higher regretted exits.")
spark.sql(
    f"""
    WITH engagement AS (
      SELECT e.pulse_year,
             ROUND(AVG(e.engagement_score), 2) AS avg_engagement,
             COUNT(DISTINCT e.employee_id) AS employees
      FROM {C}.`engagement_pulses` e
      JOIN {C}.`org_units` o ON e.org_unit_id = o.org_unit_id
      WHERE o.org_unit_name = 'Sales East'
      GROUP BY e.pulse_year
    ),
    exits AS (
      SELECT t.event_year,
             SUM(CASE WHEN t.is_regretted_loss THEN 1 ELSE 0 END) AS regretted_exits
      FROM {C}.`talent_events` t
      JOIN {C}.`org_units` o ON t.org_unit_id = o.org_unit_id
      WHERE o.org_unit_name = 'Sales East'
        AND t.event_type = 'Exit'
      GROUP BY t.event_year
    )
    SELECT e.pulse_year,
           e.avg_engagement,
           e.employees,
           COALESCE(x.regretted_exits, 0) AS regretted_exits
    FROM engagement e
    LEFT JOIN exits x ON e.pulse_year = x.event_year
    ORDER BY e.pulse_year
    """
).show()

print("\n3. Data & AI roles should show faster growth and longer time to fill.")
spark.sql(
    f"""
    SELECT job_family,
           COUNT(*) AS requisitions,
           ROUND(AVG(time_to_fill_days), 1) AS avg_time_to_fill_days,
           SUM(CASE WHEN requisition_reason = 'Growth' THEN 1 ELSE 0 END) AS growth_reqs
    FROM {C}.`requisitions`
    WHERE status = 'Filled'
    GROUP BY job_family
    ORDER BY avg_time_to_fill_days DESC
    """
).show()

print("\n4. High performers below 0.90 compa-ratio should have higher retention risk.")
spark.sql(
    f"""
    WITH annual_risk AS (
      SELECT employee_id, snapshot_year, MAX(risk_score) AS max_risk_score
      FROM {C}.`retention_risk_snapshots`
      GROUP BY employee_id, snapshot_year
    )
    SELECT CASE
             WHEN p.is_high_performer AND c.compa_ratio < 0.90 THEN 'High performer below 0.90 compa'
             WHEN p.is_high_performer THEN 'High performer at/above 0.90 compa'
             ELSE 'Other'
           END AS segment,
           COUNT(*) AS employee_years,
           ROUND(AVG(r.max_risk_score), 1) AS avg_max_risk_score
    FROM {C}.`compensation_snapshots` c
    JOIN {C}.`performance_reviews` p
      ON c.employee_id = p.employee_id AND c.snapshot_year = p.review_year
    JOIN annual_risk r
      ON c.employee_id = r.employee_id AND c.snapshot_year = r.snapshot_year
    GROUP BY segment
    ORDER BY avg_max_risk_score DESC
    """
).show(truncate=False)

print("\n5. Product offers below market midpoint should have lower acceptance.")
spark.sql(
    f"""
    SELECT CASE WHEN a.offer_to_market_midpoint_pct < 100 THEN 'Below midpoint' ELSE 'At/above midpoint' END AS offer_position,
           COUNT(*) AS offers,
           ROUND(SUM(CASE WHEN a.offer_accepted THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS acceptance_rate_pct
    FROM {C}.`applications` a
    JOIN {C}.`requisitions` r ON a.requisition_id = r.requisition_id
    WHERE r.job_family = 'Product'
      AND a.offer_salary_usd IS NOT NULL
    GROUP BY offer_position
    ORDER BY offer_position
    """
).show()

print("\n6. GTM and Data & AI Platform critical roles should have weaker succession coverage.")
spark.sql(
    f"""
    SELECT CASE
             WHEN org_unit_name = 'Data & AI Platform' THEN 'Data & AI Platform'
             WHEN business_unit = 'Go To Market' THEN 'GTM'
             ELSE 'Other'
           END AS org_group,
           SUM(critical_position_count) AS critical_positions,
           SUM(covered_position_count) AS covered_positions,
           ROUND(SUM(covered_position_count) * 100.0 / SUM(critical_position_count), 1) AS coverage_rate_pct
    FROM {C}.`mart_succession_planning`
    GROUP BY org_group
    ORDER BY coverage_rate_pct
    """
).show()

print()
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print("  Source Tables: locations, org_units, job_roles, skills, employees, employee_skills,")
print("                 learning_activity, talent_events, performance_reviews,")
print("                 compensation_snapshots, engagement_pulses, retention_risk_snapshots,")
print("                 requisitions, applications, succession_plans")
print("  Topic Marts : mart_workforce_planning, mart_hiring_funnel, mart_retention_engagement,")
print("                 mart_internal_mobility, mart_comp_performance, mart_succession_planning")
print("  Next    : Create a Genie Agent from these tables, or see notebooks/demo-data/README.md")
