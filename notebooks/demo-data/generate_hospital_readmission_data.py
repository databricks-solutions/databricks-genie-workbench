# Databricks notebook source

# MAGIC %md
# MAGIC # Hospital Readmission - Synthetic Dataset Generator
# MAGIC
# MAGIC Generates a fictional hospital readmission dataset for Databricks AI/BI Genie demos.
# MAGIC
# MAGIC Patient records use synthetic IDs only. The notebook does not generate patient names, DOBs,
# MAGIC addresses, emails, phone numbers, or real PHI-like identifiers.
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |---|---:|---|
# MAGIC | `patients` | 2,000 | Synthetic patient risk dimension |
# MAGIC | `hospitals` | 12 | Hospital dimension |
# MAGIC | `encounters` | 7,000 | Index admission encounters |
# MAGIC | `care_transitions` | 7,000 | Discharge follow-up and transition actions |
# MAGIC | `claims` | 7,000 | Encounter-level claim facts |
# MAGIC | `readmissions` | variable | 30-day readmission facts |
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
DEFAULT_SCHEMA = "hospital_readmission"  # Schema / database name

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

SERVICE_LINES = ["Medicine", "Cardiology", "Pulmonology", "Surgery", "Orthopedics", "Neurology"]
DIAGNOSIS_GROUPS = ["CHF", "COPD", "Pneumonia", "Sepsis", "Joint Replacement", "Stroke", "Diabetes", "GI"]
PAYER_TYPES = ["Medicare", "Medicaid", "Commercial", "Self Pay"]

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Hospitals

# COMMAND ----------

# =============================================================================
# TABLE 1: HOSPITALS (12 rows)
# =============================================================================
HOSPITAL_SEEDS = [
    ("HOSP-001", "North Valley Medical Center", "Northeast", "NY", 420, "Academic"),
    ("HOSP-002", "Harborview Regional", "Northeast", "MA", 310, "Community"),
    ("HOSP-003", "Piedmont County Hospital", "Southeast", "GA", 260, "Community"),
    ("HOSP-004", "Gulf Coast Medical", "Southeast", "FL", 380, "Teaching"),
    ("HOSP-005", "Lake Plains Hospital", "Midwest", "IL", 340, "Teaching"),
    ("HOSP-006", "Riverbend General", "Midwest", "OH", 220, "Community"),
    ("HOSP-007", "Front Range Health", "West", "CO", 295, "Community"),
    ("HOSP-008", "Pacific Crest Hospital", "West", "CA", 510, "Academic"),
    ("HOSP-009", "Cascade Valley Medical", "West", "WA", 275, "Community"),
    ("HOSP-010", "Desert Springs Hospital", "Southwest", "AZ", 240, "Community"),
    ("HOSP-011", "Lone Star Medical Center", "Southwest", "TX", 460, "Teaching"),
    ("HOSP-012", "Capital Care Hospital", "Northeast", "PA", 330, "Community"),
]

hospitals_data = []
for idx, (hid, name, region, state, beds, hospital_type) in enumerate(HOSPITAL_SEEDS, start=1):
    has_program = idx in (1, 4, 5, 8, 11)
    hospitals_data.append(
        {
            "hospital_id": hid,
            "hospital_name": name,
            "region": region,
            "state": state,
            "bed_count": beds,
            "hospital_type": hospital_type,
            "transition_program_start_date": date(2024, 7, 1) if has_program else None,
            "has_transition_program": bool(has_program),
            "is_active": True,
        }
    )

df_hospitals = pd.DataFrame(hospitals_data)
HOSPITAL_BY_ID = {h["hospital_id"]: h for h in hospitals_data}
HOSPITAL_IDS = [h["hospital_id"] for h in hospitals_data]
print(f"hospitals: {len(df_hospitals)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Patients

# COMMAND ----------

# =============================================================================
# TABLE 2: PATIENTS (2,000 rows)
# =============================================================================
# No patient names, DOBs, addresses, emails, or phone numbers are generated.
patients_data = []
for i in range(1, 2001):
    age_band = random.choices(
        ["18-44", "45-64", "65-74", "75-84", "85+"],
        weights=[14, 27, 25, 22, 12],
    )[0]
    risk_band = random.choices(["Low", "Medium", "High", "Very High"], weights=[28, 38, 24, 10])[0]
    chronic_base = {"Low": 0, "Medium": 1, "High": 2, "Very High": 4}[risk_band]
    patients_data.append(
        {
            "patient_id": f"PT-{i:06d}",
            "age_band": age_band,
            "clinical_risk_band": risk_band,
            "chronic_condition_count": int(chronic_base + random.choice([0, 0, 1, 1, 2])),
            "primary_region": random.choice(["Northeast", "Southeast", "Midwest", "West", "Southwest"]),
            "payer_type": random.choices(PAYER_TYPES, weights=[42, 18, 34, 6])[0],
            "prior_admission_count": random.choices([0, 1, 2, 3, 4], weights=[45, 25, 17, 9, 4])[0],
            "social_risk_index": round(random.uniform(0.05, 0.95), 2),
            "is_active": True,
        }
    )

df_patients = pd.DataFrame(patients_data)
PATIENT_BY_ID = {p["patient_id"]: p for p in patients_data}
PATIENT_IDS = [p["patient_id"] for p in patients_data]
print(f"patients: {len(df_patients)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Encounters

# COMMAND ----------

# =============================================================================
# TABLE 3: ENCOUNTERS (7,000 rows)
# =============================================================================
# Injected patterns:
# 1. CHF and COPD have higher readmission risk.
# 2. Respiratory admissions rise during flu season.
# 3. Weekend discharges have higher readmission risk.
# 4. Transition program hospitals improve after mid-2024.

month_weights = []
for year, month in MONTH_LIST:
    if month in (1, 2, 12):
        month_weights.append(1.30)
    elif month in (7, 8):
        month_weights.append(0.90)
    else:
        month_weights.append(1.0)
encounters_per_month_raw = [w / sum(month_weights) * 7000 for w in month_weights]
encounters_per_month = [int(round(x)) for x in encounters_per_month_raw]
encounters_per_month[-1] += 7000 - sum(encounters_per_month)


def diagnosis_for_month(month):
    if month in (1, 2, 12):
        return random.choices(DIAGNOSIS_GROUPS, weights=[16, 18, 26, 11, 6, 7, 9, 7])[0]
    return random.choices(DIAGNOSIS_GROUPS, weights=[15, 14, 12, 14, 14, 9, 12, 10])[0]


def service_for_diagnosis(diagnosis):
    mapping = {
        "CHF": "Cardiology",
        "COPD": "Pulmonology",
        "Pneumonia": "Pulmonology",
        "Sepsis": "Medicine",
        "Joint Replacement": "Orthopedics",
        "Stroke": "Neurology",
        "Diabetes": "Medicine",
        "GI": "Surgery",
    }
    return mapping[diagnosis]


encounters_data = []
encounter_counter = 1

for month_idx, (year, month) in enumerate(MONTH_LIST):
    n_encounters = encounters_per_month[month_idx]
    max_day = monthrange(year, month)[1]
    for _ in range(n_encounters):
        patient_id = random.choice(PATIENT_IDS)
        hospital_id = random.choice(HOSPITAL_IDS)
        diagnosis = diagnosis_for_month(month)
        service_line = service_for_diagnosis(diagnosis)
        admit_date = date(year, month, random.randint(1, max_day))
        los = random.choices([1, 2, 3, 4, 5, 6, 7, 8, 10, 12], weights=[8, 13, 18, 17, 14, 10, 8, 5, 4, 3])[0]
        if diagnosis in ("CHF", "COPD", "Sepsis", "Stroke"):
            los += random.choice([0, 1, 1, 2, 3])
        discharge_date = min(END_DATE, admit_date + timedelta(days=los))
        weekend_discharge = discharge_date.weekday() >= 5
        encounters_data.append(
            {
                "encounter_id": f"ENC-{encounter_counter:07d}",
                "patient_id": patient_id,
                "hospital_id": hospital_id,
                "admit_date": admit_date,
                "discharge_date": discharge_date,
                "encounter_year": year,
                "encounter_month": month,
                "service_line": service_line,
                "primary_diagnosis_group": diagnosis,
                "acuity_level": random.choices(["Low", "Medium", "High", "Critical"], weights=[15, 46, 30, 9])[0],
                "length_of_stay_days": int(los),
                "discharge_disposition": random.choices(
                    ["Home", "Home Health", "Skilled Nursing", "Rehab", "Expired"],
                    weights=[58, 21, 12, 8, 1],
                )[0],
                "weekend_discharge": bool(weekend_discharge),
            }
        )
        encounter_counter += 1

df_encounters = pd.DataFrame(encounters_data)
ENCOUNTER_BY_ID = {e["encounter_id"]: e for e in encounters_data}
print(f"encounters: {len(df_encounters)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Care Transitions

# COMMAND ----------

# =============================================================================
# TABLE 4: CARE TRANSITIONS (one row per encounter)
# =============================================================================
care_transitions_data = []
transition_counter = 1

for encounter in encounters_data:
    hospital = HOSPITAL_BY_ID[encounter["hospital_id"]]
    patient = PATIENT_BY_ID[encounter["patient_id"]]
    program_active = (
        hospital["has_transition_program"]
        and encounter["discharge_date"] >= hospital["transition_program_start_date"]
    )
    followup_prob = 0.48 + (0.25 if program_active else 0.0)
    if patient["clinical_risk_band"] in ("High", "Very High"):
        followup_prob += 0.08
    follow_up = random.random() < min(0.90, followup_prob)
    med_rec = random.random() < (0.82 if program_active else 0.68)
    home_health = encounter["discharge_disposition"] in ("Home Health", "Skilled Nursing") or random.random() < 0.14
    care_transitions_data.append(
        {
            "transition_id": f"TRN-{transition_counter:07d}",
            "encounter_id": encounter["encounter_id"],
            "patient_id": encounter["patient_id"],
            "hospital_id": encounter["hospital_id"],
            "discharge_date": encounter["discharge_date"],
            "follow_up_within_7_days": bool(follow_up),
            "medication_reconciliation_completed": bool(med_rec),
            "home_health_referral": bool(home_health),
            "transition_program_enrolled": bool(program_active and random.random() < 0.78),
            "outreach_attempts": int(random.choices([0, 1, 2, 3, 4], weights=[12, 38, 29, 15, 6])[0]),
        }
    )
    transition_counter += 1

df_care_transitions = pd.DataFrame(care_transitions_data)
TRANSITION_BY_ENCOUNTER = {t["encounter_id"]: t for t in care_transitions_data}
print(f"care_transitions: {len(df_care_transitions)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Claims

# COMMAND ----------

# =============================================================================
# TABLE 5: CLAIMS (one row per encounter)
# =============================================================================
BASE_COST = {
    "Medicine": 9200,
    "Cardiology": 18500,
    "Pulmonology": 12800,
    "Surgery": 23500,
    "Orthopedics": 27000,
    "Neurology": 21200,
}
PAYER_MULT = {"Medicare": 1.00, "Medicaid": 0.72, "Commercial": 1.28, "Self Pay": 0.58}

claims_data = []
claim_counter = 1
for encounter in encounters_data:
    patient = PATIENT_BY_ID[encounter["patient_id"]]
    payer = patient["payer_type"]
    base = BASE_COST[encounter["service_line"]]
    severity_mult = {"Low": 0.76, "Medium": 1.0, "High": 1.42, "Critical": 2.1}[encounter["acuity_level"]]
    allowed = base * severity_mult * PAYER_MULT[payer] * random.uniform(0.78, 1.35)
    patient_resp = allowed * {"Medicare": 0.08, "Medicaid": 0.02, "Commercial": 0.12, "Self Pay": 0.45}[payer]
    claim_status = random.choices(["Paid", "Denied", "Pending"], weights=[91, 4, 5])[0]
    paid_date = None
    if claim_status == "Paid":
        paid_date = min(END_DATE, encounter["discharge_date"] + timedelta(days=random.randint(20, 90)))
    claims_data.append(
        {
            "claim_id": f"CLM-{claim_counter:07d}",
            "encounter_id": encounter["encounter_id"],
            "patient_id": encounter["patient_id"],
            "hospital_id": encounter["hospital_id"],
            "payer_type": payer,
            "service_line": encounter["service_line"],
            "allowed_amount_usd": round(allowed, 2),
            "patient_responsibility_usd": round(patient_resp, 2),
            "claim_status": claim_status,
            "paid_date": paid_date,
        }
    )
    claim_counter += 1

df_claims = pd.DataFrame(claims_data)
print(f"claims: {len(df_claims)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Readmissions

# COMMAND ----------

# =============================================================================
# TABLE 6: READMISSIONS
# =============================================================================
readmissions_data = []
readmission_counter = 1

for encounter in encounters_data:
    if encounter["discharge_date"] > END_DATE - timedelta(days=30):
        continue
    patient = PATIENT_BY_ID[encounter["patient_id"]]
    transition = TRANSITION_BY_ENCOUNTER[encounter["encounter_id"]]
    hospital = HOSPITAL_BY_ID[encounter["hospital_id"]]
    diagnosis = encounter["primary_diagnosis_group"]
    prob = 0.07
    if diagnosis in ("CHF", "COPD"):
        prob += 0.085
    if diagnosis in ("Pneumonia", "Sepsis"):
        prob += 0.035
    if patient["clinical_risk_band"] == "High":
        prob += 0.035
    if patient["clinical_risk_band"] == "Very High":
        prob += 0.070
    if patient["age_band"] in ("75-84", "85+"):
        prob += 0.035
    if encounter["weekend_discharge"]:
        prob += 0.030
    if not transition["follow_up_within_7_days"]:
        prob += 0.060
    if hospital["has_transition_program"] and encounter["discharge_date"] >= hospital["transition_program_start_date"]:
        prob -= 0.045
    prob = max(0.015, min(0.38, prob))
    if random.random() < prob:
        days_to_readmit = random.randint(2, 30)
        readmit_date = encounter["discharge_date"] + timedelta(days=days_to_readmit)
        readmit_diag = diagnosis if random.random() < 0.66 else diagnosis_for_month(readmit_date.month)
        readmissions_data.append(
            {
                "readmission_id": f"READM-{readmission_counter:07d}",
                "index_encounter_id": encounter["encounter_id"],
                "patient_id": encounter["patient_id"],
                "hospital_id": encounter["hospital_id"],
                "readmission_date": readmit_date,
                "readmission_year": readmit_date.year,
                "readmission_month": readmit_date.month,
                "days_to_readmission": int(days_to_readmit),
                "readmission_diagnosis_group": readmit_diag,
                "planned_readmission": bool(random.random() < 0.08),
                "readmission_cost_usd": round(BASE_COST[service_for_diagnosis(readmit_diag)] * random.uniform(0.75, 1.55), 2),
            }
        )
        readmission_counter += 1

df_readmissions = pd.DataFrame(readmissions_data)
print(f"readmissions: {len(df_readmissions)} rows")

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
    "patients": df_patients,
    "hospitals": df_hospitals,
    "encounters": df_encounters,
    "care_transitions": df_care_transitions,
    "claims": df_claims,
    "readmissions": df_readmissions,
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

print("\n1. CHF/COPD should have higher readmission rates.")
spark.sql(
    f"""
    SELECT CASE WHEN e.primary_diagnosis_group IN ('CHF', 'COPD') THEN 'CHF/COPD' ELSE 'Other' END AS diagnosis_group,
           COUNT(DISTINCT e.encounter_id) AS encounters,
           COUNT(DISTINCT r.readmission_id) AS readmissions,
           ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 1) AS readmission_rate_pct
    FROM {C}.`encounters` e
    LEFT JOIN {C}.`readmissions` r ON e.encounter_id = r.index_encounter_id
    GROUP BY 1
    """
).show()

print("\n2. 7-day follow-up should lower readmission rate.")
spark.sql(
    f"""
    SELECT ct.follow_up_within_7_days,
           COUNT(DISTINCT e.encounter_id) AS encounters,
           COUNT(DISTINCT r.readmission_id) AS readmissions,
           ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 1) AS readmission_rate_pct
    FROM {C}.`encounters` e
    JOIN {C}.`care_transitions` ct ON e.encounter_id = ct.encounter_id
    LEFT JOIN {C}.`readmissions` r ON e.encounter_id = r.index_encounter_id
    GROUP BY ct.follow_up_within_7_days
    """
).show()

print("\n3. Flu season should show more respiratory admissions.")
spark.sql(
    f"""
    SELECT CASE WHEN encounter_month IN (1, 2, 12) THEN 'Flu Season' ELSE 'Other' END AS period,
           COUNT(*) FILTER (WHERE primary_diagnosis_group IN ('COPD', 'Pneumonia')) AS respiratory_encounters,
           COUNT(*) AS total_encounters
    FROM {C}.`encounters`
    GROUP BY 1
    """
).show()

print("\n4. Weekend discharges should have higher readmission risk.")
spark.sql(
    f"""
    SELECT e.weekend_discharge,
           COUNT(DISTINCT e.encounter_id) AS encounters,
           COUNT(DISTINCT r.readmission_id) AS readmissions,
           ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 1) AS readmission_rate_pct
    FROM {C}.`encounters` e
    LEFT JOIN {C}.`readmissions` r ON e.encounter_id = r.index_encounter_id
    GROUP BY e.weekend_discharge
    """
).show()

print("\n5. Program hospitals should improve after transition program launch.")
spark.sql(
    f"""
    SELECT CASE WHEN e.discharge_date >= h.transition_program_start_date THEN 'After Launch' ELSE 'Before Launch' END AS period,
           COUNT(DISTINCT e.encounter_id) AS encounters,
           COUNT(DISTINCT r.readmission_id) AS readmissions,
           ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 1) AS readmission_rate_pct
    FROM {C}.`encounters` e
    JOIN {C}.`hospitals` h ON e.hospital_id = h.hospital_id
    LEFT JOIN {C}.`readmissions` r ON e.encounter_id = r.index_encounter_id
    WHERE h.has_transition_program = TRUE
    GROUP BY 1
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
    "mv_readmission_quality": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.encounters

joins:
  - name: hospital
    source: {CATALOG}.{SCHEMA}.hospitals
    on: source.hospital_id = hospital.hospital_id
  - name: patient
    source: {CATALOG}.{SCHEMA}.patients
    on: source.patient_id = patient.patient_id
  - name: transition
    source: {CATALOG}.{SCHEMA}.care_transitions
    on: source.encounter_id = transition.encounter_id
  - name: readmission
    source: {CATALOG}.{SCHEMA}.readmissions
    on: source.encounter_id = readmission.index_encounter_id

dimensions:
  - name: Discharge Month
    expr: DATE_TRUNC('MONTH', discharge_date)
  - name: Encounter Year
    expr: encounter_year
  - name: Hospital Region
    expr: hospital.region
  - name: Hospital Type
    expr: hospital.hospital_type
  - name: Service Line
    expr: service_line
  - name: Diagnosis Group
    expr: primary_diagnosis_group
  - name: Follow Up Within 7 Days
    expr: transition.follow_up_within_7_days
  - name: Weekend Discharge
    expr: weekend_discharge
  - name: Clinical Risk Band
    expr: patient.clinical_risk_band

measures:
  - name: Encounter Count
    expr: COUNT(DISTINCT source.encounter_id)
  - name: Readmission Count
    expr: COUNT(DISTINCT readmission.readmission_id)
  - name: Readmission Rate Pct
    expr: COUNT(DISTINCT readmission.readmission_id) * 100.0 / COUNT(DISTINCT source.encounter_id)
  - name: Average Length of Stay
    expr: AVG(length_of_stay_days)
  - name: Weekend Discharge Count
    expr: COUNT(1) FILTER (WHERE weekend_discharge = TRUE)
  - name: Follow Up Rate Pct
    expr: COUNT(1) FILTER (WHERE transition.follow_up_within_7_days = TRUE) * 100.0 / COUNT(1)
""",
    "mv_claims_cost": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.claims

joins:
  - name: hospital
    source: {CATALOG}.{SCHEMA}.hospitals
    on: source.hospital_id = hospital.hospital_id
  - name: encounter
    source: {CATALOG}.{SCHEMA}.encounters
    on: source.encounter_id = encounter.encounter_id

dimensions:
  - name: Paid Month
    expr: DATE_TRUNC('MONTH', paid_date)
  - name: Payer Type
    expr: payer_type
  - name: Service Line
    expr: service_line
  - name: Claim Status
    expr: claim_status
  - name: Hospital Region
    expr: hospital.region
  - name: Diagnosis Group
    expr: encounter.primary_diagnosis_group

measures:
  - name: Claim Count
    expr: COUNT(1)
  - name: Allowed Amount
    expr: SUM(allowed_amount_usd)
  - name: Average Allowed Amount
    expr: AVG(allowed_amount_usd)
  - name: Patient Responsibility
    expr: SUM(patient_responsibility_usd)
  - name: Denied Claim Count
    expr: COUNT(1) FILTER (WHERE claim_status = 'Denied')
""",
    "mv_care_transitions": f"""
version: "0.1"
source: {CATALOG}.{SCHEMA}.care_transitions

joins:
  - name: hospital
    source: {CATALOG}.{SCHEMA}.hospitals
    on: source.hospital_id = hospital.hospital_id

dimensions:
  - name: Discharge Month
    expr: DATE_TRUNC('MONTH', discharge_date)
  - name: Hospital Region
    expr: hospital.region
  - name: Transition Program Enrolled
    expr: transition_program_enrolled
  - name: Follow Up Within 7 Days
    expr: follow_up_within_7_days

measures:
  - name: Transition Count
    expr: COUNT(1)
  - name: Follow Up Rate Pct
    expr: COUNT(1) FILTER (WHERE follow_up_within_7_days = TRUE) * 100.0 / COUNT(1)
  - name: Medication Reconciliation Rate Pct
    expr: COUNT(1) FILTER (WHERE medication_reconciliation_completed = TRUE) * 100.0 / COUNT(1)
  - name: Home Health Referral Count
    expr: COUNT(1) FILTER (WHERE home_health_referral = TRUE)
  - name: Average Outreach Attempts
    expr: AVG(outreach_attempts)
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
    "patients": "patient_id",
    "hospitals": "hospital_id",
    "encounters": "encounter_id",
    "care_transitions": "transition_id",
    "claims": "claim_id",
    "readmissions": "readmission_id",
}

FOREIGN_KEYS = [
    ("encounters", "patient_id", "patients", "patient_id"),
    ("encounters", "hospital_id", "hospitals", "hospital_id"),
    ("care_transitions", "encounter_id", "encounters", "encounter_id"),
    ("care_transitions", "patient_id", "patients", "patient_id"),
    ("care_transitions", "hospital_id", "hospitals", "hospital_id"),
    ("claims", "encounter_id", "encounters", "encounter_id"),
    ("claims", "patient_id", "patients", "patient_id"),
    ("claims", "hospital_id", "hospitals", "hospital_id"),
    ("readmissions", "index_encounter_id", "encounters", "encounter_id"),
    ("readmissions", "patient_id", "patients", "patient_id"),
    ("readmissions", "hospital_id", "hospitals", "hospital_id"),
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
    "patients": "Synthetic patient risk dimension using IDs only. Contains no patient names, DOBs, addresses, emails, or phone numbers.",
    "hospitals": "Fictional hospital dimension with region, bed count, hospital type, and transition program flag.",
    "encounters": "Index admission encounters from 2023-01-01 to 2025-12-31 with diagnosis, service line, discharge, and acuity attributes.",
    "care_transitions": "One discharge transition row per encounter, including 7-day follow-up, medication reconciliation, and outreach actions.",
    "claims": "Encounter-level claim facts with payer type, allowed amount, patient responsibility, and claim status.",
    "readmissions": "30-day readmission facts linked to index encounters. CHF, COPD, missed follow-up, and weekend discharge increase risk.",
}

for table, comment in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {C}.`{table}` IS '{sql_text(comment)}'")

for table, df_pd in TABLES.items():
    for column in df_pd.columns:
        readable = column.replace("_", " ")
        spark.sql(
            f"ALTER TABLE {C}.`{table}` ALTER COLUMN `{column}` "
            f"COMMENT '{sql_text(readable)} for the synthetic hospital readmission dataset'"
        )

print("  OK constraints and comments registered")

print()
print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog     : {CATALOG}")
print(f"  Schema      : {SCHEMA}")
print("  Tables      : patients, hospitals, encounters, care_transitions, claims, readmissions")
print("  Metric Views: mv_readmission_quality, mv_claims_cost, mv_care_transitions")
print("  Next    : Create a Genie space from these tables, or see notebooks/demo-data/README.md")
