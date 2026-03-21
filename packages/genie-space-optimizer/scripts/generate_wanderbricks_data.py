"""
Generate synthetic Wanderbricks vacation rental marketplace data
and upload it to main.wanderbricks_gold via SQL warehouse.
"""

import json
import random
import hashlib
import time
import urllib.request
import subprocess
from datetime import date, timedelta, datetime


CATALOG = "main"
SCHEMA = "wanderbricks_gold"
WAREHOUSE_ID = "02c1885622661afa"


def get_auth():
    token_raw = subprocess.check_output(
        ["databricks", "auth", "token", "--profile", "genie-test"],
        stderr=subprocess.DEVNULL,
    )
    token = json.loads(token_raw)["access_token"]
    env_raw = subprocess.check_output(
        ["databricks", "auth", "env", "--profile", "genie-test"],
        stderr=subprocess.DEVNULL,
    )
    host = json.loads(env_raw)["env"]["DATABRICKS_HOST"]
    return host, token


HOST, TOKEN = get_auth()


def run_sql(sql: str, timeout: str = "50s") -> dict:
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": timeout,
    })
    result_raw = subprocess.check_output(
        [
            "curl", "-s", "-X", "POST",
            f"{HOST}/api/2.0/sql/statements/",
            "-H", f"Authorization: Bearer {TOKEN}",
            "-H", "Content-Type: application/json",
            "-d", payload,
        ],
        timeout=120,
    )
    result = json.loads(result_raw)
    state = result.get("status", {}).get("state")
    if state == "FAILED":
        err = result["status"]["error"]["message"]
        raise RuntimeError(f"SQL failed: {err[:500]}")
    if state == "PENDING":
        stmt_id = result["statement_id"]
        for _ in range(60):
            time.sleep(3)
            poll_raw = subprocess.check_output(
                [
                    "curl", "-s",
                    f"{HOST}/api/2.0/sql/statements/{stmt_id}",
                    "-H", f"Authorization: Bearer {TOKEN}",
                ],
                timeout=30,
            )
            result = json.loads(poll_raw)
            state = result.get("status", {}).get("state")
            if state == "SUCCEEDED":
                return result
            if state == "FAILED":
                err = result["status"]["error"]["message"]
                raise RuntimeError(f"SQL failed (poll): {err[:500]}")
        raise RuntimeError("SQL timed out after polling")
    return result


def surr_key(prefix: str, id_val: int) -> str:
    return hashlib.md5(f"{prefix}_{id_val}".encode()).hexdigest()


random.seed(42)

# ── Destinations ──
DESTINATIONS = [
    (1, "Paris", "France", "FR", "Europe", "Île-de-France", "IDF"),
    (2, "Barcelona", "Spain", "ES", "Europe", "Catalonia", "CT"),
    (3, "Tokyo", "Japan", "JP", "Asia", "Tokyo", "TK"),
    (4, "New York", "United States", "US", "North America", "New York", "NY"),
    (5, "Bali", "Indonesia", "ID", "Asia", "Bali", "BA"),
    (6, "London", "United Kingdom", "GB", "Europe", "England", "EN"),
    (7, "Sydney", "Australia", "AU", "Oceania", "New South Wales", "NSW"),
    (8, "Cancún", "Mexico", "MX", "North America", "Quintana Roo", "QR"),
    (9, "Dubai", "United Arab Emirates", "AE", "Asia", "Dubai", "DU"),
    (10, "Cape Town", "South Africa", "ZA", "Africa", "Western Cape", "WC"),
    (11, "Rome", "Italy", "IT", "Europe", "Lazio", "LZ"),
    (12, "Bangkok", "Thailand", "TH", "Asia", "Bangkok", "BK"),
    (13, "Lisbon", "Portugal", "PT", "Europe", "Lisboa", "LI"),
    (14, "Marrakech", "Morocco", "MA", "Africa", "Marrakech-Safi", "MS"),
    (15, "San Francisco", "United States", "US", "North America", "California", "CA"),
]

DEST_DESCRIPTIONS = {
    "Paris": "The City of Light, known for its art, cuisine, and iconic Eiffel Tower.",
    "Barcelona": "Vibrant Mediterranean city with stunning Gaudí architecture.",
    "Tokyo": "A dazzling blend of ultramodern and traditional Japanese culture.",
    "New York": "The city that never sleeps — world-class dining, theatre, and culture.",
    "Bali": "Tropical paradise with lush rice terraces, temples, and surfing.",
    "London": "Historic city with world-class museums, parks, and theatre.",
    "Sydney": "Harbour city famous for its Opera House and beaches.",
    "Cancún": "Caribbean beach resort with turquoise waters and Mayan ruins nearby.",
    "Dubai": "Futuristic skyline, luxury shopping, and desert adventures.",
    "Cape Town": "Where mountains meet the ocean — stunning natural beauty.",
    "Rome": "The Eternal City — ancient ruins, art, and incredible food.",
    "Bangkok": "Bustling city with ornate temples, vibrant street food, and markets.",
    "Lisbon": "Charming hilltop city with pastel buildings and fresh seafood.",
    "Marrakech": "Exotic souks, palaces, and the Atlas Mountains.",
    "San Francisco": "Golden Gate, tech hub, and eclectic neighborhoods.",
}

# ── Amenities ──
AMENITIES = [
    (1, "WiFi", "Essentials", "wifi"),
    (2, "Air Conditioning", "Essentials", "snowflake"),
    (3, "Kitchen", "Essentials", "utensils"),
    (4, "Washer", "Essentials", "shirt"),
    (5, "Pool", "Outdoor", "pool"),
    (6, "Hot Tub", "Outdoor", "hot-tub"),
    (7, "BBQ Grill", "Outdoor", "grill"),
    (8, "Free Parking", "Facilities", "car"),
    (9, "EV Charger", "Facilities", "bolt"),
    (10, "Gym", "Facilities", "dumbbell"),
    (11, "Workspace", "Productivity", "laptop"),
    (12, "TV", "Entertainment", "tv"),
    (13, "Fireplace", "Comfort", "fire"),
    (14, "Balcony", "Outdoor", "sun"),
    (15, "Pet Friendly", "Policies", "paw"),
    (16, "Elevator", "Accessibility", "elevator"),
    (17, "Doorman", "Safety", "shield"),
    (18, "Smoke Alarm", "Safety", "alarm"),
    (19, "First Aid Kit", "Safety", "medkit"),
    (20, "Beach Access", "Outdoor", "umbrella-beach"),
]

# ── Hosts ──
HOST_NAMES = [
    "Maria Garcia", "James Smith", "Yuki Tanaka", "Chen Wei", "Ahmed Hassan",
    "Sophie Dubois", "Luca Rossi", "Ana Silva", "David Brown", "Fatima Al-Rashid",
    "Oliver Wilson", "Priya Sharma", "Hans Müller", "Isabella López", "Ravi Patel",
    "Emma Johnson", "Carlos Mendez", "Mia Anderson", "Kenji Watanabe", "Sarah Davis",
    "Lars Eriksson", "Amara Okafor", "Diego Torres", "Chloe Martin", "Hiroshi Suzuki",
    "Nina Petrov", "Marco Bianchi", "Aaliya Khan", "Thomas Lee", "Eva Novak",
]
HOST_COUNTRIES = [
    "France", "United States", "Japan", "China", "UAE",
    "France", "Italy", "Portugal", "United Kingdom", "UAE",
    "Australia", "India", "Germany", "Spain", "India",
    "United States", "Mexico", "United States", "Japan", "United States",
    "Sweden", "Nigeria", "Spain", "France", "Japan",
    "Russia", "Italy", "Pakistan", "United States", "Czech Republic",
]

# ── Users ──
FIRST_NAMES = ["Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Sam",
               "Quinn", "Avery", "Blake", "Drew", "Skyler", "Jamie", "Charlie",
               "Rowan", "Harper", "Emery", "Finley", "Sage", "Kai"]
LAST_NAMES = ["Kim", "Patel", "Nguyen", "Zhang", "Silva", "García", "Johnson",
              "Müller", "Tanaka", "Wilson", "Anderson", "Brown", "Lee", "Martin",
              "Davis", "López", "Taylor", "Thomas", "Moore", "Jackson"]
USER_COUNTRIES = ["United States", "United Kingdom", "Germany", "France", "Japan",
                  "Brazil", "India", "Canada", "Australia", "Mexico", "Spain",
                  "Italy", "South Korea", "Netherlands", "Sweden"]

PROPERTY_TYPES = ["apartment", "house", "villa", "cabin", "condo", "loft", "cottage", "studio"]
BOOKING_STATUSES = ["confirmed", "cancelled", "pending", "completed"]
PAYMENT_METHODS = ["credit_card", "bank_transfer", "paypal", "crypto", "apple_pay"]
PAYMENT_STATUSES = ["completed", "refunded", "failed", "pending"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
REFERRERS = ["google.com", "facebook.com", "instagram.com", "direct", "tiktok.com",
             "twitter.com", "bing.com", "email", "partner_site"]

NUM_HOSTS = 30
NUM_USERS = 200
NUM_PROPERTIES = 120
NUM_BOOKINGS = 2000
NUM_PAYMENTS = 1800
NUM_REVIEWS = 1500
NUM_PAGEVIEWS = 5000

NOW = datetime(2026, 2, 27, 12, 0, 0)
NOW_TS = NOW.strftime("%Y-%m-%d %H:%M:%S")
DATE_START = date(2024, 1, 1)
DATE_END = date(2026, 2, 27)
DAYS_RANGE = (DATE_END - DATE_START).days


def rand_date(start=DATE_START, end=DATE_END):
    return start + timedelta(days=random.randint(0, (end - start).days))


def esc(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''").replace("\\", "\\\\") + "'"


def batch_insert(table: str, columns: list[str], rows: list[list], batch_size: int = 100):
    fqn = f"{CATALOG}.{SCHEMA}.{table}"
    cols = ", ".join(columns)
    total = len(rows)
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            vals = ", ".join(str(v) if v == "NULL" else v for v in row)
            values.append(f"({vals})")
        sql = f"INSERT INTO {fqn} ({cols}) VALUES\n" + ",\n".join(values)
        run_sql(sql)
        done = min(i + batch_size, total)
        print(f"  {table}: {done}/{total} rows inserted")


# ──────────────────────────────────────────────
# CREATE TABLES
# ──────────────────────────────────────────────
def create_tables():
    fqn = f"{CATALOG}.{SCHEMA}"
    stmts = [
        f"""CREATE TABLE IF NOT EXISTS {fqn}.dim_destination (
            destination_key LONG COMMENT 'Surrogate key for the destination.',
            destination_id LONG COMMENT 'Original destination identifier.',
            destination_name STRING COMMENT 'Name of the travel destination.',
            country STRING COMMENT 'Country where the destination is located.',
            country_code STRING COMMENT 'ISO 2-letter country code.',
            continent STRING COMMENT 'Continent where the destination is located.',
            state_or_province STRING COMMENT 'State, province, or region name.',
            state_or_province_code STRING COMMENT 'State/province abbreviation code.',
            description STRING COMMENT 'Rich description of the destination.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.dim_amenity (
            amenity_key LONG COMMENT 'Surrogate key for the amenity.',
            amenity_id LONG COMMENT 'Original amenity identifier.',
            amenity_name STRING COMMENT 'Display name of the amenity.',
            category STRING COMMENT 'Category grouping for the amenity.',
            icon STRING COMMENT 'Icon identifier for UI display.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.dim_host (
            host_key STRING COMMENT 'Surrogate key uniquely identifying each host version.',
            host_id LONG COMMENT 'Original host identifier.',
            host_name STRING COMMENT 'Full name of the host.',
            email STRING COMMENT 'Host email address.',
            phone STRING COMMENT 'Host phone number.',
            verification_status STRING COMMENT 'Whether the host has been verified.',
            active_status STRING COMMENT 'Whether the host account is active.',
            rating DOUBLE COMMENT 'Host rating score.',
            country STRING COMMENT 'Country of residence for the host.',
            joined_date DATE COMMENT 'Date the host joined the platform.',
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.dim_user (
            user_key STRING COMMENT 'Surrogate key uniquely identifying each user version.',
            user_id LONG COMMENT 'Original user identifier.',
            user_name STRING COMMENT 'Full name of the user.',
            email STRING COMMENT 'User email address.',
            country STRING COMMENT 'Country of residence for the user.',
            user_type STRING COMMENT 'Type of user account.',
            business_status STRING COMMENT 'Whether the user is a business account.',
            company_name STRING COMMENT 'Company name for business accounts.',
            account_created_date DATE COMMENT 'Date the user account was created.',
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.dim_property (
            property_key STRING COMMENT 'Surrogate key uniquely identifying each property version.',
            property_id LONG COMMENT 'Original property identifier.',
            host_key STRING COMMENT 'Surrogate key linking to the host dimension.',
            destination_key LONG COMMENT 'Surrogate key linking to the destination dimension.',
            title STRING COMMENT 'Property listing title.',
            description STRING COMMENT 'Rich description of the property.',
            base_price DOUBLE COMMENT 'Base nightly price for the property.',
            property_type STRING COMMENT 'Type of property.',
            max_guests INT COMMENT 'Maximum number of guests.',
            bedrooms INT COMMENT 'Number of bedrooms.',
            bathrooms INT COMMENT 'Number of bathrooms.',
            property_latitude DOUBLE COMMENT 'Geographic latitude.',
            property_longitude DOUBLE COMMENT 'Geographic longitude.',
            destination_name STRING COMMENT 'Name of the destination (denormalized).',
            destination_country STRING COMMENT 'Country where the property is located (denormalized).',
            host_name STRING COMMENT 'Name of the property host (denormalized).',
            host_rating_status STRING COMMENT 'Host name and rating concatenation.',
            listing_created_date DATE COMMENT 'Date the property listing was created.',
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN,
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.dim_date (
            date_value DATE COMMENT 'Calendar date value.',
            year INT COMMENT 'Calendar year.',
            quarter INT COMMENT 'Calendar quarter (1-4).',
            month INT COMMENT 'Calendar month (1-12).',
            month_name STRING COMMENT 'Full month name.',
            week_of_year INT COMMENT 'ISO week number.',
            day_of_week INT COMMENT 'Day of week (1=Monday, 7=Sunday).',
            day_of_week_name STRING COMMENT 'Full day name.',
            is_weekend BOOLEAN COMMENT 'Whether the date falls on a weekend.',
            is_holiday BOOLEAN COMMENT 'Whether the date is a recognized holiday.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.bridge_property_amenity (
            property_id LONG COMMENT 'Property identifier.',
            amenity_id LONG COMMENT 'Amenity identifier.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.fact_booking (
            booking_key STRING COMMENT 'Surrogate key uniquely identifying each booking.',
            booking_id LONG COMMENT 'Original booking identifier.',
            user_key STRING COMMENT 'Surrogate key linking to dim_user.',
            property_key STRING COMMENT 'Surrogate key linking to dim_property.',
            check_in_date DATE COMMENT 'Date of check-in.',
            check_out_date DATE COMMENT 'Date of check-out.',
            booking_created_date DATE COMMENT 'Date the booking was created.',
            total_amount DOUBLE COMMENT 'Total booking revenue amount in USD.',
            guests_count INT COMMENT 'Number of guests.',
            nights_count INT COMMENT 'Number of nights.',
            booking_status STRING COMMENT 'Booking lifecycle status.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.fact_payment (
            payment_key STRING COMMENT 'Surrogate key uniquely identifying each payment.',
            payment_id LONG COMMENT 'Original payment identifier.',
            booking_id LONG COMMENT 'Booking identifier.',
            user_key STRING COMMENT 'Surrogate key linking to dim_user.',
            property_key STRING COMMENT 'Surrogate key linking to dim_property.',
            payment_date DATE COMMENT 'Date of the payment.',
            amount DOUBLE COMMENT 'Payment amount in USD.',
            payment_method STRING COMMENT 'Payment method used.',
            payment_status STRING COMMENT 'Payment status.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.fact_review (
            review_key STRING COMMENT 'Surrogate key uniquely identifying each review.',
            review_id LONG COMMENT 'Original review identifier.',
            user_key STRING COMMENT 'Surrogate key linking to dim_user.',
            property_key STRING COMMENT 'Surrogate key linking to dim_property.',
            booking_id LONG COMMENT 'Booking identifier.',
            review_date DATE COMMENT 'Date the review was submitted.',
            rating DOUBLE COMMENT 'Review rating (1-5).',
            review_comment STRING COMMENT 'Free-text review comment.',
            deletion_status STRING COMMENT 'Whether the review is active or deleted.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.fact_pageview (
            pageview_key STRING COMMENT 'Surrogate key uniquely identifying each page view.',
            view_id LONG COMMENT 'Original page view identifier.',
            user_key STRING COMMENT 'Surrogate key linking to dim_user.',
            property_key STRING COMMENT 'Surrogate key linking to dim_property.',
            view_date DATE COMMENT 'Date of the page view.',
            device_type STRING COMMENT 'Device type used.',
            referrer STRING COMMENT 'Referrer URL or source.',
            page_url STRING COMMENT 'URL of the viewed page.',
            record_created_timestamp TIMESTAMP,
            record_updated_timestamp TIMESTAMP
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.booking_analytics_metrics (
            check_in_date DATE COMMENT 'Booking check-in date — PRIMARY temporal dimension.',
            booking_created_date DATE COMMENT 'Date the booking was created.',
            destination_name STRING COMMENT 'Travel destination name.',
            destination_country STRING COMMENT 'Destination country.',
            property_type STRING COMMENT 'Property type.',
            booking_status STRING COMMENT 'Booking status.',
            business_status STRING COMMENT 'Guest business status.',
            total_revenue DOUBLE COMMENT 'Total booking revenue (confirmed bookings only).',
            booking_count LONG COMMENT 'Number of bookings.',
            avg_booking_value DOUBLE COMMENT 'Average value per booking.',
            total_nights LONG COMMENT 'Total nights sold.',
            avg_nights DOUBLE COMMENT 'Average length of stay in nights.',
            avg_guests DOUBLE COMMENT 'Average guests per booking.'
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.pageview_analytics_metrics (
            view_date DATE COMMENT 'Date of the page view.',
            device_type STRING COMMENT 'Device type used.',
            referrer STRING COMMENT 'Traffic source / referrer URL.',
            destination_name STRING COMMENT 'Destination of the viewed property.',
            view_count LONG COMMENT 'Total number of page views.',
            unique_viewers LONG COMMENT 'Number of unique viewers.',
            unique_properties_viewed LONG COMMENT 'Number of distinct properties viewed.'
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.payment_analytics_metrics (
            payment_date DATE COMMENT 'Date of payment transaction — PRIMARY temporal dimension.',
            payment_method STRING COMMENT 'Payment method.',
            payment_status STRING COMMENT 'Payment status.',
            destination_name STRING COMMENT 'Destination for geographic revenue analysis.',
            total_payment_amount DOUBLE COMMENT 'Total payment amount processed.',
            payment_count LONG COMMENT 'Number of payment transactions.',
            avg_payment DOUBLE COMMENT 'Average payment amount.',
            refund_amount DOUBLE COMMENT 'Total refund amount.',
            completion_rate DECIMAL(5,2) COMMENT 'Percentage of payments completed.'
        ) USING DELTA""",

        f"""CREATE TABLE IF NOT EXISTS {fqn}.review_analytics_metrics (
            review_date DATE COMMENT 'Date the review was submitted.',
            destination_name STRING COMMENT 'Destination of the reviewed property.',
            property_type STRING COMMENT 'Type of reviewed property.',
            deletion_status STRING COMMENT 'Whether review is active or deleted.',
            avg_rating DOUBLE COMMENT 'Average review rating (1-5 scale).',
            review_count LONG COMMENT 'Total number of reviews.',
            active_review_count LONG COMMENT 'Number of active reviews.',
            five_star_rate DECIMAL(5,2) COMMENT 'Percentage of 5-star reviews.'
        ) USING DELTA""",
    ]

    for stmt in stmts:
        tbl_name = stmt.split(f"{fqn}.")[1].split("(")[0].strip()
        print(f"Creating table {tbl_name}...")
        run_sql(stmt)
    print("All tables created.\n")


# ──────────────────────────────────────────────
# GENERATE AND INSERT DATA
# ──────────────────────────────────────────────

def gen_dim_destination():
    print("Generating dim_destination...")
    rows = []
    for d in DESTINATIONS:
        did, name, country, cc, cont, state, sc = d
        desc = DEST_DESCRIPTIONS.get(name, f"Beautiful destination: {name}")
        rows.append([
            str(did), str(did), esc(name), esc(country), esc(cc), esc(cont),
            esc(state), esc(sc), esc(desc), esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("dim_destination",
        ["destination_key", "destination_id", "destination_name", "country",
         "country_code", "continent", "state_or_province", "state_or_province_code",
         "description", "record_created_timestamp", "record_updated_timestamp"],
        rows)


def gen_dim_amenity():
    print("Generating dim_amenity...")
    rows = []
    for a in AMENITIES:
        aid, name, cat, icon = a
        rows.append([
            str(aid), str(aid), esc(name), esc(cat), esc(icon), esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("dim_amenity",
        ["amenity_key", "amenity_id", "amenity_name", "category", "icon",
         "record_created_timestamp", "record_updated_timestamp"],
        rows)


def gen_dim_date():
    print("Generating dim_date...")
    MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    rows = []
    d = DATE_START
    while d <= DATE_END:
        iso_cal = d.isocalendar()
        dow = iso_cal[2]
        rows.append([
            esc(str(d)), str(d.year), str((d.month - 1) // 3 + 1), str(d.month),
            esc(MONTH_NAMES[d.month - 1]), str(iso_cal[1]), str(dow),
            esc(DAY_NAMES[dow - 1]),
            "true" if dow >= 6 else "false",
            "false",
            esc(NOW_TS), esc(NOW_TS),
        ])
        d += timedelta(days=1)
    batch_insert("dim_date",
        ["date_value", "year", "quarter", "month", "month_name", "week_of_year",
         "day_of_week", "day_of_week_name", "is_weekend", "is_holiday",
         "record_created_timestamp", "record_updated_timestamp"],
        rows, batch_size=200)


def gen_dim_host():
    print("Generating dim_host...")
    rows = []
    for i in range(NUM_HOSTS):
        hid = i + 1
        hkey = surr_key("host", hid)
        name = HOST_NAMES[i]
        email_name = name.lower().replace(" ", ".") + "@wanderbricks.com"
        phone = f"+1-555-{random.randint(100,999)}-{random.randint(1000,9999)}"
        verified = random.choice(["verified", "unverified"])
        active = random.choices(["active", "inactive"], weights=[90, 10])[0]
        rating = round(random.uniform(3.5, 5.0), 2)
        country = HOST_COUNTRIES[i]
        joined = rand_date(date(2020, 1, 1), date(2025, 6, 1))
        rows.append([
            esc(hkey), str(hid), esc(name), esc(email_name), esc(phone),
            esc(verified), esc(active), str(rating), esc(country),
            esc(str(joined)), esc(NOW_TS), "NULL", "true", esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("dim_host",
        ["host_key", "host_id", "host_name", "email", "phone",
         "verification_status", "active_status", "rating", "country",
         "joined_date", "effective_from", "effective_to", "is_current",
         "record_created_timestamp", "record_updated_timestamp"],
        rows)
    return rows


def gen_dim_user():
    print("Generating dim_user...")
    rows = []
    for i in range(NUM_USERS):
        uid = i + 1
        ukey = surr_key("user", uid)
        fname = FIRST_NAMES[i % len(FIRST_NAMES)]
        lname = LAST_NAMES[i % len(LAST_NAMES)]
        name = f"{fname} {lname}"
        if i >= len(FIRST_NAMES):
            name += f" {uid}"
        email = f"{fname.lower()}.{lname.lower()}{uid}@example.com"
        country = random.choice(USER_COUNTRIES)
        utype = random.choices(["guest", "host", "both"], weights=[70, 20, 10])[0]
        is_biz = random.random() < 0.15
        biz_status = "Business Account" if is_biz else "Personal Account"
        company = esc(f"{lname} Corp") if is_biz else "NULL"
        created = rand_date(date(2020, 1, 1), date(2025, 12, 31))
        rows.append([
            esc(ukey), str(uid), esc(name), esc(email), esc(country),
            esc(utype), esc(biz_status), company, esc(str(created)),
            esc(NOW_TS), "NULL", "true", esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("dim_user",
        ["user_key", "user_id", "user_name", "email", "country",
         "user_type", "business_status", "company_name", "account_created_date",
         "effective_from", "effective_to", "is_current",
         "record_created_timestamp", "record_updated_timestamp"],
        rows)
    return rows


def gen_dim_property(host_rows):
    print("Generating dim_property...")
    rows = []
    property_meta = []
    for i in range(NUM_PROPERTIES):
        pid = i + 1
        pkey = surr_key("property", pid)
        host_idx = i % NUM_HOSTS
        hkey = surr_key("host", host_idx + 1)
        dest = random.choice(DESTINATIONS)
        dest_key, dest_name, dest_country = dest[0], dest[1], dest[2]
        host_name = HOST_NAMES[host_idx]
        host_rating = round(random.uniform(3.5, 5.0), 2)
        ptype = random.choice(PROPERTY_TYPES)
        base_price = round(random.uniform(50, 800), 2)
        max_guests = random.randint(1, 10)
        bedrooms = random.randint(1, 5)
        bathrooms = random.randint(1, 3)
        lat = round(random.uniform(-40, 60), 6)
        lon = round(random.uniform(-120, 150), 6)
        title = f"{ptype.title()} in {dest_name} #{pid}"
        desc = f"Beautiful {ptype} located in the heart of {dest_name}. Perfect for travelers."
        listing_date = rand_date(date(2021, 1, 1), date(2025, 6, 1))
        host_rs = f"{host_name} ({host_rating})"

        rows.append([
            esc(pkey), str(pid), esc(hkey), str(dest_key),
            esc(title), esc(desc), str(base_price), esc(ptype),
            str(max_guests), str(bedrooms), str(bathrooms),
            str(lat), str(lon), esc(dest_name), esc(dest_country),
            esc(host_name), esc(host_rs), esc(str(listing_date)),
            esc(NOW_TS), "NULL", "true", esc(NOW_TS), esc(NOW_TS),
        ])
        property_meta.append({
            "pid": pid, "pkey": pkey, "dest_name": dest_name,
            "dest_country": dest_country, "ptype": ptype,
            "base_price": base_price, "max_guests": max_guests,
        })

    batch_insert("dim_property",
        ["property_key", "property_id", "host_key", "destination_key",
         "title", "description", "base_price", "property_type",
         "max_guests", "bedrooms", "bathrooms", "property_latitude",
         "property_longitude", "destination_name", "destination_country",
         "host_name", "host_rating_status", "listing_created_date",
         "effective_from", "effective_to", "is_current",
         "record_created_timestamp", "record_updated_timestamp"],
        rows)
    return property_meta


def gen_bridge_property_amenity():
    print("Generating bridge_property_amenity...")
    rows = []
    for pid in range(1, NUM_PROPERTIES + 1):
        num_amenities = random.randint(3, 12)
        amenity_ids = random.sample(range(1, len(AMENITIES) + 1), num_amenities)
        for aid in amenity_ids:
            rows.append([str(pid), str(aid), esc(NOW_TS), esc(NOW_TS)])
    batch_insert("bridge_property_amenity",
        ["property_id", "amenity_id", "record_created_timestamp", "record_updated_timestamp"],
        rows, batch_size=200)


def gen_fact_booking(property_meta, user_rows):
    print("Generating fact_booking...")
    rows = []
    booking_meta = []
    for i in range(NUM_BOOKINGS):
        bid = i + 1
        bkey = surr_key("booking", bid)
        user_idx = random.randint(0, NUM_USERS - 1)
        ukey = surr_key("user", user_idx + 1)
        prop = random.choice(property_meta)
        pkey = prop["pkey"]
        created = rand_date()
        checkin = created + timedelta(days=random.randint(1, 90))
        nights = random.randint(1, 14)
        checkout = checkin + timedelta(days=nights)
        guests = random.randint(1, min(prop["max_guests"], 6))
        amount = round(prop["base_price"] * nights * (1 + random.uniform(-0.2, 0.3)), 2)
        status = random.choices(BOOKING_STATUSES, weights=[50, 15, 10, 25])[0]

        rows.append([
            esc(bkey), str(bid), esc(ukey), esc(pkey),
            esc(str(checkin)), esc(str(checkout)), esc(str(created)),
            str(amount), str(guests), str(nights), esc(status),
            esc(NOW_TS), esc(NOW_TS),
        ])
        booking_meta.append({
            "bid": bid, "ukey": ukey, "pkey": pkey, "amount": amount,
            "created": created, "dest_name": prop["dest_name"],
            "dest_country": prop["dest_country"], "ptype": prop["ptype"],
            "status": status, "nights": nights, "guests": guests,
            "checkin": checkin, "business_status": "Personal Account",
        })

    batch_insert("fact_booking",
        ["booking_key", "booking_id", "user_key", "property_key",
         "check_in_date", "check_out_date", "booking_created_date",
         "total_amount", "guests_count", "nights_count", "booking_status",
         "record_created_timestamp", "record_updated_timestamp"],
        rows)
    return booking_meta


def gen_fact_payment(booking_meta, property_meta):
    print("Generating fact_payment...")
    rows = []
    sampled = random.sample(booking_meta, min(NUM_PAYMENTS, len(booking_meta)))
    for i, bk in enumerate(sampled):
        payid = i + 1
        paykey = surr_key("payment", payid)
        pay_date = bk["created"] + timedelta(days=random.randint(0, 3))
        method = random.choice(PAYMENT_METHODS)
        status = random.choices(PAYMENT_STATUSES, weights=[75, 10, 5, 10])[0]
        amount = bk["amount"] if status != "refunded" else -bk["amount"]

        rows.append([
            esc(paykey), str(payid), str(bk["bid"]), esc(bk["ukey"]), esc(bk["pkey"]),
            esc(str(pay_date)), str(round(amount, 2)), esc(method), esc(status),
            esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("fact_payment",
        ["payment_key", "payment_id", "booking_id", "user_key", "property_key",
         "payment_date", "amount", "payment_method", "payment_status",
         "record_created_timestamp", "record_updated_timestamp"],
        rows)


def gen_fact_review(booking_meta, property_meta):
    print("Generating fact_review...")
    rows = []
    completed = [b for b in booking_meta if b["status"] == "completed"]
    sampled = random.sample(completed, min(NUM_REVIEWS, len(completed)))
    if len(sampled) < NUM_REVIEWS:
        sampled += random.choices(completed, k=NUM_REVIEWS - len(sampled))
    comments = [
        "Amazing stay! Highly recommended.",
        "Great location but could be cleaner.",
        "Perfect for families. Kids loved the pool.",
        "Wonderful host, very responsive.",
        "Beautiful views but noisy neighborhood.",
        "Exactly as described. Would book again.",
        "A bit pricey for what you get.",
        "Cozy and comfortable. Felt like home.",
        "The kitchen was well-equipped.",
        "Great value for money!",
        "Could use some updates but overall good.",
        "Fantastic experience from start to finish.",
    ]
    for i, bk in enumerate(sampled):
        rid = i + 1
        rkey = surr_key("review", rid)
        rev_date = bk["created"] + timedelta(days=random.randint(3, 30))
        rating = round(random.choices(
            [1.0, 2.0, 3.0, 4.0, 5.0], weights=[3, 5, 12, 35, 45]
        )[0], 1)
        comment = random.choice(comments)
        deletion = random.choices(["active", "deleted"], weights=[95, 5])[0]

        rows.append([
            esc(rkey), str(rid), esc(bk["ukey"]), esc(bk["pkey"]),
            str(bk["bid"]), esc(str(rev_date)), str(rating),
            esc(comment), esc(deletion), esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("fact_review",
        ["review_key", "review_id", "user_key", "property_key",
         "booking_id", "review_date", "rating", "review_comment",
         "deletion_status", "record_created_timestamp", "record_updated_timestamp"],
        rows)


def gen_fact_pageview(property_meta):
    print("Generating fact_pageview...")
    rows = []
    for i in range(NUM_PAGEVIEWS):
        vid = i + 1
        vkey = surr_key("pageview", vid)
        user_idx = random.randint(0, NUM_USERS - 1)
        ukey = surr_key("user", user_idx + 1)
        prop = random.choice(property_meta)
        pkey = prop["pkey"]
        view_date = rand_date()
        device = random.choices(DEVICE_TYPES, weights=[45, 40, 15])[0]
        referrer = random.choice(REFERRERS)
        page_url = f"https://wanderbricks.com/property/{prop['pid']}"

        rows.append([
            esc(vkey), str(vid), esc(ukey), esc(pkey),
            esc(str(view_date)), esc(device), esc(referrer), esc(page_url),
            esc(NOW_TS), esc(NOW_TS),
        ])
    batch_insert("fact_pageview",
        ["pageview_key", "view_id", "user_key", "property_key",
         "view_date", "device_type", "referrer", "page_url",
         "record_created_timestamp", "record_updated_timestamp"],
        rows, batch_size=200)


def gen_booking_analytics(booking_meta):
    print("Generating booking_analytics_metrics (aggregated)...")
    fqn = f"{CATALOG}.{SCHEMA}"
    sql = f"""
    INSERT INTO {fqn}.booking_analytics_metrics
    SELECT
        b.check_in_date,
        b.booking_created_date,
        p.destination_name,
        p.destination_country,
        p.property_type,
        b.booking_status,
        u.business_status,
        SUM(CASE WHEN b.booking_status = 'confirmed' OR b.booking_status = 'completed' THEN b.total_amount ELSE 0 END) as total_revenue,
        COUNT(*) as booking_count,
        AVG(b.total_amount) as avg_booking_value,
        SUM(b.nights_count) as total_nights,
        AVG(b.nights_count) as avg_nights,
        AVG(b.guests_count) as avg_guests
    FROM {fqn}.fact_booking b
    JOIN {fqn}.dim_property p ON b.property_key = p.property_key
    JOIN {fqn}.dim_user u ON b.user_key = u.user_key
    GROUP BY 1,2,3,4,5,6,7
    """
    run_sql(sql)
    print("  booking_analytics_metrics: aggregated from fact_booking")


def gen_pageview_analytics():
    print("Generating pageview_analytics_metrics (aggregated)...")
    fqn = f"{CATALOG}.{SCHEMA}"
    sql = f"""
    INSERT INTO {fqn}.pageview_analytics_metrics
    SELECT
        pv.view_date,
        pv.device_type,
        pv.referrer,
        p.destination_name,
        COUNT(*) as view_count,
        COUNT(DISTINCT pv.user_key) as unique_viewers,
        COUNT(DISTINCT pv.property_key) as unique_properties_viewed
    FROM {fqn}.fact_pageview pv
    JOIN {fqn}.dim_property p ON pv.property_key = p.property_key
    GROUP BY 1,2,3,4
    """
    run_sql(sql)
    print("  pageview_analytics_metrics: aggregated from fact_pageview")


def gen_payment_analytics():
    print("Generating payment_analytics_metrics (aggregated)...")
    fqn = f"{CATALOG}.{SCHEMA}"
    sql = f"""
    INSERT INTO {fqn}.payment_analytics_metrics
    SELECT
        fp.payment_date,
        fp.payment_method,
        fp.payment_status,
        p.destination_name,
        SUM(fp.amount) as total_payment_amount,
        COUNT(*) as payment_count,
        AVG(fp.amount) as avg_payment,
        SUM(CASE WHEN fp.payment_status = 'refunded' THEN ABS(fp.amount) ELSE 0 END) as refund_amount,
        CAST(SUM(CASE WHEN fp.payment_status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as completion_rate
    FROM {fqn}.fact_payment fp
    JOIN {fqn}.dim_property p ON fp.property_key = p.property_key
    GROUP BY 1,2,3,4
    """
    run_sql(sql)
    print("  payment_analytics_metrics: aggregated from fact_payment")


def gen_review_analytics():
    print("Generating review_analytics_metrics (aggregated)...")
    fqn = f"{CATALOG}.{SCHEMA}"
    sql = f"""
    INSERT INTO {fqn}.review_analytics_metrics
    SELECT
        fr.review_date,
        p.destination_name,
        p.property_type,
        fr.deletion_status,
        AVG(fr.rating) as avg_rating,
        COUNT(*) as review_count,
        SUM(CASE WHEN fr.deletion_status = 'active' THEN 1 ELSE 0 END) as active_review_count,
        CAST(SUM(CASE WHEN fr.rating = 5.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as five_star_rate
    FROM {fqn}.fact_review fr
    JOIN {fqn}.dim_property p ON fr.property_key = p.property_key
    GROUP BY 1,2,3,4
    """
    run_sql(sql)
    print("  review_analytics_metrics: aggregated from fact_review")


def main():
    print(f"=== Generating Wanderbricks data in {CATALOG}.{SCHEMA} ===\n")
    start = time.time()

    create_tables()

    gen_dim_destination()
    gen_dim_amenity()
    gen_dim_date()
    host_rows = gen_dim_host()
    user_rows = gen_dim_user()
    property_meta = gen_dim_property(host_rows)
    gen_bridge_property_amenity()
    booking_meta = gen_fact_booking(property_meta, user_rows)
    gen_fact_payment(booking_meta, property_meta)
    gen_fact_review(booking_meta, property_meta)
    gen_fact_pageview(property_meta)

    gen_booking_analytics(booking_meta)
    gen_pageview_analytics()
    gen_payment_analytics()
    gen_review_analytics()

    elapsed = time.time() - start
    print(f"\n=== Done! Elapsed: {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()
