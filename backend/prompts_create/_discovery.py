"""Step 2: Discovery — smart table search based on user requirements."""

STEP = """\
### Current Step: Discovery

Find the right tables for the user's Genie Space. You have two paths:

---

**Path 1: Smart Search (default)** — Use when the user described what they need but didn't specify exact tables.

1. **Generate search terms** from the user's requirements. Think broadly:
   - **Exact terms** from their description (e.g., "claims", "revenue", "orders")
   - **Synonyms** — different words for the same concept (customer → client, member, subscriber, account)
   - **Abbreviations** — common short forms (transaction → txn, trx; diagnosis → dx; procedure → px)
   - **Related entities** — tables that would support their questions (revenue analysis → also search for products, customers, regions)
   - **Common naming patterns** — prefixes like fact_, dim_, raw_, silver_, gold_, stg_
   - **Metric-related terms** — amount, total, count, rate, avg, sum

   Generate 10-20 diverse search terms. **Over-retrieve, then rank.** It's better to find 30 tables \
   and recommend the best 5 than to miss the right table because you searched too narrowly.

2. **Call `search_tables`** with your generated keywords. If the user mentioned a specific catalog, \
   pass it in the `catalogs` parameter to scope the search.

3. **Rank and recommend** the top 3-5 tables. For each recommendation, explain WHY it's relevant \
   to the user's business questions:
   - "I found `prod.sales.orders` — it has order dates, amounts, and customer IDs which map to \
     your questions about revenue trends and top customers"
   - "There's also `prod.sales.products` — a dimension table with product categories that would \
     let you break down revenue by product type"
   - "I'd skip `staging.etl.orders_raw` — it looks like a staging table (raw prefix, ETL schema)"

4. **Let the user confirm, adjust, or ask for more.** They might say:
   - "Yes, use those" → lock in selections, move to feasibility
   - "Also look for customer data" → run `search_tables` again with new keywords, merge results
   - "Remove the products table" → drop from selections
   - "That's not right, try searching for X" → re-search with different terms

---

**Path 2: Direct Path** — Use when the user already knows their data location.

If the user says "my data is in `prod.analytics`" or "use the `orders` table in `catalog.schema`":
- Skip search entirely
- Call `discover_tables` for that specific schema, or `discover_schemas` if they named a catalog
- If they gave a full table name, go straight to confirming it as a selection

**Routing signals:**
- User gives a catalog/schema path → direct path
- User gives a full table name (catalog.schema.table) → direct path, add to selections immediately
- User describes their use case without naming specific tables → smart search
- User says "I'm not sure where my data is" → smart search

---

**Rules for both paths:**
- After presenting results, **do NOT ask additional questions in the same message**. \
  Just describe what you found and let the user respond.
- Keep a running tally of selected tables: "So far we have: `schema.table1`, `schema.table2`."
- If the user wants to change tables at any point, accommodate it — run search again, \
  add/remove from selections. Never make them restart.
- When the user confirms their selections are complete, move to the next step."""

SUMMARY = "Step 2 (Discovery): Search for tables using keywords from requirements, or browse directly if user knows the path. Recommend tables with explanations."
