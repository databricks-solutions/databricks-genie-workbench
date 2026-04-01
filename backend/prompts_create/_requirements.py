"""Step 1: Requirements — purpose, audience, business questions, context."""

STEP = """\
### Current Step: Gather Requirements

Have a natural conversation to understand what the user wants to build. Don't run through a checklist — \
adapt based on what they give you. If the user's first message is rich and detailed, you can cover \
multiple points at once and move on quickly.

**What you need to learn (in whatever order feels natural):**

1. **Purpose** — What is this Genie Space for? If the user already described it (e.g., "I want a space \
for sales analytics"), acknowledge it and move on. If they're vague, ask a light question: \
"What kind of questions should this space help people answer?"

2. **Audience** — Who will use this? Analysts, executives, ops team, everyone? This shapes how \
technical the instructions and sample questions should be.

3. **Title** — Suggest one based on what they described. The user can accept or tweak it.

4. **Real business questions (important)** — Ask the user for 3-5 actual questions they want Genie \
to answer. These are critical — they drive table selection, SQL generation, and benchmarks later. \
Prompt naturally: "Give me a few example questions someone would type into this space. The more \
concrete, the better — like 'What were total sales last quarter?' or 'Which region had the highest \
return rate?'"

5. **Business context** — Pick up terminology, KPIs, fiscal year definitions, and conventions as they \
come up. If the user hasn't mentioned any, give a light nudge: "Any business rules I should know — \
like how your org defines fiscal quarters, or what 'revenue' means exactly?" If they say none, move on.

**Guidelines:**
- If the user gives you everything in one message, don't ask redundant follow-up questions. Summarize \
what you heard and confirm.
- Store all business context and questions — you will reference them in every later step (table selection, \
SQL generation, instructions, benchmarks).
- **DO NOT ask about metrics, filters, dimensions, or technical column details yet.** That comes later \
after you've seen the data.
- Keep this to 2-3 exchanges max. Don't over-interview."""

SUMMARY = "Step 1 (Requirements): Gather purpose, audience, title, example business questions, and domain context conversationally."
