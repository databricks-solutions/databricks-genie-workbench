"""Step 1: Understand the Goal — purpose, title, audience, business context."""

STEP_REQUIREMENTS = """### Step 1: Understand the Goal (2-3 short exchanges)

**1a — Purpose (first message):** Start by asking what they want to build. Keep it light:
> "What kind of space are you looking to build? For example:
> - **Analytics dashboard** — metrics, trends, KPIs
> - **Self-service exploration** — ad-hoc questions on a dataset
> - **Executive reporting** — high-level summaries for leadership
> - Or describe your own use case"

If the user's first message already describes the purpose (e.g., "create a space for NYC taxi analytics"), acknowledge it and skip to 1b.

**1b — Title & audience:** Once you know the purpose, ask:
> "What should we call this space? And who's the main audience — analysts, executives, ops team?"

Suggest a title based on what they described. The user can accept or change it.

**1c — Key questions (optional):** If their purpose was vague, ask:
> "What are the top 2-3 questions this space should answer?"

If they gave a clear purpose, skip this and move to 1d.

**1d — Business context (optional):** Ask if there are any domain-specific rules or conventions you should know:
> "Any business rules or conventions I should keep in mind? For example:
>
> - How your org defines fiscal quarters (e.g. Q1 = Feb-Apr)
> - Default time scope (e.g. always use current year unless specified)
> - Key terminology (e.g. 'revenue' means net revenue after returns)
> - KPI definitions (e.g. 'conversion rate' = orders / visits)
>
> These help me write better instructions and SQL. Feel free to skip if none apply."

Store any business rules the user provides — you will reference them explicitly when generating text instructions, filters, example SQLs, and benchmarks in Step 4. If the user says none or skips, move on immediately.

**DO NOT ask about metrics, filters, dimensions, or technical column details yet.** That comes later after you've seen the data."""

SUMMARY_REQUIREMENTS = "Step 1 (Requirements): Gather purpose, title, audience, and optional business context from the user."
