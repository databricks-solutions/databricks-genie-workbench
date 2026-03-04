# Genie Space Checklist (Organized by Schema)

Based on the [official Databricks Genie best-practices doc](https://docs.databricks.com/aws/en/genie/best-practices) and the [Genie conversation API create/select guidance](https://docs.databricks.com/aws/en/genie/conversation-api#create-or-select-a-genie-space). Last verified on 2026-02-18.

This checklist is organized according to the serialized Genie Space JSON schema structure. All items are evaluated by LLM analysis.

---

## `data_sources`

### `tables`

**Table Selection:**

- [ ] Space scope is focused to only the data needed for intended business questions
- [ ] Aim for 5 or fewer tables/views when possible, while staying within platform limits (currently up to 30 tables/views)
- [ ] Datasets are simplified (prejoined/de-normalized where appropriate, unnecessary columns removed)
- [ ] Tables are well-annotated with clear descriptions

**Column Descriptions:**

- [ ] Columns have descriptions defined
- [ ] Descriptions provide clear, contextual information beyond what column names convey

**Column Synonyms:**

- [ ] Key columns have synonyms defined
- [ ] Synonyms include business terminology, abbreviations, and alternative phrasings users naturally use

**Prompt Matching / Entity Matching:**

- [ ] Filterable columns leverage Genie prompt matching and/or have `enable_format_assistance` configured
- [ ] Columns with discrete values have `enable_entity_matching` enabled

**Column Exclusions:**

- [ ] No duplicative columns exist within the same table
- [ ] Columns not relevant to the space's purpose are hidden

### `metric_views`

- [ ] Metric views have descriptions (if any exist)
- [ ] Pre-computed metrics have comments explaining valid aggregations
- [ ] Metric views are used to simplify the model when raw table count would otherwise grow too large

---

## `instructions`

### `text_instructions`

- [ ] At most 1 text instruction exists (platform limit)
- [ ] Instructions are focused and minimal (avoid excessive/unrelated guidance)
- [ ] Instructions provide globally-applied context only
- [ ] Instructions are specific and actionable (avoid vague wording)
- [ ] Text instructions do not conflict with SQL examples/expressions/snippets
- [ ] Clarification triggers follow structure: condition, missing details, required action, example
- [ ] Clarification guidance appears near the end of general instructions to improve prioritization
- [ ] Summary customization (if used) includes a dedicated heading: `Instructions you must follow when providing summaries`
- [ ] Business jargon is mapped to standard terminology where needed
- [ ] SQL examples, metrics, join logic, and filters are moved to their structured sections (not embedded in text instructions)

### `example_question_sqls`

**Example Questions:**

- [ ] At least 5 tested example question-SQL pairs exist
- [ ] Examples cover complex, multi-part questions with intricate SQL patterns
- [ ] Examples are diverse (not redundant)
- [ ] Examples are validated against anticipated end-user prompts
- [ ] Queries are as short as possible while remaining complete

**Parameters:**

- [ ] Parameters have descriptions defined (if parameters exist)
- [ ] Parameters are used for commonly varied values (dates, names, limits)

**Usage Guidance:**

- [ ] Complex examples have usage guidance describing applicable scenarios and trigger keywords

### `sql_functions`

- [ ] SQL functions are registered and documented in Unity Catalog (if any defined)
- [ ] SQL function entries include valid IDs and fully-qualified function identifiers

### `join_specs`

- [ ] Join specs are defined for multi-table relationships and complex scenarios like self-joins (if applicable)
- [ ] Foreign key references are defined in Unity Catalog when possible
- [ ] Join specs include comments and/or instructions explaining the relationship
- [ ] Join SQL is explicit and uses clear aliases to avoid ambiguous joins

### `sql_snippets`

#### `filters`

- [ ] Common time period filters exist
- [ ] Business-specific filters are defined
- [ ] Filter SQL is non-empty and aligns with business terminology/synonyms

#### `expressions`

- [ ] Reusable expressions are defined for common categorizations and business terms
- [ ] Expressions include synonyms for user terminology
- [ ] Expression SQL is non-empty and reflects standardized business logic

#### `measures`

- [ ] More than 1 measure is defined (consider adding more if only 1 exists)
- [ ] Measures cover standard business concepts used across queries
- [ ] Measure SQL is non-empty and uses clear business-friendly aliases/display names

---

## `benchmarks`

### `questions`

- [ ] At least 10 diverse benchmark Q&A pairs exist, covering different use cases and topics
- [ ] Benchmark questions include varied phrasings to test robustness
- [ ] Each benchmark question has exactly one SQL answer

---

## Summary

| Section | Items |
| --------- | ------- |
| `data_sources.tables` | 12 |
| `data_sources.metric_views` | 3 |
| `instructions.text_instructions` | 10 |
| `instructions.example_question_sqls` | 8 |
| `instructions.sql_functions` | 2 |
| `instructions.join_specs` | 4 |
| `instructions.sql_snippets.filters` | 3 |
| `instructions.sql_snippets.expressions` | 3 |
| `instructions.sql_snippets.measures` | 3 |
| `benchmarks.questions` | 3 |
| **Total** | **51** |

---

## Customization

You can customize this checklist by:

- **Adding items**: Add new `- [ ] Description` lines under any section
- **Removing items**: Delete any checklist item line
- **Modifying items**: Edit the description text of any item

**Note**: The section structure must match the Genie Space schema. Do not add new sections - only modify items within existing sections.
