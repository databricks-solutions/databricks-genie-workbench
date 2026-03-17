---
name: Fix Schema Overwrite
overview: Add a fallback to `ensure_labeling_schemas` so that when `overwrite=True` fails because a labeling session references the schema, we verify the schema exists via `get_label_schema()` and reuse it.
todos:
  - id: fix-schema-fallback
    content: Add get_label_schema() fallback in ensure_labeling_schemas except block (labeling.py lines 106-113)
    status: completed
isProject: false
---

# Fix Schema Creation When Referenced by Existing Sessions

## Problem

`create_label_schema(..., overwrite=True)` fails with `BAD_REQUEST: Cannot rename or remove labeling schemas that are referenced by existing labeling sessions` when any labeling session (e.g. the manually-created "test" session) holds a reference to the schema. This causes all 4 schemas to fail, returning `Ensured 0/4 schemas`, which blocks the entire labeling pipeline.

## Fix

Single change in [labeling.py](src/genie_space_optimizer/optimization/labeling.py), lines 106-113. Add a fallback inside the `except` block: when `overwrite=True` fails, call `schemas.get_label_schema(name)` to check if the schema already exists. If it does, add it to `available` (the schema is usable as-is). Only report a real failure if the schema neither created nor exists.

```python
for defn in _schema_defs:
    name = defn["name"]
    try:
        schemas.create_label_schema(**defn, overwrite=True)
        available.append(name)
    except Exception as exc:
        try:
            existing = schemas.get_label_schema(name)
            if existing is not None:
                available.append(name)
                print(f"[Labeling] Schema '{name}' exists (referenced by session) — reusing")
            else:
                print(f"[Labeling] Failed to create schema '{name}': {exc}")
                logger.warning("Failed to create label schema '%s': %s", name, exc)
        except Exception:
            print(f"[Labeling] Failed to create schema '{name}': {exc}")
            logger.warning("Failed to create label schema '%s': %s", name, exc)
```

- Happy path (no existing session): `overwrite=True` succeeds directly
- Existing session references schema: `overwrite=True` fails, `get_label_schema` confirms it exists, reuse it
- Schema genuinely missing and creation fails: both paths fail, error is printed

