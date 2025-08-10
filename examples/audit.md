# Layker Audit Process

> **Purpose**: Document how Layker detects changes, writes audit rows, and how to query/operate the audit log.

---

## Overview

Layker is a **YAML-driven table metadata manager** for Databricks/Spark.  
Every time Layker **applies** a change (create or alter), it writes an **audit row** that captures:

- The target **table FQN** (catalog.schema.table)
- **Before** snapshot (JSON) of the table metadata
- **Differences** (JSON) that were applied
- **After** snapshot (JSON) of the table metadata
- **Change category** (create/update) and a readable **change key** sequence
- Context (env, yaml_path, who/when)

No audit row is written when **no differences** are detected.

---

## When does Layker write to the audit table?

1. **YAML is validated** and the live table is snapshotted (if it exists).
2. Layker computes the **differences** between YAML and the live table.
3. If **no change** (the diff contains only `full_table_name`), Layker **exits cleanly** and **skips auditing**.
4. If changes exist and the run mode is `apply` or `all`, Layker:
   - Applies CREATE/ALTER operations
   - Calls **`audit_log_flow(...)`** to append a single row to the audit table

> The audit table is automatically created from the packaged YAML if it does not exist yet.

---

## Audit table schema (default)

Defined in `src/layker/resources/layker_audit.yaml` (packaged by Layker). Columns (in order):

| # | Column           | Type      | Notes |
|---|------------------|-----------|-------|
| 1 | `change_id`      | string    | UUID for each audit row (primary key) |
| 2 | `run_id`         | string    | Optional job or pipeline run identifier |
| 3 | `env`            | string    | Environment/catalog hint (`prd`, `dev`, `test`, `qa`) |
| 4 | `yaml_path`      | string    | Path to the YAML used for this run |
| 5 | `fqn`            | string    | Fully qualified target table (`catalog.schema.table`) |
| 6 | `change_category`| string    | `"create"` if no *before*, otherwise `"update"` |
| 7 | `change_key`     | string    | Human-friendly sequence per table: `create-1`, `create-1~update-3`, … |
| 8 | `before_value`   | string    | JSON snapshot before change (can be null on first create) |
| 9 | `differences`    | string    | JSON diff dictionary that was applied |
|10 | `after_value`    | string    | JSON snapshot after change |
|11 | `notes`          | string    | Optional free text |
|12 | `created_at`     | timestamp | UTC timestamp for row creation |
|13 | `created_by`     | string    | Actor (user/service principal) |
|14 | `updated_at`     | timestamp | Reserved (null for append-only) |
|15 | `updated_by`     | string    | Reserved (null for append-only) |

> **Uniqueness expectation**: `(fqn, change_key)` should be unique over time. Layker computes the sequence to avoid collisions.

---

## Change category & change key

- **Change category**
  - `create` when **no “before” snapshot** is present (table didn’t exist previously)
  - `update` when **before** is present

- **Change key** (per table FQN)
  - First-ever create → `create-1`
  - Next updates on the same lineage → `create-1~update-1`, `create-1~update-2`, ...
  - If the table is later dropped and re-created → new lineage starts at `create-2` (then `create-2~update-1`, etc.)

Layker computes these counts by filtering the audit table **by FQN** and counting prior rows by `change_category`.

---

## End-to-end flow

```
run_table_load(yaml_path, env, mode, audit_log_table, dry_run=False)

1) Validate YAML
   - On failure → exit with clear messages
   - On mode='validate' → exit(0)

2) Snapshot live table (if exists)

3) Compute differences (YAML vs table)
   - If only 'full_table_name' in diff → NO-OP → exit(0), no audit row

4) Validate differences (schema-evolution preflight)
   - Detect add/rename/drop column intents
   - Require Delta props:
       delta.columnMapping.mode = name
       delta.minReaderVersion   = 2
       delta.minWriterVersion   = 5
   - On missing/invalid → print and exit(2)

5) Apply metadata changes
   - CREATE TABLE (from diff) or ALTER clauses

6) Audit (if audit_log_table != False)
   - Ensure audit table exists (create from packaged YAML if needed)
   - Refresh target table (skipped with a warning on serverless)
   - Take AFTER snapshot
   - Write one audit row with: before, differences, after, keys & context
```

---

## Serverless handling

- Databricks **serverless** does not support `REFRESH TABLE`.
- Layker detects this and **skips refresh with a warning**; auditing proceeds.

---

## Failure modes & guardrails

- **Invalid YAML** → fails early with explicit error list.
- **Schema evolution without required Delta properties** → validation error; no changes applied.
- **Spark Connect inference issues** → Layker uses an **explicit Spark schema** when writing the audit row.
- **No changes** → exits early; does **not** write an audit row.

---

## Example usage

**Python**
```python
from pyspark.sql import SparkSession
from layker.main import run_table_load

spark = SparkSession.builder.getOrCreate()

run_table_load(
    yaml_path="src/layker/resources/example.yaml",
    env="prd",
    mode="all",
    dry_run=False,
    audit_log_table=True  # or a custom audit YAML path
)
```

**Direct audit call** (rarely needed; run_table_load calls this for you)
```python
from layker.logger import audit_log_flow

audit_log_flow(
    spark=spark,
    env="prd",
    before_snapshot=before_dict,          # None for first create
    differences=diff_dict,                # the applied diff
    target_table_fq="dq_dev.schema.tbl",
    yaml_path="src/layker/resources/example.yaml",
    audit_table_yaml_path="layker/resources/layker_audit.yaml",
    run_id=None,
    notes=None,
    snapshot_format="json_pretty",
)
```

---

## Query examples

**Count changes per table**
```sql
SELECT fqn, change_category, COUNT(*) AS cnt
FROM dq_dev.layker.layker_audit
GROUP BY fqn, change_category
ORDER BY fqn, change_category;
```

**Latest change per table**
```sql
SELECT fqn, MAX(created_at) AS last_change_at
FROM dq_dev.layker.layker_audit
GROUP BY fqn;
```

**Show lineage for one table**
```sql
SELECT change_key, change_category, created_at
FROM dq_dev.layker.layker_audit
WHERE fqn = 'dq_dev.schema.table_name'
ORDER BY created_at;
```

**Compare before vs after for last update**
```sql
WITH last_row AS (
  SELECT *
  FROM dq_dev.layker.layker_audit
  WHERE fqn = 'dq_dev.schema.table_name'
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  from_json(before_value, 'STRING') AS before_json,
  from_json(after_value,  'STRING') AS after_json,
  differences
FROM last_row;
```

---

## Operations & governance

- **Append-only**: rows are appended; no deletes/updates.
- **Retention**: choose retention based on your compliance posture.
- **Access control**: restrict write access to the service account that runs Layker.
- **Backfilling**: if you recreate the audit table, Layker will resume at the computed lineage for each table FQN.

---

## FAQ (audit-specific)

**Q: Will Layker ever write an audit row on a NO-OP?**  
A: No. If there are no differences to apply, Layker exits before auditing.

**Q: Can I customize the audit table name/location?**  
A: Yes—pass a custom audit YAML path to `audit_log_table` (or `True` to use the default). The **YAML defines** the destination table.

**Q: What if someone drops the target table outside Layker?**  
A: The next run will treat it as `create`; the `change_key` lineage will increment (`create-2`, etc.).

**Q: What format is used for JSON snapshots?**  
A: Default is **pretty JSON**; you can switch to compact or key/value formatting via `snapshot_format`.

---

*Last updated: generated by Layker docs helper.*
