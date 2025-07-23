# Layker Audit Logic and Scenario Rules

---

## Purpose

The Layker audit logger captures every structural or metadata modification to tables managed by Layker.  
Modifications include: **table creation**, any updates (add, remove, alter of any property), and table recreation.  
The log is append-only for the lifetime of usage and provides a forensic record for compliance and rollback.

---

## Audit Table Column Reference (all columns)

- **change_id:** Unique ID (UUID or similar) for each change event; primary key for audit table.
- **batch_id:** Identifier for the batch (create or update run) that produced this log row. All rows in a batch share the same batch_id.
- **run_id:** Databricks job run ID, if available (or null if run from notebook/manual).
- **env:** Target environment ("dev", "prd", "test", "qa", etc.)—always lower case.
- **yaml_path:** Full path to the YAML config file that triggered this event.
- **fqn:** Fully qualified name of the target table, constructed as `f"{catalog}.{schema}.{table}"` from the YAML config.
- **change_category:** High-level type: "create" (table creation), "update" (any change), "delete" (not typically used), or "noop" (no change, not logged).
- **change_type:** Fine-grained type of change (e.g. "add_column", "drop_column", "rename_column", "type_change", "update_tag", etc.).
- **subject_type:** Entity type affected by the change (e.g. "column_name", "table_tag", "column_check_constraint", etc.).
- **subject_name:** Name/key of the subject changed (column index, tag key, constraint name, etc.).
- **before_value:** Previous value of the changed item (null if new).
- **after_value:** New value of the changed item (null if removed).
- **notes:** Optional free-text notes for error info, context, or explanation.
- **created_at:** UTC timestamp when this log row was written.
- **created_by:** User/service principal that executed the operation.

---

## Batch, Change ID, FQN, and Uniqueness Rules

- **change_id:** Globally unique per log row; primary key. Used for deduplication and record tracking.
- **batch_id:** All log rows from a single operation (create or update batch) share the same batch_id, grouping changes together.
    - **Format (f-string style):**
        - For creates:       `f"create-{N}_{fqn}"`
        - For updates:       `f"{maxCreate}_update-{M}_{fqn}"`
    - **Where:**
        - **N:** Number of CREATE events already logged for this table, **plus one**.
            - Example:  
                If log rows exist with  
                    batch_id = create-1_{fqn}  
                    batch_id = create-2_{fqn}  
                ...and the table is deleted, then recreated,  
                the next batch_id on CREATE will be  
                    batch_id = create-3_{fqn}
            - N is determined at runtime by querying all batch_ids for the current {fqn} with pattern 'create-*_{fqn}' and taking the highest N, then incrementing by 1.
        - **maxCreate:** The highest N for the current {fqn}. All updates after a given create are grouped under this N.
        - **M:** Number of UPDATE batches already performed since the most recent create, plus one.
            - Example:  
                After batch_id = create-3_{fqn},  
                the first update run would use batch_id = 3_update-1_{fqn},  
                the next would be 3_update-2_{fqn}, etc.
            - When a table is deleted and recreated, N increases, and M resets for the new create version.
- **fqn:** The **fully qualified table name**. Constructed as  
    `f"{catalog}.{schema}.{table}"`
    using values from the YAML config.
- **run_id:** The Databricks job or notebook run ID when the change is detected and logged (should be dynamically captured using Databricks context if possible).
- To reconstruct all changes for a table, batch, or update: filter on batch_id and/or fqn.

---

## Audit Logic (Nested Rule Outline)

1. **On CREATE** (table does not exist in target catalog.schema)
    - Action: Table is created as defined in the YAML.
    - Audit logging:
      - Log a single row:
          - batch_id: `f"create-{N}_{fqn}"`
          - change_category: "create"
          - before_value: JSON array with a single object:
            ```json
            [
              {
                "{fqn}": { full YAML config, all present/nested keys }
              }
            ]
            ```
          - after_value: JSON object of the config, top-level only.
          - Only present keys from YAML included—matches user input structure.
          - No empty/null/unused keys included.

2. **On UPDATE** (table exists and any schema or metadata difference is detected)
    - Action: Each addition, removal, or change is applied as specified by YAML.
    - Audit logging:
      - Log one row **per change**, all from same run share the same batch_id:
          - batch_id: `f"{maxCreate}_update-{M}_{fqn}"`
          - change_category: "update"
          - change_type: ("add_column", "drop_column", "rename_column", "type_change", "update_tag", "update_property", "add_constraint", "drop_constraint", etc.)
          - subject_type: (e.g. "column_name", "table_tag", "column_check_constraint", etc.)
          - subject_name: the column name, tag key, or constraint key being changed
          - before_value: old value (JSON or null)
          - after_value: new value (JSON or null)
      - All rows from the same batch share a batch_id, so you can group a run's changes together.

3. **On NO-OP** (table exists and no difference detected)
    - Action: No log rows are written.
    - Only actual modifications (create or update) result in audit events.

4. **On MULTIPLE CREATES** (table dropped and re-created)
    - Every new create (even after drop) increments N in batch_id ("create-2_", "create-3_", etc).
    - All future updates after a create are grouped under that version.
    - Enables complete tracking of table incarnations.

---

## Example CREATE Log Row

```yaml
- change_id: 1742fbc0-210a-42ca-b0e0-845a1f3bca8e  # (UUID for this event)
  batch_id: create-1_dq_dev.lmg_sandbox.config_driven_table_example
  run_id: 98765432100
  env: dev
  yaml_path: /Workspace/Users/levi.gagne@claconnect.com/ddl/config_example.yaml
  fqn: dq_dev.lmg_sandbox.config_driven_table_example
  change_category: create
  change_type: create_table
  subject_type: table_description
  subject_name: config_driven_table_example
  before_value: |
    [
      {
        "dq_dev.lmg_sandbox.config_driven_table_example": {
          "catalog": "dq_dev",
          "schema": "lmg_sandbox",
          "table": "config_driven_table_example",
          "primary_key": ["example_id"],
          "partitioned_by": ["event_date"],
          "unique_keys": [],
          "table_check_constraints": {
            "constraint_1": {
              "name": "check_event_date_past",
              "expression": "event_date <= current_date()"
            }
          },
          "row_filters": {
            "row_filter_1": {
              "name": "rls_internal_only",
              "expression": "classification = 'internal'"
            }
          },
          "tags": {
            "team": "dq",
            "project": "poc",
            "data_owner": "levi.gagne@claconnect.com",
            "classification": "internal"
          },
          "owner": "levi.gagne@claconnect.com",
          "properties": {
            "comment": "**Table:** dq_dev.lmg_sandbox.config_driven_table_example\n\n**Governance**\n- YAML is the source of truth; changes in UI are not tracked.\n- Required: catalog, schema, table, at least one column with name/datatype.\n- Use partitioned_by for scalability if needed.\n\n**Evolution**\n- Adding columns to the end is supported.\n- Renaming columns (by position/type) is supported.\n- Data type changes or column drops require matching positions.\n- Foreign keys here are not enforced—reference only.\n",
            "table_properties": {
              "delta.columnMapping.mode": "name",
              "delta.minReaderVersion": "2",
              "delta.minWriterVersion": "5"
            }
          },
          "columns": {
            "1": {
              "name": "example_id",
              "datatype": "string",
              "nullable": false,
              "comment": "Primary key: unique row id for this table.",
              "tags": {
                "pii": false,
                "business_key": "yes",
                "sensitive": true
              },
              "column_masking_rule": "MASKED WITH SHA256",
              "default_value": null,
              "variable_value": "special_code_string",
              "allowed_values": ["A", "B", "C"],
              "column_check_constraints": {
                "constraint_1": {
                  "name": "check_example_id_nonempty",
                  "expression": "length(example_id) > 0"
                },
                "constraint_2": {
                  "name": "check_example_id_not_test",
                  "expression": "example_id != 'TEST'"
                }
              },
              "active": true
            },
            "2": {
              "name": "minimal_col",
              "datatype": "string",
              "nullable": true,
              "active": true
            }
            // ...other columns as in the YAML
          }
        }
      }
    ]
  after_value: |
    {
      "catalog": "dq_dev",
      "schema": "lmg_sandbox",
      "table": "config_driven_table_example",
      "primary_key": ["example_id"],
      "partitioned_by": ["event_date"],
      "unique_keys": [],
      "table_check_constraints": {
        "constraint_1": {
          "name": "check_event_date_past",
          "expression": "event_date <= current_date()"
        }
      },
      "row_filters": {
        "row_filter_1": {
          "name": "rls_internal_only",
          "expression": "classification = 'internal'"
        }
      },
      "tags": {
        "team": "dq",
        "project": "poc",
        "data_owner": "levi.gagne@claconnect.com",
        "classification": "internal"
      },
      "owner": "levi.gagne@claconnect.com",
      "properties": {
        "comment": "...",
        "table_properties": {
          "delta.columnMapping.mode": "name",
          "delta.minReaderVersion": "2",
          "delta.minWriterVersion": "5"
        }
      },
      "columns": {
        "1": { ... },
        "2": { ... }
        // ...etc
      }
    }
  notes: "Initial table create"
  created_at: 2024-07-21T21:18:07.102Z
  created_by: "levi.gagne@claconnect.com"
```

---

## Example UPDATE Log Rows (same batch_id, one run)

```yaml
- change_id: 4ae3f1e2-abc2-4a7c-9e27-0807e9572b0a
  batch_id: 1_update-2_dq_dev.lmg_sandbox.config_driven_table_example
  run_id: 98765432111
  env: dev
  yaml_path: /Workspace/Users/levi.gagne@claconnect.com/ddl/config_example.yaml
  fqn: dq_dev.lmg_sandbox.config_driven_table_example
  change_category: update
  change_type: add_column
  subject_type: column_name
  subject_name: "14"
  before_value: null
  after_value: |
    {
      "name": "new_col",
      "datatype": "int",
      "nullable": true,
      "active": true
    }
  notes: "Added a new column as per YAML"
  created_at: 2024-07-21T21:28:05.221Z
  created_by: "levi.gagne@claconnect.com"

- change_id: 41ac92c3-3827-4fd2-b4b2-603aaeffcdbb
  batch_id: 1_update-2_dq_dev.lmg_sandbox.config_driven_table_example
  run_id: 98765432111
  env: dev
  yaml_path: /Workspace/Users/levi.gagne@claconnect.com/ddl/config_example.yaml
  fqn: dq_dev.lmg_sandbox.config_driven_table_example
  change_category: update
  change_type: drop_column
  subject_type: column_name
  subject_name: "3"
  before_value: |
    {
      "name": "column_1",
      "datatype": "string",
      "nullable": true,
      "active": true
    }
  after_value: null
  notes: "Dropped column_1 as per YAML diff"
  created_at: 2024-07-21T21:28:05.221Z
  created_by: "levi.gagne@claconnect.com"
```

---

**END**