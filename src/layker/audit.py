# src/layker/audit.py

import uuid, json, yaml, hashlib
from datetime import datetime
from pyspark.sql import Row

class TableAuditLogger:
    """
    
    ===============================
    LAYKER AUDIT LOGIC AND SCENARIO RULES
    ===============================
    
    Purpose:
      The Layker audit logger captures every structural or metadata modification to tables managed by Layker.
      Modifications include: table creation, any updates (add, remove, alter of any property), and table recreation.
      The log is append-only for the lifetime of usage and provides a forensic record for compliance and rollback.
    
    -----------------------------------------
    Audit Table Column Reference (all columns)
    -----------------------------------------
    - change_id:       Unique ID (UUID or similar) for each change event; primary key for audit table.
    - batch_id:        Identifier for the batch (create or update run) that produced this log row. All rows in a batch share the same batch_id.
    - run_id:          Databricks job run ID, if available (or null if run from notebook/manual).
    - env:             Target environment ("dev", "prd", "test", "qa", etc.)—always lower case.
    - yaml_path:       Full path to the YAML config file that triggered this event.
    - fqn:             Fully qualified name of the target table, constructed as f"{catalog}.{schema}.{table}" from the YAML config.
    - change_category: High-level type: "create" (table creation), "update" (any change), "delete" (not typically used), or "noop" (no change, not logged).
    - change_type:     Fine-grained type of change (e.g. "add_column", "drop_column", "rename_column", "type_change", "update_tag", etc.).
    - subject_type:    Entity type affected by the change (e.g. "column_name", "table_tag", "column_check_constraint", etc.).
    - subject_name:    Name/key of the subject changed (column index, tag key, constraint name, etc.).
    - before_value:    Previous value of the changed item (null if new).
    - after_value:     New value of the changed item (null if removed).
    - notes:           Optional free-text notes for error info, context, or explanation.
    - created_at:      UTC timestamp when this log row was written.
    - created_by:      User/service principal that executed the operation.
    
    -----------------------------------------
    Batch, Change ID, FQN, and Uniqueness Rules
    -----------------------------------------
    - change_id:  Globally unique per log row; primary key. Used for deduplication and record tracking.
    - batch_id:   All log rows from a single operation (create or update batch) share the same batch_id, grouping changes together.
        - Format (f-string style):
            - For creates:       f"create-{N}_{fqn}"
            - For updates:       f"{maxCreate}_update-{M}_{fqn}"
        - Where:
            - N: Number of CREATE events already logged for this table, **plus one**.
                - Example:  
                    If log rows exist with  
                        batch_id = create-1_{fqn}  
                        batch_id = create-2_{fqn}  
                    ...and the table is deleted, then recreated,  
                    the next batch_id on CREATE will be  
                        batch_id = create-3_{fqn}
                - N is determined at runtime by querying all batch_ids for the current {fqn} with pattern 'create-*_{fqn}' and taking the highest N, then incrementing by 1.
            - maxCreate: The highest N for the current {fqn}. All updates after a given create are grouped under this N.
            - M: Number of UPDATE batches already performed since the most recent create, plus one.
                - Example:  
                    After batch_id = create-3_{fqn},  
                    the first update run would use batch_id = 3_update-1_{fqn},  
                    the next would be 3_update-2_{fqn}, etc.
            - When a table is deleted and recreated, N increases, and M resets for the new create version.
    
    - fqn: The **fully qualified table name**. Constructed as  
        f"{catalog}.{schema}.{table}"  
        using values from the YAML config.
    
    - run_id: The Databricks job or notebook run ID when the change is detected and logged (should be dynamically captured using Databricks context if possible).
    
    - To reconstruct all changes for a table, batch, or update: filter on batch_id and/or fqn.
    
    -----------------------------------------
    Audit Logic (Nested Rule Outline)
    -----------------------------------------
    
    1. On CREATE (table does not exist in target catalog.schema)
        - Action: Table is created as defined in the YAML.
        - Audit logging:
          - Log a single row:
              - batch_id: f"create-{N}_{fqn}"
              - change_category: "create"
              - before_value: JSON array with a single object:
                    [
                      {
                        "{fqn}": { full YAML config, all present/nested keys }
                      }
                    ]
              - after_value: JSON object of the config, top-level only.
              - Only present keys from YAML included—matches user input structure.
              - No empty/null/unused keys included.
    
    2. On UPDATE (table exists and any schema or metadata difference is detected)
        - Action: Each addition, removal, or change is applied as specified by YAML.
        - Audit logging:
          - Log one row **per change**, all from same run share the same batch_id:
              - batch_id: f"{maxCreate}_update-{M}_{fqn}"
              - change_category: "update"
              - change_type: 
                  "add_column", "drop_column", "rename_column", "type_change", "update_tag", "update_property", "add_constraint", "drop_constraint", etc.
              - subject_type: (e.g. "column_name", "table_tag", "column_check_constraint", etc.)
              - subject_name: the column name, tag key, or constraint key being changed
              - before_value: old value (JSON or null)
              - after_value: new value (JSON or null)
          - All rows from the same batch share a batch_id, so you can group a run's changes together.
    
    3. On NO-OP (table exists and no difference detected)
        - Action: No log rows are written.
        - Only actual modifications (create or update) result in audit events.
    
    4. On MULTIPLE CREATES (table dropped and re-created)
        - Every new create (even after drop) increments N in batch_id ("create-2_", "create-3_", etc).
        - All future updates after a create are grouped under that version.
        - Enables complete tracking of table incarnations.
    
    -----------------------------------------
    Example CREATE Log Row (using provided YAML)
    -----------------------------------------
    
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
    
    -----------------------------------------
    Example UPDATE Log Rows (same batch_id, one run)
    -----------------------------------------
    
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
    
    -----------------------------------------
    END
    -----------------------------------------
    
    
    ############
    
    
    Databricks/Spark audit logger for schema/table/column changes.

    BUSINESS RULES:
      - Only YAML-defined columns are written.
      - Handles create/update/remove with versioned batch IDs:
        * create-N_{fqn}: N = current create count + 1
        * {max-create}_update-M_{fqn}: max-create = latest create, M = update count since that create
      - before_value/after_value always JSON-stringified.
      - For create:
          * before_value is [ {fqn: config} ]
          * after_value is the config as JSON
      - For update/remove:
          * batch_id includes max-create and update count
      - change_hash is SHA256 of row content (for uniqueness/audit)
      - Auto-creates table from YAML if not present.
    """

    ALLOWED_ENVS = {'prd', 'dev', 'test', 'qa'}
    ALLOWED_CHANGE_CATEGORIES = {'create', 'update', 'delete', 'noop'}
    ALLOWED_SUBJECT_TYPES = {
        'table_description', 'table_tag', 'table_property', 'table_check_constraint',
        'row_filter', 'foreign_key', 'primary_key', 'partitioned_by', 'unique_key',
        'column_name', 'column_datatype', 'column_tag', 'column_comment',
        'column_check_constraint', 'column_nullable', 'column_active_status',
        'column_masking_rule', 'column_default_value', 'column_variable_value'
    }

    def __init__(self, spark, ddl_yaml_path, log_table, actor):
        self.spark = spark
        self.ddl_yaml_path = ddl_yaml_path
        self.log_table = log_table
        self.actor = actor
        self.ddl = self._load_ddl_yaml()
        self.columns = self._get_columns()
        if "change_hash" not in self.columns:
            self.columns.append("change_hash")  # Optional: add if you want to store it in the table

    def _load_ddl_yaml(self):
        with open(self.ddl_yaml_path) as f:
            return yaml.safe_load(f)
    
    def _get_columns(self):
        return [v["name"] for k, v in sorted(self.ddl["columns"].items(), key=lambda x: int(x[0]))]
    
    def _table_exists(self):
        return self.spark.catalog.tableExists(self.log_table)
    
    def _create_table(self):
        cols = []
        for k, v in sorted(self.ddl["columns"].items(), key=lambda x: int(x[0])):
            cdef = f"{v['name']} {v['datatype']}"
            if not v.get('nullable', True):
                cdef += " NOT NULL"
            cols.append(cdef)
        if "change_hash" not in [c.split()[0] for c in cols]:
            cols.append("change_hash STRING")
        schema = ", ".join(cols)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.log_table} ({schema}) USING DELTA")
        print(f"[AUDIT] Table {self.log_table} created.")

    def _j(self, val):
        if val is None: return None
        if isinstance(val, (dict, list)): return json.dumps(val, default=str)
        return str(val)
    
    def _enforce_allowed(self, field, value):
        if field == "env":
            val = (value or "dev").lower()
            if val not in self.ALLOWED_ENVS:
                raise ValueError(f"env must be one of {self.ALLOWED_ENVS}, got '{val}'")
            return val
        if field == "change_category":
            if value not in self.ALLOWED_CHANGE_CATEGORIES:
                raise ValueError(f"change_category must be one of {self.ALLOWED_CHANGE_CATEGORIES}, got '{value}'")
            return value
        if field == "subject_type":
            if value not in self.ALLOWED_SUBJECT_TYPES:
                raise ValueError(f"subject_type must be one of {self.ALLOWED_SUBJECT_TYPES}, got '{value}'")
            return value
        return value

    def _make_change_hash(self, row_dict):
        # Use a stable set of fields for the hash
        relevant = {k: row_dict.get(k) for k in self.columns if k not in {"change_id", "created_at", "change_hash"}}
        encoded = json.dumps(relevant, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _row(self, **kw):
        now = datetime.utcnow()
        out = {k: None for k in self.columns}
        out['change_id']       = str(uuid.uuid4())
        out['batch_id']        = kw.get('batch_id')
        out['run_id']          = kw.get('run_id')
        out['env']             = self._enforce_allowed("env", kw.get('env'))
        out['yaml_path']       = kw.get('yaml_path', self.ddl_yaml_path)
        out['fqn']             = kw.get('fqn')
        out['change_category'] = self._enforce_allowed("change_category", kw.get('change_category'))
        out['change_type']     = kw.get('change_type')
        out['subject_type']    = self._enforce_allowed("subject_type", kw.get('subject_type'))
        out['subject_name']    = kw.get('subject_name')
        out['tag_key']         = kw.get('tag_key')
        out['tag_value']       = kw.get('tag_value')
        out['property_key']    = kw.get('property_key')
        out['property_value']  = kw.get('property_value')
        out['before_value']    = self._j(kw.get('before_value'))
        out['after_value']     = self._j(kw.get('after_value'))
        out['notes']           = kw.get('notes')
        out['created_at']      = now
        out['created_by']      = self.actor
        out['updated_at']      = None
        out['updated_by']      = None
        out['change_hash']     = self._make_change_hash(out)
        return Row(**out)

    def _get_next_create_num(self, fqn):
        df = self.spark.sql(f"SELECT batch_id FROM {self.log_table} WHERE fqn = '{fqn}' AND change_category = 'create'")
        batch_ids = [row['batch_id'] for row in df.collect()]
        nums = [int(bid.split('-')[1].split('_')[0]) for bid in batch_ids if bid.startswith("create-")]
        return max(nums, default=0) + 1

    def _get_max_create_num(self, fqn):
        df = self.spark.sql(f"SELECT batch_id FROM {self.log_table} WHERE fqn = '{fqn}' AND change_category = 'create'")
        batch_ids = [row['batch_id'] for row in df.collect()]
        nums = [int(bid.split('-')[1].split('_')[0]) for bid in batch_ids if bid.startswith("create-")]
        return max(nums, default=0)

    def _get_next_update_num(self, fqn, create_num):
        prefix = f"{create_num}_update-"
        df = self.spark.sql(f"SELECT batch_id FROM {self.log_table} WHERE fqn = '{fqn}' AND change_category = 'update'")
        batch_ids = [row['batch_id'] for row in df.collect() if prefix in row['batch_id']]
        nums = [int(bid.split('_update-')[1].split('_')[0]) for bid in batch_ids if '_update-' in bid]
        return max(nums, default=0) + 1

    def log_changes(self, diff, cfg, fqn, env, run_id):
        if not self._table_exists():
            self._create_table()

        log_rows = []
        # CREATE EVENT
        if diff.get("table_created"):
            create_num = self._get_next_create_num(fqn)
            batch_id = f"create-{create_num}_{fqn}"
            full_snapshot = {fqn: cfg}
            before_array = [full_snapshot]
            log_rows.append(self._row(
                batch_id=batch_id,
                run_id=run_id,
                env=env,
                yaml_path=self.ddl_yaml_path,
                fqn=fqn,
                change_category="create",
                change_type="create_table",
                subject_type="table_description",
                subject_name=cfg["table"],
                before_value=before_array,
                after_value=cfg
            ))

        # UPDATE/REMOVE EVENTS (handles all diff types)
        else:
            create_num = self._get_max_create_num(fqn)
            update_num = self._get_next_update_num(fqn, create_num)
            batch_id = f"{create_num}_update-{update_num}_{fqn}"
            # Add columns
            for name, dtype in diff.get("added_columns", []):
                idx = next((k for k, v in cfg["columns"].items() if v["name"] == name), name)
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="add_column",
                    subject_type="column_name", subject_name=str(idx),
                    before_value=None, after_value=dtype
                ))
            # Drop columns
            for name in diff.get("dropped_columns", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="drop_column",
                    subject_type="column_name", subject_name=name,
                    before_value=name, after_value=None
                ))
            # Rename columns
            for old, new in diff.get("renamed_columns", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="rename_column",
                    subject_type="column_name", subject_name=new,
                    before_value=old, after_value=new
                ))
            # Type changes
            for col, from_type, to_type in diff.get("type_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="type_change",
                    subject_type="column_datatype", subject_name=col,
                    before_value=from_type, after_value=to_type
                ))
            # Property changes
            for k, old, new in diff.get("property_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_property",
                    subject_type="table_property", subject_name=k,
                    property_key=k, property_value=new,
                    before_value=old, after_value=new
                ))
            # Table tag changes
            for k, old, new in diff.get("table_tag_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_tag",
                    subject_type="table_tag", subject_name=k,
                    tag_key=k, tag_value=new,
                    before_value=old, after_value=new
                ))
            # Column tag changes
            for col, k, old, new in diff.get("column_tag_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_column_tag",
                    subject_type="column_tag", subject_name=col,
                    tag_key=k, tag_value=new,
                    before_value=old, after_value=new
                ))
            # Column comment changes
            for col, old, new in diff.get("column_comment_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_column_comment",
                    subject_type="column_comment", subject_name=col,
                    before_value=old, after_value=new
                ))
            # Table comment change
            if diff.get("table_comment_change"):
                old, new = diff["table_comment_change"]
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_comment",
                    subject_type="table_description", subject_name=cfg["table"],
                    before_value=old, after_value=new
                ))
            # Unique key changes
            for _, old, new in diff.get("unique_key_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_unique_key",
                    subject_type="unique_key", subject_name="unique_keys",
                    before_value=old, after_value=new
                ))
            # Foreign key changes
            for fk, old, new in diff.get("foreign_key_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_foreign_key",
                    subject_type="foreign_key", subject_name=fk,
                    before_value=old, after_value=new
                ))
            # Table check constraint changes
            for cname, old, new in diff.get("table_check_constraint_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_table_check_constraint",
                    subject_type="table_check_constraint", subject_name=cname,
                    before_value=old, after_value=new
                ))
            # Row filter changes
            for fname, old, new in diff.get("row_filter_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_row_filter",
                    subject_type="row_filter", subject_name=fname,
                    before_value=old, after_value=new
                ))
            # Column check constraint changes
            for col, ck, old, new in diff.get("column_check_constraint_changes", []):
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_column_check_constraint",
                    subject_type="column_check_constraint", subject_name=col,
                    before_value=old, after_value=new, notes=f"constraint: {ck}"
                ))

        if log_rows:
            df = self.spark.createDataFrame(log_rows)
            df.write.format("delta").mode("append").saveAsTable(self.log_table)
            print(f"[AUDIT] {len(log_rows)} rows logged to {self.log_table}")
        else:
            print("[AUDIT] No changes to log.")
