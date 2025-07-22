import uuid, json, yaml, hashlib
from datetime import datetime
from pyspark.sql import Row

class TableAuditLogger:
    """
    Databricks/Spark audit logger for schema/table/column changes.
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

    # ---- EVENT MAPPING ----
    EVENT_MAP = {
        "added_columns": dict(
            change_type="add_column",
            subject_type="column_name",
            subject_name=lambda item, cfg: str(next((k for k, v in cfg["columns"].items() if v["name"] == item[0]), item[0])),
            before_value=lambda item: None,
            after_value=lambda item: item[1],
            extra_fields={}
        ),
        "dropped_columns": dict(
            change_type="drop_column",
            subject_type="column_name",
            subject_name=lambda item, cfg: item,
            before_value=lambda item: item,
            after_value=lambda item: None,
            extra_fields={}
        ),
        "renamed_columns": dict(
            change_type="rename_column",
            subject_type="column_name",
            subject_name=lambda item, cfg: item[1],
            before_value=lambda item: item[0],
            after_value=lambda item: item[1],
            extra_fields={}
        ),
        "type_changes": dict(
            change_type="type_change",
            subject_type="column_datatype",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields={}
        ),
        "property_changes": dict(
            change_type="update_property",
            subject_type="table_property",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields=lambda item: {"property_key": item[0], "property_value": item[2]}
        ),
        "table_tag_changes": dict(
            change_type="update_tag",
            subject_type="table_tag",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields=lambda item: {"tag_key": item[0], "tag_value": item[2]}
        ),
        "column_tag_changes": dict(
            change_type="update_column_tag",
            subject_type="column_tag",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[2],
            after_value=lambda item: item[3],
            extra_fields=lambda item: {"tag_key": item[1], "tag_value": item[3]}
        ),
        "column_comment_changes": dict(
            change_type="update_column_comment",
            subject_type="column_comment",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields={}
        ),
        "unique_key_changes": dict(
            change_type="update_unique_key",
            subject_type="unique_key",
            subject_name=lambda item, cfg: "unique_keys",
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields={}
        ),
        "foreign_key_changes": dict(
            change_type="update_foreign_key",
            subject_type="foreign_key",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields={}
        ),
        "table_check_constraint_changes": dict(
            change_type="update_table_check_constraint",
            subject_type="table_check_constraint",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields={}
        ),
        "row_filter_changes": dict(
            change_type="update_row_filter",
            subject_type="row_filter",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[1],
            after_value=lambda item: item[2],
            extra_fields={}
        ),
        "column_check_constraint_changes": dict(
            change_type="update_column_check_constraint",
            subject_type="column_check_constraint",
            subject_name=lambda item, cfg: item[0],
            before_value=lambda item: item[2],
            after_value=lambda item: item[3],
            extra_fields=lambda item: {"notes": f"constraint: {item[1]}"}
        ),
    }

    def __init__(self, spark, ddl_yaml_path, log_table=None, actor=None):
        self.spark = spark
        self.ddl_yaml_path = ddl_yaml_path
        self.ddl = self._load_ddl_yaml()

        if log_table:
            self.log_table = log_table
        else:
            catalog = self.ddl.get("catalog", "")
            schema = self.ddl.get("schema", "")
            table = self.ddl.get("table", "")
            if not all([catalog, schema, table]):
                raise ValueError("Audit DDL YAML must specify catalog, schema, and table, or log_table must be passed explicitly.")
            self.log_table = f"{catalog}.{schema}.{table}"

        self.actor = actor
        self.columns = self._get_columns()
        if "change_hash" not in self.columns:
            self.columns.append("change_hash")

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
        else:
            create_num = self._get_max_create_num(fqn)
            update_num = self._get_next_update_num(fqn, create_num)
            batch_id = f"{create_num}_update-{update_num}_{fqn}"

            # Super-DRY unified loop
            for diff_key, event in self.EVENT_MAP.items():
                items = diff.get(diff_key, [])
                for item in items:
                    base = dict(
                        batch_id=batch_id,
                        run_id=run_id,
                        env=env,
                        yaml_path=self.ddl_yaml_path,
                        fqn=fqn,
                        change_category="update",
                        change_type=event["change_type"],
                        subject_type=event["subject_type"],
                        subject_name=event["subject_name"](item, cfg),
                        before_value=event["before_value"](item),
                        after_value=event["after_value"](item),
                    )
                    # Extra fields can be static dict or lambda
                    extra = event.get("extra_fields", {})
                    if callable(extra):
                        base.update(extra(item))
                    elif isinstance(extra, dict):
                        base.update(extra)
                    log_rows.append(self._row(**base))

            # Table comment change (special, single tuple)
            if diff.get("table_comment_change"):
                old, new = diff["table_comment_change"]
                log_rows.append(self._row(
                    batch_id=batch_id, run_id=run_id, env=env, yaml_path=self.ddl_yaml_path, fqn=fqn,
                    change_category="update", change_type="update_comment",
                    subject_type="table_description", subject_name=cfg["table"],
                    before_value=old, after_value=new
                ))

        if log_rows:
            df = self.spark.createDataFrame(log_rows)
            df.write.format("delta").mode("append").saveAsTable(self.log_table)
            print(f"[AUDIT] {len(log_rows)} rows logged to {self.log_table}")
        else:
            print("[AUDIT] No changes to log.")