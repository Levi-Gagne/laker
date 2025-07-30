# src/layker/audit/logger.py

import uuid
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional, List
from pyspark.sql import Row, SparkSession

class TableAuditLogger:
    """
    Databricks/Spark audit logger for table changes.
    """

    ALLOWED_ENVS = {'prd', 'dev', 'test', 'qa'}
    ALLOWED_CHANGE_CATEGORIES = {'create', 'update'}
    ALLOWED_SUBJECT_TYPES = {'table_description'}

    def __init__(
        self,
        spark: SparkSession,
        log_table: str,
        actor: str,
    ) -> None:
        self.spark = spark
        self.log_table = log_table
        self.actor = actor

        # Set columns explicitly as your table DDL defines them (ordered)
        self.columns = [
            "change_id", "run_id", "env", "yaml_path", "fqn", "change_category", "change_type",
            "subject_type", "subject_name", "tag_key", "tag_value", "property_key", "property_value",
            "before_value", "after_value", "notes", "created_at", "created_by", "updated_at", "updated_by"
        ]

    def _j(self, val: Any) -> Optional[str]:
        if val is None:
            return None
        if isinstance(val, (dict, list)):
            return json.dumps(val, default=str)
        return str(val)

    def _make_change_hash(self, row_dict: Dict[str, Any]) -> str:
        relevant = {k: row_dict.get(k) for k in self.columns if k not in {"change_id", "created_at"}}
        encoded = json.dumps(relevant, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _row(self, **kw) -> Row:
        now = datetime.utcnow()
        out = {k: None for k in self.columns}
        out['change_id']       = str(uuid.uuid4())
        out['run_id']          = kw.get('run_id')
        out['env']             = (kw.get('env') or "dev").lower()
        out['yaml_path']       = kw.get('yaml_path')
        out['fqn']             = kw.get('fqn')
        # category: infer from before_value is None
        out['change_category'] = "create" if kw.get("before_value") is None else "update"
        out['change_type']     = "create_table" if out['change_category'] == "create" else "update_table"
        out['subject_type']    = "table_description"
        out['subject_name']    = kw.get('subject_name')
        out['tag_key']         = None
        out['tag_value']       = None
        out['property_key']    = None
        out['property_value']  = None
        out['before_value']    = self._j(kw.get('before_value'))
        out['after_value']     = self._j(kw.get('after_value'))
        out['notes']           = kw.get('notes')
        out['created_at']      = now
        out['created_by']      = self.actor
        out['updated_at']      = None
        out['updated_by']      = None
        return Row(**out)

    def log_change(
        self,
        run_id: Optional[str],
        env: str,
        yaml_path: str,
        fqn: str,
        before_value: Any,
        after_value: Any,
        subject_name: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        # Compose row (change_category logic is 100% internal here)
        row = self._row(
            run_id=run_id,
            env=env,
            yaml_path=yaml_path,
            fqn=fqn,
            before_value=before_value,
            after_value=after_value,
            subject_name=subject_name,
            notes=notes,
        )
        df = self.spark.createDataFrame([row])
        df.write.format("delta").mode("append").saveAsTable(self.log_table)
        print(f"[AUDIT] 1 row logged to {self.log_table}")
