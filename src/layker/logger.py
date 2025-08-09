# src/layker/logger.py

import uuid
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional, List

import yaml
from pyspark.sql import Row, SparkSession
from layker.yaml import TableSchemaConfig


class TableAuditLogger:
    """
    Audit logger that derives:
      - target table FQN from the audit DDL YAML
      - column order from the YAML's `columns` section

    Snapshot formatting:
      - "json_pretty" (default): sorted, pretty JSON
      - "json": sorted, compact JSON
      - "kv": nested [{'key': ..., 'value': ...}] arrays for readability
    """

    ALLOWED_ENVS = {"prd", "dev", "test", "qa"}
    ALLOWED_SUBJECT_TYPES = {"table_description"}

    def __init__(
        self,
        spark: SparkSession,
        audit_yaml_path: str,
        env: str,
        actor: str,
        snapshot_format: str = "json_pretty",  # "json_pretty" | "json" | "kv"
    ) -> None:
        self.spark = spark
        self.actor = actor
        self.snapshot_format = snapshot_format

        # Resolve table name from YAML (handles any env rules your TableSchemaConfig applies)
        self._table_cfg = TableSchemaConfig(audit_yaml_path, env=env)
        self.log_table: str = self._table_cfg.full_table_name

        # Load column order from YAML so no hard-coding lives here
        self.columns: List[str] = self._load_columns_from_yaml(audit_yaml_path)

    # ------------------------
    # YAML helpers
    # ------------------------
    def _load_columns_from_yaml(self, audit_yaml_path: str) -> List[str]:
        with open(audit_yaml_path, "r") as f:
            data = yaml.safe_load(f) or {}

        cols = data.get("columns", {})
        # keys like "1","2"... or ints; sort numerically when possible
        def _as_int(k):
            try:
                return int(k)
            except Exception:
                return 10**9

        ordered_keys = sorted(cols.keys(), key=_as_int)
        names: List[str] = []
        for k in ordered_keys:
            col = cols.get(k, {})
            name = col.get("name")
            if name:
                names.append(name)
        return names

    # ------------------------
    # Formatting helpers
    # ------------------------
    def _format_snapshot(self, snap: Any) -> Optional[str]:
        if snap is None:
            return None
        if self.snapshot_format == "kv":
            return json.dumps(self._to_kv_array(snap), indent=2, default=str)
        if self.snapshot_format == "json":
            return json.dumps(snap, sort_keys=True, separators=(",", ":"), default=str)
        return json.dumps(snap, sort_keys=True, indent=2, default=str)

    def _to_kv_array(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return [{"key": k, "value": self._to_kv_array(obj[k])} for k in sorted(obj.keys())]
        if isinstance(obj, list):
            return [self._to_kv_array(v) for v in obj]
        return obj

    def _normalized_env(self, env: Optional[str]) -> str:
        e = (env or "dev").lower()
        return e if e in self.ALLOWED_ENVS else "dev"

    def _make_change_hash(self, row_dict: Dict[str, Any]) -> str:
        # If you ever want a deterministic id beyond UUID
        relevant = {k: row_dict.get(k) for k in self.columns if k not in {"change_id", "created_at"}}
        encoded = json.dumps(relevant, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    # ------------------------
    # Row construction
    # ------------------------
    def _row(self, **kw) -> Row:
        now = datetime.utcnow()
        before_val = self._format_snapshot(kw.get("before_value"))
        after_val = self._format_snapshot(kw.get("after_value"))

        # Values we know how to populate if present in YAML
        computed: Dict[str, Any] = {
            "change_id": str(uuid.uuid4()),
            "run_id": kw.get("run_id"),
            "env": self._normalized_env(kw.get("env")),
            "yaml_path": kw.get("yaml_path"),
            "fqn": kw.get("fqn"),
            "change_category": "create" if before_val is None else "update",
            "change_type": "create_table" if before_val is None else "update_table",
            "subject_type": "table_description",
            "subject_name": kw.get("subject_name") or (kw.get("fqn") or "").split(".")[-1],
            "before_value": before_val,
            "after_value": after_val,
            "notes": kw.get("notes"),
            "created_at": now,
            "created_by": self.actor,
            "updated_at": None,
            "updated_by": None,
        }

        # Build ordered dict strictly per YAML-defined columns
        ordered: Dict[str, Any] = {c: computed.get(c) for c in self.columns}
        # Any columns in YAML that we didn't explicitly compute will be None (already via dict.get)

        return Row(**ordered)

    # ------------------------
    # Public API
    # ------------------------
    def log_change(
        self,
        run_id: Optional[str],
        env: str,
        yaml_path: Optional[str],
        fqn: str,
        before_value: Any,
        after_value: Any,
        subject_name: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
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