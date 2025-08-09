# src/layker/logger.py

import os
import getpass
import uuid
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional, List

import yaml
from pyspark.sql import Row, SparkSession

from layker.yaml import TableSchemaConfig
from layker.snapshot_table import TableSnapshot
from layker.utils.table import table_exists, refresh_table
from layker.steps.validate import validate_and_sanitize_yaml
from layker.steps.loader import apply_loader_step


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

        ordered: Dict[str, Any] = {c: computed.get(c) for c in self.columns}
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


# ------------------------
# One-call audit flow (with local ensure helper)
# ------------------------
def audit_log_flow(
    spark: SparkSession,
    env: str,
    before_snapshot: Optional[Dict[str, Any]],
    target_table_fq: str,
    yaml_path: Optional[str],
    audit_table_yaml_path: str,
    run_id: Optional[str] = None,
    notes: Optional[str] = None,
    snapshot_format: str = "json_pretty",
) -> None:
    """
    Ensure audit table exists, refresh target table, take AFTER snapshot inline,
    and append a single audit row. Uses BEFORE snapshot provided by caller.
    """

    def _ensure_audit_table_exists_local() -> str:
        audit_fq = TableSchemaConfig(audit_table_yaml_path, env=env).full_table_name
        if not table_exists(spark, audit_fq):
            print(f"[AUDIT] Audit table {audit_fq} not found; creating now...")
            _ddl_cfg, cfg, _ = validate_and_sanitize_yaml(audit_table_yaml_path, env=env)
            apply_loader_step(cfg, spark, dry_run=False, action_desc="Audit table create")
        return audit_fq

    # 1) Ensure the audit table exists (scoped to this flow)
    _ensure_audit_table_exists_local()

    # 2) Refresh target table to ensure latest metadata, then take AFTER snapshot
    refresh_table(spark, target_table_fq)
    after_snapshot = TableSnapshot(spark, target_table_fq).build_table_metadata_dict()

    # 3) Write the audit log row (BEFORE = provided snapshot, AFTER = freshly captured)
    actor = os.environ.get("USER") or getpass.getuser() or "AdminUser"
    logger = TableAuditLogger(
        spark=spark,
        audit_yaml_path=audit_table_yaml_path,
        env=env,
        actor=actor,
        snapshot_format=snapshot_format,
    )
    logger.log_change(
        run_id=run_id,
        env=env,
        yaml_path=yaml_path,
        fqn=target_table_fq,
        before_value=before_snapshot,
        after_value=after_snapshot,
        subject_name=target_table_fq.split(".")[-1],
        notes=notes,
    )
    print(f"[AUDIT] Event logged to {logger.log_table}")