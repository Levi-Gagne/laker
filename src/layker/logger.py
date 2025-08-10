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
from pyspark.sql import functions as F

from layker.snapshot_yaml import validate_and_snapshot_yaml
from layker.snapshot_table import TableSnapshot
from layker.differences import generate_differences
from layker.loader import DatabricksTableLoader
from layker.utils.table import table_exists, refresh_table


class TableAuditLogger:
    """
    Audit logger that derives:
      - target audit log table FQN by reading the audit DDL YAML
      - column order from the YAML's `columns` section

    Snapshot formatting:
      - "json_pretty" (default): sorted, pretty JSON
      - "json": sorted, compact JSON
      - "kv": nested [{'key': ..., 'value': ...}] arrays for readability
    """

    ALLOWED_ENVS = {"prd", "dev", "test", "qa"}

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

        # Resolve audit table FQN from YAML using the new snapshot validator
        audit_snapshot_yaml, audit_fq = validate_and_snapshot_yaml(
            audit_yaml_path, env=env, mode="apply"
        )
        self._audit_snapshot_yaml = audit_snapshot_yaml
        self.log_table: str = audit_fq

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
    # change_key helpers
    # ------------------------
    def _compute_change_key(self, fqn: str, change_category: str) -> str:
        """
        Build a human-readable sequence key per table:
          - For CREATE:        "create-{n}"
          - For UPDATE:        "create-{max_create}~update-{m}"
        Counts are computed from existing rows in the audit table for this FQN.
        """
        try:
            logs_df = self.spark.table(self.log_table).where(F.col("fqn") == fqn)
        except Exception:
            logs_df = None

        prior_create = 0
        prior_update = 0
        if logs_df is not None and logs_df.head(1):
            agg = (
                logs_df.groupBy("change_category")
                .agg(F.count(F.lit(1)).alias("cnt"))
                .collect()
            )
            for row in agg:
                if row["change_category"] == "create":
                    prior_create = int(row["cnt"] or 0)
                elif row["change_category"] == "update":
                    prior_update = int(row["cnt"] or 0)

        if change_category == "create":
            create_seq = prior_create + 1
            return f"create-{create_seq}"
        else:
            update_seq = prior_update + 1
            return f"create-{prior_create}~update-{update_seq}"

    # ------------------------
    # Row construction
    # ------------------------
    def _row(self, **kw) -> Row:
        now = datetime.utcnow()
        before_val = self._format_snapshot(kw.get("before_value"))
        after_val = self._format_snapshot(kw.get("after_value"))

        env_norm = self._normalized_env(kw.get("env"))
        fqn = kw.get("fqn")

        change_category = "create" if before_val is None else "update"
        change_key = self._compute_change_key(fqn=fqn, change_category=change_category)

        computed: Dict[str, Any] = {
            "change_id": str(uuid.uuid4()),
            "run_id": kw.get("run_id"),
            "env": env_norm,
            "yaml_path": kw.get("yaml_path"),
            "fqn": fqn,
            "change_category": change_category,
            "change_key": change_key,  # human-readable sequence key
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
        subject_name: Optional[str] = None,  # kept for compatibility; ignored
        notes: Optional[str] = None,
    ) -> None:
        row = self._row(
            run_id=run_id,
            env=env,
            yaml_path=yaml_path,
            fqn=fqn,
            before_value=before_value,
            after_value=after_value,
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
        # Resolve audit table FQN and snapshot from YAML
        audit_snapshot_yaml, audit_fq = validate_and_snapshot_yaml(
            audit_table_yaml_path, env=env, mode="apply"
        )
        if not table_exists(spark, audit_fq):
            print(f"[AUDIT] Audit table {audit_fq} not found; creating now...")
            # Use diff generator + loader to create from snapshot YAML
            diff = generate_differences(audit_snapshot_yaml, table_snapshot=None)
            DatabricksTableLoader(diff, spark, dry_run=False).run()
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
        subject_name=None,  # ignored by logger
        notes=notes,
    )
    print(f"[AUDIT] Event logged to {logger.log_table}")