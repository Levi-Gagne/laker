# src/layker/logger.py

import os
import getpass
import uuid
import json
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional, List

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from layker.snapshot_yaml import validate_and_snapshot_yaml
from layker.snapshot_table import TableSnapshot
from layker.differences import generate_differences
from layker.loader import DatabricksTableLoader
from layker.utils.table import table_exists, refresh_table


"""
AUDIT_LOG_DOC: Dict[str, str] = {
    "change_id": "UUID per row; primary key.",
    "run_id": "Optional external/job run identifier.",
    "env": "Normalized environment (prd|dev|test|qa); unknowns coerced to 'dev'.",
    "yaml_path": "Path to the YAML that drove the change.",
    "fqn": "Fully qualified table name (catalog.schema.table).",
    "change_category": "Derived: 'create' iff BEFORE snapshot is None; else 'update'.",
    "change_key": (
        "Human sequence key per table. "
        "CREATE: 'create-{n}'. UPDATE: 'create-{C}~update-{m}', where C is the latest create number "
        "and m counts updates since that create. Guaranteed unique per (fqn, change_key)."
    ),
    "before_value": "JSON string (pretty) of pre-change metadata; NULL for create.",
    "after_value": "JSON string (pretty) of post-change metadata.",
    "notes": "Optional free-text context.",
    "created_at": "UTC timestamp when the row is written.",
    "created_by": "User/service principal writing the row.",
    "updated_at": "Reserved for future updates; NULL by default.",
    "updated_by": "Reserved for future updates; NULL by default.",
}
"""

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

        # Resolve audit table FQN from YAML using the YAML validator/snapshotter
        audit_snapshot_yaml, audit_fq = validate_and_snapshot_yaml(
            audit_yaml_path, env=env, mode="apply"
        )
        self._audit_snapshot_yaml = audit_snapshot_yaml
        self.log_table: str = audit_fq

        # Load column order from YAML so no hard-coding lives here
        self.columns: List[str] = self._load_columns_from_yaml(audit_yaml_path)

        # Build an explicit schema so Spark Connect doesn't need to infer types
        self._schema: StructType = self._build_schema_from_yaml(self._audit_snapshot_yaml)

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

    def _build_schema_from_yaml(self, snap_yaml: Dict[str, Any]) -> StructType:
        """
        Build a Spark StructType that matches the audit table YAML snapshot.
        Defaults unknown/unsupported types to StringType (safe for JSON payloads).
        """
        type_map = {
            "string": StringType(),
            "timestamp": TimestampType(),
        }

        fields: List[StructField] = []
        cols = snap_yaml.get("columns", {}) or {}
        # YAML snapshot has string keys "1","2"...; sort by numeric index
        for idx in sorted(map(int, cols.keys())):
            c = cols[str(idx)]
            name = c.get("name")
            dt = (c.get("datatype") or "string").strip().lower()
            nullable = bool(c.get("nullable", True))
            spark_type = type_map.get(dt, StringType())
            fields.append(StructField(name, spark_type, nullable))
        return StructType(fields)

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
          - For CREATE:  'create-{n}', where n = max existing create number + 1 (for this fqn)
          - For UPDATE:  'create-{C}~update-{m}', where
                         C = max existing create number (>=1),
                         m = existing updates since that C + 1
        Guarantees uniqueness per (fqn, change_key) by probing.
        """
        try:
            all_for_table = self.spark.table(self.log_table).where(F.col("fqn") == fqn)
        except Exception:
            all_for_table = None

        def _max_create_num(df) -> int:
            if df is None or not df.head(1):
                return 0
            row = (
                df.where(F.col("change_category") == "create")
                  .select(F.max(F.regexp_extract("change_key", r"create-(\d+)", 1).cast("int")).alias("c"))
                  .collect()
            )
            return int(row[0]["c"] or 0) if row else 0

        def _max_update_num_for_create(df, C: int) -> int:
            if df is None or not df.head(1):
                return 0
            row = (
                df.where(
                    (F.col("change_category") == "update")
                    & (F.col("change_key").like(f"create-{C}~update-%"))
                )
                .select(F.max(F.regexp_extract("change_key", r"update-(\d+)$", 1).cast("int")).alias("m"))
                .collect()
            )
            return int(row[0]["m"] or 0) if row else 0

        def _exists_key(df, key: str) -> bool:
            if df is None:
                return False
            try:
                return df.where(F.col("change_key") == key).limit(1).count() > 0
            except Exception:
                # Fallback if count not available in environment
                rows = df.where(F.col("change_key") == key).limit(1).collect()
                return len(rows) > 0

        if change_category == "create":
            c = _max_create_num(all_for_table) + 1
            candidate = f"create-{c}"
            # Probe uniqueness
            while _exists_key(all_for_table, candidate):
                c += 1
                candidate = f"create-{c}"
            return candidate

        # update
        current_c = _max_create_num(all_for_table)
        if current_c <= 0:
            # No prior create recorded; start at 1 for readability.
            current_c = 1
        m = _max_update_num_for_create(all_for_table, current_c) + 1
        candidate = f"create-{current_c}~update-{m}"
        # Probe uniqueness
        while _exists_key(all_for_table, candidate):
            m += 1
            candidate = f"create-{current_c}~update-{m}"
        return candidate

    # ------------------------
    # Row construction
    # ------------------------
    def _row(self, **kw) -> Dict[str, Any]:
        now = datetime.utcnow()
        before_obj = kw.get("before_value")
        after_obj = kw.get("after_value")

        before_val = self._format_snapshot(before_obj)
        after_val = self._format_snapshot(after_obj)

        env_norm = self._normalized_env(kw.get("env"))
        fqn = kw.get("fqn")

        change_category = "create" if before_val is None else "update"
        # Sanity: CREATE rows should always have None BEFORE
        if change_category == "create" and before_obj is not None:
            print("[AUDIT][WARN] CREATE row but BEFORE snapshot is not None; proceeding.")

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

        # Keep order defined by YAML columns
        return {c: computed.get(c) for c in self.columns}

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
        row_dict = self._row(
            run_id=run_id,
            env=env,
            yaml_path=yaml_path,
            fqn=fqn,
            before_value=before_value,
            after_value=after_value,
            notes=notes,
        )
        # Explicit schema avoids Spark Connect inference issues on serverless
        df = self.spark.createDataFrame([row_dict], schema=self._schema)
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

    # 2) Refresh target table (no-op/skip on serverless inside refresh_table) and take AFTER snapshot
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