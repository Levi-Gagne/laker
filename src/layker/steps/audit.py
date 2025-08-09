# src/layker/steps/audit.py

import os
import getpass
from typing import Any, Dict, Optional

from layker.utils.table import table_exists, refresh_table
from layker.steps.loader import apply_loader_step
from layker.logger import TableAuditLogger
from layker.steps.validate import validate_and_sanitize_yaml
from layker.snapshot_table import TableSnapshot
from layker.yaml import TableSchemaConfig

# Default path updated to the resource you pasted
DEFAULT_AUDIT_TABLE_YAML_PATH = "src/layker/resources/audit.yaml"


def resolve_audit_yaml_path(audit_log_table: Any) -> Optional[str]:
    if audit_log_table is True:
        return DEFAULT_AUDIT_TABLE_YAML_PATH
    if isinstance(audit_log_table, str):
        return audit_log_table
    if audit_log_table is False:
        return None
    raise ValueError("audit_log_table must be True, False, or a string path.")


def ensure_audit_table_exists(spark, env: str, audit_table_yaml_path: str) -> str:
    audit_fq = TableSchemaConfig(audit_table_yaml_path, env=env).full_table_name
    if not table_exists(spark, audit_fq):
        print(f"[AUDIT] Audit table {audit_fq} not found; creating now...")
        _ddl_cfg, cfg, _ = validate_and_sanitize_yaml(audit_table_yaml_path, env=env)
        apply_loader_step(cfg, spark, dry_run=False, action_desc="Audit table create")
    return audit_fq


def get_table_snapshot(spark, fq_table: str) -> Optional[Dict[str, Any]]:
    """
    Build a full snapshot dict from Unity Catalog for the target table.
    Returns None if the table does not exist.
    """
    try:
        snap = TableSnapshot(spark, fq_table).build_table_metadata_dict()
        return snap
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not get snapshot for {fq_table}: {e}")
        return None


def before_audit_log_flow(
    spark: Any,
    env: str,
    target_table_fq: str,
    audit_table_yaml_path: str,
) -> Optional[Dict[str, Any]]:
    """
    Ensures audit table exists, then gets BEFORE snapshot of the target table.
    """
    ensure_audit_table_exists(spark, env, audit_table_yaml_path)
    before_snapshot = get_table_snapshot(spark, target_table_fq)
    return before_snapshot


def after_audit_log_flow(
    spark: Any,
    env: str,
    before_snapshot: Optional[Dict[str, Any]],
    target_table_fq: str,
    yaml_path: Optional[str],
    audit_table_yaml_path: str,
    run_id: Optional[str] = None,
    notes: Optional[str] = None,
    snapshot_format: str = "json_pretty",  # "json_pretty" | "json" | "kv"
) -> None:
    """
    Refreshes target table, gets AFTER snapshot, and writes one audit row.
    """
    # Pull latest metadata after the loader updated the table
    refresh_table(spark, target_table_fq)
    after_snapshot = get_table_snapshot(spark, target_table_fq)

    audit_fq = TableSchemaConfig(audit_table_yaml_path, env=env).full_table_name
    actor = os.environ.get("USER") or getpass.getuser() or "AdminUser"

    logger = TableAuditLogger(
        spark=spark,
        log_table=audit_fq,
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
    print(f"[AUDIT] Event logged to {audit_fq}")