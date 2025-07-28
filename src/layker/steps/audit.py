# src/layker/steps/audit.py

import os
import getpass
from typing import Any, Dict, Optional

from layker.utils.table import table_exists, refresh_table
from layker.steps.loader import apply_loader_step
from layker.audit.logger import TableAuditLogger
from layker.steps.validate import validate_and_sanitize_yaml
from layker.introspector import TableIntrospector
from layker.sanitizer import sanitize_snapshot
from layker.yaml import TableSchemaConfig

AUDIT_TABLE_YAML_PATH = "src/layker/audit/layker_audit.yaml"

def ensure_audit_table_exists(spark: Any, env: str) -> str:
    """
    Ensures the audit table exists, creating it if necessary.
    Returns the fully qualified name of the audit table.
    """
    if not table_exists(spark, audit_fq := TableSchemaConfig(AUDIT_TABLE_YAML_PATH, env=env).full_table_name):
        print(f"[AUDIT] Audit table {audit_fq} not found; creating now...")
        ddl_cfg, cfg, _ = validate_and_sanitize_yaml(AUDIT_TABLE_YAML_PATH, env=env)
        apply_loader_step(cfg, spark, dry_run=False, action_desc="Audit table create")
    return audit_fq

def get_before_audit_snapshot(
    spark: Any,
    target_table_fq: str
) -> Optional[Dict[str, Any]]:
    """
    Returns the current sanitized snapshot for before/after audit logging.
    """
    try:
        introspector = TableIntrospector(spark)
        raw_snap = introspector.snapshot(target_table_fq)
        before = sanitize_snapshot(raw_snap)
        return before
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not get before snapshot for {target_table_fq}: {e}")
        return None

def log_after_audit(
    spark: Any,
    env: str,
    before_snapshot: Dict[str, Any],
    target_table_fq: str,
    diff: Dict[str, Any],
    cfg: Dict[str, Any],
) -> None:
    """
    Ensures the audit table exists, refreshes the target table, grabs after snapshot,
    and logs the audit row.
    """
    # Ensure audit table exists (may create if missing)
    audit_fq = ensure_audit_table_exists(spark, env)

    # Refresh the target table
    refresh_table(spark, target_table_fq)

    # Get after snapshot
    try:
        introspector = TableIntrospector(spark)
        raw_snap = introspector.snapshot(target_table_fq)
        after = sanitize_snapshot(raw_snap)
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not get after snapshot for {target_table_fq}: {e}")
        after = None

    admin_user = os.environ.get("USER") or getpass.getuser() or "AdminUser"
    logger = TableAuditLogger(
        spark=spark,
        ddl_yaml_path=AUDIT_TABLE_YAML_PATH,
        log_table=audit_fq,
        actor=admin_user,
    )

    try:
        logger.log_changes(
            diff=diff,
            cfg=cfg,
            env=env,
            before_snapshot=before_snapshot,
            after_snapshot=after
        )
        print(f"[AUDIT] Event logged to {audit_fq}")
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not log to {audit_fq}: {e}")