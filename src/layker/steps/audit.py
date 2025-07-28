# src/layker/steps/audit.py

import os
import getpass
from typing import Any, Dict, Optional, Union

from layker.utils.table import table_exists, refresh_table
from layker.steps.loader import apply_loader_step
from layker.audit.logger import TableAuditLogger
from layker.steps.validate import validate_and_sanitize_yaml
from layker.introspector import TableIntrospector
from layker.sanitizer import sanitize_snapshot
from layker.yaml import TableSchemaConfig

DEFAULT_AUDIT_TABLE_YAML_PATH = "src/layker/audit/layker_audit.yaml"

def ensure_audit_table_exists(spark: Any, env: str, audit_table_yaml_path: str) -> str:
    """
    Ensures the audit table exists, creating it if necessary.
    Returns the fully qualified name of the audit table.
    """
    audit_fq = TableSchemaConfig(audit_table_yaml_path, env=env).full_table_name
    if not table_exists(spark, audit_fq):
        print(f"[AUDIT] Audit table {audit_fq} not found; creating now...")
        ddl_cfg, cfg, _ = validate_and_sanitize_yaml(audit_table_yaml_path, env=env)
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

def get_after_audit_snapshot(spark: Any, target_table_fq: str) -> Optional[Dict[str, Any]]:
    """
    Returns the current sanitized after snapshot for the target table.
    """
    try:
        introspector = TableIntrospector(spark)
        raw_snap = introspector.snapshot(target_table_fq)
        after = sanitize_snapshot(raw_snap)
        return after
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not get after snapshot for {target_table_fq}: {e}")
        return None

def audit_log_flow(
    spark: Any,
    env: str,
    before_snapshot: Dict[str, Any],
    target_table_fq: str,
    diff: Dict[str, Any],
    cfg: Dict[str, Any],
    audit_table_yaml: Union[str, bool, None] = None
) -> None:
    """
    Handles the full audit log flow: path resolution, table ensure, refresh, after snapshot, and logging.
    """
    # --- Step 1: Determine audit YAML path
    if audit_table_yaml is True:
        audit_table_yaml_path = DEFAULT_AUDIT_TABLE_YAML_PATH
    elif isinstance(audit_table_yaml, str):
        audit_table_yaml_path = audit_table_yaml
    else:
        raise ValueError("Invalid audit_table_yaml: should be True or path to YAML.")

    # --- Step 2: Ensure audit table exists
    audit_fq = ensure_audit_table_exists(spark, env, audit_table_yaml_path)

    # --- Step 3: Refresh target table (so after snapshot is accurate)
    refresh_table(spark, target_table_fq)

    # --- Step 4: Get after snapshot
    after_snapshot = get_after_audit_snapshot(spark, target_table_fq)

    # --- Step 5: Log audit event
    admin_user = os.environ.get("USER") or getpass.getuser() or "AdminUser"
    logger = TableAuditLogger(
        spark=spark,
        ddl_yaml_path=audit_table_yaml_path,
        log_table=audit_fq,
        actor=admin_user,
    )

    try:
        logger.log_changes(
            diff=diff,
            cfg=cfg,
            env=env,
            before_snapshot=before_snapshot,
            after_snapshot=after_snapshot
        )
        print(f"[AUDIT] Event logged to {audit_fq}")
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not log to {audit_fq}: {e}")