# src/layker/steps/audit.py

import os
import getpass
from typing import Any, Dict, Optional, Tuple

from layker.utils.table import table_exists, refresh_table
from layker.steps.loader import apply_loader_step
from layker.audit.logger import TableAuditLogger
from layker.steps.validate import validate_and_sanitize_yaml
from layker.introspector import TableIntrospector
from layker.sanitizer import sanitize_snapshot
from layker.yaml import TableSchemaConfig

AUDIT_TABLE_YAML_PATH = "src/layker/audit/layker_audit.yaml"

def get_before_audit_snapshot(
    spark: Any,
    env: str,
    target_table_fq: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Ensures the audit log table exists (creating if needed), then returns the current
    sanitized snapshot of the target table AND the audit table FQ name.
    """
    if not table_exists(spark, audit_fq := TableSchemaConfig(AUDIT_TABLE_YAML_PATH, env=env).full_table_name):
        print(f"[AUDIT] Audit table {audit_fq} not found; starting validation...")
        ddl_cfg, cfg, _ = validate_and_sanitize_yaml(AUDIT_TABLE_YAML_PATH, env=env)
        apply_loader_step(cfg, spark, dry_run=False, action_desc="Audit table create")
        # After creation, move on—don't need a before snapshot of the audit table itself

    # Get snapshot of the *target* table we're auditing
    try:
        introspector = TableIntrospector(spark)
        raw_snap = introspector.snapshot(target_table_fq)
        before = sanitize_snapshot(raw_snap)
        return before, audit_fq
    except Exception as e:
        print(f"[AUDIT][ERROR] Could not get before snapshot for {target_table_fq}: {e}")
        return None, audit_fq

def log_after_audit(
    spark: Any,
    env: str,
    audit_fq: str,
    before_snapshot: Optional[Dict[str, Any]],
    target_table_fq: str,
    diff: Dict[str, Any],
    cfg: Dict[str, Any],
) -> None:
    """
    Refreshes the target table, grabs the after snapshot, and logs the audit row.
    """
    refresh_table(spark, target_table_fq)

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