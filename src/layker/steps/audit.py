# src/layker/steps/audit.py

import os
import getpass
from typing import Any, Dict, Optional

from layker.utils.table import table_exists, refresh_table
from layker.steps.loader import apply_loader_step
from layker.audit.logger import TableAuditLogger
from layker.steps.validate import validate_and_sanitize_yaml
from layker.introspector import TableIntrospector
from layker.yaml import TableSchemaConfig

DEFAULT_AUDIT_TABLE_YAML_PATH = "src/layker/audit/layker_audit.yaml"

def resolve_audit_yaml_path(audit_log_table: Any) -> str:
    if audit_log_table is True:
        return DEFAULT_AUDIT_TABLE_YAML_PATH
    elif isinstance(audit_log_table, str):
        return audit_log_table
    else:
        raise ValueError("Audit table YAML path must be True (default) or a string path.")

def ensure_audit_table_exists(spark, env, audit_table_yaml_path) -> str:
    audit_fq = TableSchemaConfig(audit_table_yaml_path, env=env).full_table_name
    if not table_exists(spark, audit_fq):
        print(f"[AUDIT] Audit table {audit_fq} not found; creating now...")
        ddl_cfg, cfg, _ = validate_and_sanitize_yaml(audit_table_yaml_path, env=env)
        apply_loader_step(cfg, spark, dry_run=False, action_desc="Audit table create")
    return audit_fq

def get_table_snapshot(spark, fq_table: str) -> Optional[Dict[str, Any]]:
    try:
        introspector = TableIntrospector(spark)
        return introspector.snapshot(fq_table)
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
    yaml_path: str,
    audit_table_yaml_path: str,
    run_id: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Refreshes target table, gets after snapshot, and writes audit log row.
    """
    refresh_table(spark, target_table_fq)
    after_snapshot = get_table_snapshot(spark, target_table_fq)
    audit_fq = TableSchemaConfig(audit_table_yaml_path, env=env).full_table_name
    admin_user = os.environ.get("USER") or getpass.getuser() or "AdminUser"
    logger = TableAuditLogger(
        spark=spark,
        log_table=audit_fq,
        actor=admin_user,
    )
    logger.log_change(
        run_id=run_id,
        env=env,
        yaml_path=yaml_path,
        fqn=target_table_fq,
        before_value=before_snapshot,
        after_value=after_snapshot,
        subject_name=target_table_fq.split('.')[-1],
        notes=notes,
    )
    print(f"[AUDIT] Event logged to {audit_fq}")