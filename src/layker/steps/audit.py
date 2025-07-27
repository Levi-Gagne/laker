# src/layker/steps/audit.py

import os
import getpass
from typing import Dict, Any

from layker.steps.loader import apply_loader_step
from layker.utils.table import table_exists
from layker.audit.logger import TableAuditLogger
from layker.yaml import TableSchemaConfig

def apply_audit_log_step(
    spark,                   # SparkSession
    audit_table_yaml_path: str,
    diff: Dict[str, Any],
    cfg: Dict[str, Any],
    env: str,
) -> None:
    """
    Ensures the audit log table exists, creating if needed, then logs the change event.
    """
    # Load YAML config for the audit table structure
    audit_cfg = TableSchemaConfig(audit_table_yaml_path, env=env)
    fq = audit_cfg.full_table_name

    # Use os environment, else default to 'AdminUser'
    admin_user = os.environ.get("USER") or getpass.getuser() or "AdminUser"

    logger = TableAuditLogger(
        spark=spark,
        ddl_yaml_path=audit_table_yaml_path,
        log_table=fq,
        actor=admin_user,
    )

    if not table_exists(spark, fq):
        print(f"[AUDIT] Audit table {fq} not found; creating now...")
        apply_loader_step(audit_cfg._config, spark, dry_run=False, fq=fq, action_desc="Audit table create")
    else:
        try:
            logger.log_changes(
                diff=diff,
                cfg=cfg,
                env=env,
            )
            print(f"[AUDIT] Event logged to {fq}")
        except Exception as e:
            print(f"[AUDIT][ERROR] Could not log to {fq}: {e}")