# src/layker/steps/audit.py

import os
from layker.audit.logger import TableAuditLogger
from layker.loader import DatabricksTableLoader

def apply_audit_log_step(
    spark,
    audit_yaml_path,
    diff,
    cfg,
    env,
    admin_user="unknown_actor"
):
    """
    Handles logging to the audit table, creating it if missing.
    """
    logger = TableAuditLogger(
        spark=spark,
        ddl_yaml_path=audit_yaml_path,
        admin_user=admin_user,
    )
    # Ensure the table exists, create if missing (using the loader)
    if not logger._table_exists():
        print(f"[AUDIT] Audit log table does not exist. Creating from {audit_yaml_path}...")
        with open(audit_yaml_path) as f:
            audit_cfg = yaml.safe_load(f)
        DatabricksTableLoader(audit_cfg, spark).create_or_update_table()
        logger = TableAuditLogger(
            spark=spark,
            ddl_yaml_path=audit_yaml_path,
            admin_user=admin_user,
        )
    logger.log_changes(
        diff=diff,
        cfg=cfg,
        env=env,
    )