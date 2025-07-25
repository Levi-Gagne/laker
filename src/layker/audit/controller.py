# src/layker/audit/controller.py

from layker.audit.logger import TableAuditLogger
from layker.utils.color import Color

def log_table_audit(
    spark,
    audit_yaml_path,
    log_table,
    actor,
    diff,
    cfg,
    fqn,
    env,
    run_id,
    announce: bool = True,
):
    """
    Centralized controller for logging table audit events and printing workflow status.
    """
    try:
        logger = TableAuditLogger(
            spark,
            ddl_yaml_path=audit_yaml_path,
            log_table=log_table,
            actor=actor,
        )
        logger.log_changes(
            diff=diff,
            cfg=cfg,
            fqn=fqn,
            env=env,
            run_id=run_id,
        )
        if announce:
            if diff.get("table_created"):
                print(f"{Color.b}{Color.green}✔ Workflow complete. Table created and audit logged.{Color.r}")
            else:
                print(f"{Color.b}{Color.green}✔ Workflow complete. Updates applied and audit logged.{Color.r}")
    except Exception as e:
        print(f"{Color.b}{Color.candy_red}✘ Audit log failed: {e}{Color.r}")