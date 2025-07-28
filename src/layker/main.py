# src/layker/main.py

import os
import sys
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

# --- Steps ---
from layker.steps.validate import validate_and_sanitize_yaml
from layker.steps.loader import apply_loader_step
from layker.steps.audit import get_before_audit_snapshot, log_after_audit
from layker.steps.differences import compute_and_validate_diff_controller

# --- Validators ---
from layker.validators.params import validate_params

# --- Utils ---
from layker.utils.table import table_exists
from layker.utils.spark import get_or_create_spark_session
from layker.utils.color import Color
from layker.utils.printer import (
    section_header,
    print_success,
    print_warning,
    print_error,
)

DEFAULT_AUDIT_TABLE_YAML_PATH = "src/layker/audit/layker_audit.yaml"

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit_table_yaml: Optional[bool or str] = True,  # True = default path, str = custom, False/None = disable
) -> None:
    try:
        if spark is None:
            spark = get_or_create_spark_session()

        mode, env, _ = validate_params(
            yaml_path, log_ddl, mode, env, None, spark
        )

        print(section_header("STEP 1/4: VALIDATING YAML"))
        ddl_cfg, cfg, fq = validate_and_sanitize_yaml(yaml_path, env=env)
        if mode == "validate":
            print_success("YAML validation passed.")
            sys.exit(0)

        print(section_header("STEP 2/4: TABLE EXISTENCE CHECK"))
        if not (exists := table_exists(spark, fq)):
            print(f"{Color.b}{Color.ivory}Table {Color.aqua_blue}{fq}{Color.ivory} not found.{Color.r}")
            if mode == "diff":
                print_warning(f"[DIFF] No table to compare; would create table {fq}.")
                return
            elif mode in ("apply", "all"):
                if not dry_run:
                    before_snapshot = get_before_audit_snapshot(spark, fq)
                    apply_loader_step(cfg, spark, dry_run, fq, action_desc="Full create")
                    # -- AUDIT LOGGING --
                    if audit_table_yaml is not False and audit_table_yaml is not None:
                        if audit_table_yaml is True:
                            audit_table_yaml_path = DEFAULT_AUDIT_TABLE_YAML_PATH
                        else:
                            audit_table_yaml_path = audit_table_yaml
                        log_after_audit(
                            spark,
                            env,
                            before_snapshot=before_snapshot,
                            target_table_fq=fq,
                            diff={"table_created": True},
                            cfg=cfg,
                            audit_table_yaml_path=audit_table_yaml_path,
                        )
                else:
                    print_success("[DRY RUN] Would create table and log audit event (skipped).")
                return
            print_error("Unreachable: Table does not exist and mode is not 'diff', 'apply', or 'all'. Exiting.")
            sys.exit(1)
        else:
            # ---- Table exists: Diff, and maybe apply changes
            diff, clean_snap = compute_and_validate_diff_controller(
                spark=spark,
                cfg=cfg,
                fq=fq,
                mode=mode,
                yaml_path=yaml_path,
                log_ddl=log_ddl,
            )

            if mode in ("apply", "all"):
                if not dry_run:
                    print(section_header("APPLYING METADATA CHANGES", color=Color.green))
                    before_snapshot = get_before_audit_snapshot(spark, fq)
                    apply_loader_step(cfg, spark, dry_run, fq, action_desc="Updates applied")
                    # -- AUDIT LOGGING --
                    if audit_table_yaml is not False and audit_table_yaml is not None:
                        if audit_table_yaml is True:
                            audit_table_yaml_path = DEFAULT_AUDIT_TABLE_YAML_PATH
                        else:
                            audit_table_yaml_path = audit_table_yaml
                        log_after_audit(
                            spark,
                            env,
                            before_snapshot=before_snapshot,
                            target_table_fq=fq,
                            diff=diff,
                            cfg=cfg,
                            audit_table_yaml_path=audit_table_yaml_path,
                        )
                else:
                    print_success("[DRY RUN] Would apply updates and log audit event (skipped).")

    except SystemExit:
        raise
    except KeyboardInterrupt:
        print_error("Interrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        print_error(f"Fatal error during run_table_load:\n{e}")
        import traceback
        print(f"{Color.red}{traceback.format_exc()}{Color.r}")
        sys.exit(1)

def cli_entry():
    if len(sys.argv) < 2:
        print_error("Usage: python -m layker <yaml_path> [env] [dry_run] [mode] [audit_table_yaml]")
        sys.exit(1)
    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    audit_table_yaml = sys.argv[5] if len(sys.argv) > 5 else True
    run_table_load(
        yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode, audit_table_yaml=audit_table_yaml
    )

if __name__ == "__main__":
    cli_entry()