# src/layker/main.py

import os
import sys
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

# --- Steps ---
from layker.steps.validate import validate_and_sanitize_yaml
from layker.steps.loader import apply_loader_step
from layker.steps.audit import before_audit_log_flow, after_audit_log_flow
from layker.steps.differences import compute_and_validate_diff_controller

# --- Validators ---
from layker.validators.params import validate_params

# --- Utils ---
from layker.utils.table import table_exists
from layker.utils.spark import get_or_create_spark
from layker.utils.color import Color
from layker.utils.printer import (
    section_header,
    print_success,
    print_warning,
    print_error,
)

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit_log_table: Any = False,  # True, False, or str (YAML path)
) -> None:
    try:
        if spark is None:
            spark = get_or_create_spark()

        mode, env, audit_log_table = validate_params(
            yaml_path, log_ddl, mode, env, audit_log_table, spark
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
            elif mode in ("apply", "all") and not dry_run:
                apply_loader_step(cfg, spark, dry_run, fq, action_desc="Full create")
                if audit_log_table is not False:
                    after_audit_log_flow(
                        spark=spark,
                        env=env,
                        before_snapshot=None,
                        target_table_fq=fq,
                        yaml_path=yaml_path,
                        audit_table_yaml_path=audit_log_table,
                        run_id=None,
                        notes=None,
                    )
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

            if mode in ("apply", "all") and not dry_run:
                print(section_header("APPLYING METADATA CHANGES", color=Color.green))
                if audit_log_table is not False:
                    before_snapshot = before_audit_log_flow(
                        spark=spark,
                        env=env,
                        target_table_fq=fq,
                        audit_table_yaml_path=audit_log_table,
                    )
                    apply_loader_step(cfg, spark, dry_run, fq, action_desc="Updates applied")
                    after_audit_log_flow(
                        spark=spark,
                        env=env,
                        before_snapshot=before_snapshot,
                        target_table_fq=fq,
                        yaml_path=yaml_path,
                        audit_table_yaml_path=audit_log_table,
                        run_id=None,
                        notes=None,
                    )
                else:
                    apply_loader_step(cfg, spark, dry_run, fq, action_desc="Updates applied")

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
        print_error("Usage: python -m layker <yaml_path> [env] [dry_run] [mode] [audit_log_table]")
        sys.exit(1)
    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    audit_log_table = sys.argv[5] if len(sys.argv) > 5 else False
    run_table_load(
        yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode, audit_log_table=audit_log_table
    )

if __name__ == "__main__":
    cli_entry()
    
    
#################################

#    MAIN REFACTOR

import os
import sys
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

# --- Steps ---
from layker.snapshot_yaml import validate_and_snapshot_yaml
from layker.steps.loader import apply_loader_step
from layker.steps.audit import before_audit_log_flow, after_audit_log_flow
from layker.steps.differences import compute_and_validate_diff_controller

# --- Diff generator for full-create path ---
from layker.differences import generate_differences  # keep this

# --- New loader (accepts snapshot differences dict) ---
from layker.loader import DatabricksTableLoader  # NEW

# --- Validators ---
from layker.validators.params import validate_params

# --- Utils ---
from layker.utils.table import table_exists
from layker.utils.spark import get_or_create_spark
from layker.utils.color import Color
from layker.utils.printer import (
    section_header,
    print_success,
    print_warning,
    print_error,
)

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit_log_table: Any = False,  # True, False, or str (YAML path)
) -> None:
    try:
        if spark is None:
            spark = get_or_create_spark()

        mode, env, audit_log_table = validate_params(
            yaml_path, log_ddl, mode, env, audit_log_table, spark
        )

        print(section_header("STEP 1/4: VALIDATING YAML"))
        # Returns (snapshot_yaml, fully_qualified_table)
        snapshot_yaml, fq = validate_and_snapshot_yaml(yaml_path, env=env, mode=mode)
        if mode == "validate":
            print_success("YAML validation passed.")
            sys.exit(0)

        print(section_header("STEP 2/4: TABLE EXISTENCE CHECK"))
        if not (exists := table_exists(spark, fq)):
            print(f"{Color.b}{Color.ivory}Table {Color.aqua_blue}{fq}{Color.ivory} not found.{Color.r}")
            if mode == "diff":
                print_warning(f"[DIFF] No table to compare; would create table {fq}.")
                return
            elif mode in ("apply", "all") and not dry_run:
                # Build a full-create diff from the YAML snapshot
                snapshot_dict = generate_differences(snapshot_yaml, None)
                snapshot_diff = snapshot_dict  # alias for inline checks, per request

                # Inline: exit_if_no_changes(snapshot_diff)
                if not any(snapshot_diff.values()):
                    print_success("No metadata changes detected; exiting cleanly. Everything is up to date.")
                    sys.exit(0)

                # Inline: exit_if_mode_diff(snapshot_diff, mode)
                if mode == "diff":
                    print_warning(f"[DIFF] Proposed changes:")
                    for k, v in snapshot_diff.items():
                        if v:
                            print(f"{Color.b}{Color.aqua_blue}{k}:{Color.ivory} {v}{Color.r}")
                    sys.exit(0)

                # NEW: use the new loader that accepts the snapshot differences dict
                DatabricksTableLoader(snapshot_dict, spark, dry_run=dry_run).run()

                if audit_log_table is not False:
                    after_audit_log_flow(
                        spark=spark,
                        env=env,
                        before_snapshot=None,            # create path => no "before"
                        target_table_fq=fq,
                        yaml_path=yaml_path,
                        audit_table_yaml_path=audit_log_table,
                        run_id=None,
                        notes=None,
                    )
                return
            print_error("Unreachable: Table does not exist and mode is not 'diff', 'apply', or 'all'. Exiting.")
            sys.exit(1)
        else:
            # ---- Table exists: Diff, and maybe apply changes
            diff, clean_snap = compute_and_validate_diff_controller(
                spark=spark,
                cfg=snapshot_yaml,
                fq=fq,
                mode=mode,
                yaml_path=yaml_path,
                log_ddl=log_ddl,
            )

            if mode in ("apply", "all") and not dry_run:
                print(section_header("APPLYING METADATA CHANGES", color=Color.green))
                if audit_log_table is not False:
                    before_snapshot = before_audit_log_flow(
                        spark=spark,
                        env=env,
                        target_table_fq=fq,
                        audit_table_yaml_path=audit_log_table,
                    )
                    apply_loader_step(snapshot_yaml, spark, dry_run, fq, action_desc="Updates applied")
                    after_audit_log_flow(
                        spark=spark,
                        env=env,
                        before_snapshot=before_snapshot,
                        target_table_fq=fq,
                        yaml_path=yaml_path,
                        audit_table_yaml_path=audit_log_table,
                        run_id=None,
                        notes=None,
                    )
                else:
                    apply_loader_step(snapshot_yaml, spark, dry_run, fq, action_desc="Updates applied")

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
        print_error("Usage: python -m layker <yaml_path> [env] [dry_run] [mode] [audit_log_table]")
        sys.exit(1)
    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    audit_log_table = sys.argv[5] if len(sys.argv) > 5 else False
    run_table_load(
        yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode, audit_log_table=audit_log_table
    )

if __name__ == "__main__":
    cli_entry()
