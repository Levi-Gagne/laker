# source: layker/main.py

import os
import sys
import yaml
import getpass
from pathlib import Path
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession

# --- Steps ---
from layker.steps.validate import validate_and_sanitize_yaml
from layker.steps.loader import apply_loader_step
from layker.steps.audit import apply_audit_log_step

# --- Validators ---
from layker.validators.params import validate_params
from layker.validators.differences import validate_diff
from layker.validators.evolution import handle_schema_evolution

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

# --- Core logic ---
from layker.introspector import TableIntrospector
from layker.differences import compute_diff, log_comparison
from layker.yaml import TableSchemaConfig
from layker.sanitizer import sanitize_snapshot

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit_table_yaml_path: Optional[str] = None,
) -> None:
    try:
        # ---- Spark init
        if spark is None:
            spark = get_or_create_spark_session()

        # ---- Param validation
        mode, env, audit_table_yaml_path = validate_params(
            yaml_path, log_ddl, mode, env, audit_table_yaml_path, spark
        )

        # ---- STEP 1: VALIDATE & SANITIZE YAML
        print(section_header("STEP 1/4: VALIDATING YAML"))
        ddl_cfg, cfg, fq = validate_and_sanitize_yaml(yaml_path, env=env)
        if mode == "validate":
            print_success("YAML validation passed.")
            sys.exit(0)

        # ---- STEP 2: TABLE EXISTENCE CHECK (with walrus operator)
        print(section_header("STEP 2/4: TABLE EXISTENCE CHECK"))
        if not (exists := table_exists(spark, fq)):
            print(f"{Color.b}{Color.ivory}Table {Color.aqua_blue}{fq}{Color.ivory} not found.{Color.r}")
            if mode == "diff":
                print_warning(f"[DIFF] No table to compare; would create table {fq}.")
                return
            elif mode in ("apply", "all"):
                apply_loader_step(cfg, spark, dry_run, fq, action_desc="Full create")
                # --- AUDIT: Log CREATE (only if not dry run)
                if audit_table_yaml_path and not dry_run:
                    apply_audit_log_step(
                        spark,
                        audit_table_yaml_path,
                        diff={"table_created": True},
                        cfg=cfg,
                        env=env,
                    )
                return
            print_error("Unreachable: Table does not exist and mode is not 'diff', 'apply', or 'all'. Exiting.")
            sys.exit(1)
        else:
            # ---- Table exists: Diff, and maybe apply changes
            try:
                introspector = TableIntrospector(spark)
                raw_snap   = introspector.snapshot(fq)
                clean_snap = sanitize_snapshot(raw_snap)
                diff       = compute_diff(cfg, clean_snap)
            except Exception as e:
                print_error(f"Error during introspection or diff: {e}")
                sys.exit(2)

            if log_ddl:
                try:
                    log_comparison(yaml_path, cfg, fq, raw_snap, log_ddl, clean_snap=clean_snap)
                    print(f"{Color.b}{Color.ivory}Wrote comparison log to {Color.aqua_blue}{log_ddl}{Color.r}")
                except Exception as e:
                    print_error(f"Could not write diff log: {e}")

            if not any(diff.values()):
                print_success("No metadata changes detected; exiting cleanly. Everything is up to date.")
                return

            if mode == "diff":
                print_warning(f"[DIFF] Proposed changes for {fq}:")
                for k, v in diff.items():
                    if v:
                        print(f"{Color.b}{Color.aqua_blue}{k}:{Color.ivory} {v}{Color.r}")
                return

            # ---- DIFFERENCE VALIDATION (will exit if not allowed)
            validate_diff(cfg, clean_snap, diff)

            # ---- SCHEMA EVOLUTION PRE-FLIGHT (now in validators)
            handle_schema_evolution(diff, clean_snap["tbl_props"])

            if mode in ("apply", "all") and not dry_run:
                print(section_header("APPLYING METADATA CHANGES", color=Color.green))
                apply_loader_step(cfg, spark, dry_run, fq, action_desc="Updates applied")
                # --- AUDIT: Log UPDATE (only if not dry run)
                if audit_table_yaml_path:
                    apply_audit_log_step(
                        spark,
                        audit_table_yaml_path,
                        diff=diff,
                        cfg=cfg,
                        env=env,
                    )

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
        print_error("Usage: python -m layker <yaml_path> [env] [dry_run] [mode] [audit_table_yaml_path]")
        sys.exit(1)
    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    audit_table_yaml_path = sys.argv[5] if len(sys.argv) > 5 else None
    run_table_load(
        yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode, audit_table_yaml_path=audit_table_yaml_path
    )

if __name__ == "__main__":
    cli_entry()