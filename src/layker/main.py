# src/layker/main.py

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
import os
import sys
import yaml
import getpass

from pathlib import Path
import re

from layker.sanitizer import (
    recursive_sanitize_comments,
    sanitize_metadata,
    sanitize_snapshot,
)

from layker.introspector import TableIntrospector
from layker.differences import compute_diff
from layker.loader import DatabricksTableLoader
from layker.differences_logger import log_comparison
from layker.yaml import TableSchemaConfig
from layker.validators.params import validate_params
from layker.validators.yaml import TableYamlValidator
from layker.validators.evolution import (
    check_type_changes,
    check_delta_properties,
)
from layker.audit.logger import TableAuditLogger
from layker.utils.color import Color
from layker.utils.printer import (
    section_header,
    print_success,
    print_warning,
    print_error,
)
from layker.utils.table import table_exists

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIT_TABLE_YAML_PATH = os.path.join(REPO_ROOT, "layker", "audit", "layker_audit.yaml")
ACTOR = os.environ.get("USER") or getpass.getuser() or "unknown_actor"

def get_run_id():
    # Placeholder for run id logic; update to fetch Databricks job run id if needed
    return None

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit_log_table: Optional[str] = None,
) -> None:
    try:
        # initialize SparkSession if not provided
        if spark is None:
            print_warning("No SparkSession passed; starting a new one.")
            try:
                spark = SparkSession.builder.getOrCreate()
            except Exception as e:
                print_error(f"Could not start SparkSession: {e}")
                sys.exit(2)

        # PARAMETER VALIDATION
        mode, env, audit_log_table = validate_params(
            yaml_path, log_ddl, mode, env, audit_log_table, spark
        )

        # STEP 1: VALIDATE & SANITIZE YAML
        print(section_header("STEP 1/4: VALIDATING YAML"))
        try:
            ddl_cfg = TableSchemaConfig(yaml_path, env=env)
            raw_cfg = ddl_cfg._config
        except FileNotFoundError as e:
            print_error(f"YAML file not found: {e}")
            sys.exit(2)
        except yaml.YAMLError as e:
            print_error(f"YAML syntax error in {yaml_path}: {e}")
            sys.exit(2)
        except Exception as e:
            print_error(f"Error loading or parsing YAML: {e}")
            sys.exit(2)

        try:
            cfg = recursive_sanitize_comments(raw_cfg)
            cfg = sanitize_metadata(cfg)
        except Exception as e:
            print_error(f"Error sanitizing YAML: {e}")
            sys.exit(2)

        try:
            valid, errors = TableYamlValidator.validate_dict(cfg)
        except Exception as e:
            print_error(f"Validation crashed: {e}")
            sys.exit(2)

        if not valid:
            print_error("Validation failed:")
            for err in errors:
                print(f"    {Color.candy_red}- {err}{Color.r}")
            sys.exit(1)
        print_success("YAML validation passed.")

        if mode == "validate":
            print(f"{Color.b}{Color.ivory}Mode 'validate': validation complete. No further action taken.{Color.r}")
            return

        fq = ddl_cfg.full_table_name
        try:
            introspector = TableIntrospector(spark)
            loader       = DatabricksTableLoader(cfg, spark, dry_run=dry_run)
        except Exception as e:
            print_error(f"Could not initialize introspector or loader: {e}")
            sys.exit(2)

        # STEP 2: AUDIT LOGGER INIT
        print(section_header("STEP 2/4: AUDIT LOGGING CONFIGURATION", color=Color.sky_blue))
        if audit_log_table:
            print(f"{Color.b}{Color.ivory}Audit logging is {Color.green}ENABLED{Color.ivory}; using table: {Color.aqua_blue}{audit_log_table}{Color.r}")
            try:
                audit_logger = TableAuditLogger(
                    spark,
                    ddl_yaml_path=AUDIT_TABLE_YAML_PATH,
                    log_table=audit_log_table,
                    actor=ACTOR,
                )
            except FileNotFoundError as e:
                print_error(f"Audit log YAML missing: {e}")
                sys.exit(2)
            except yaml.YAMLError as e:
                print_error(f"Audit log YAML is malformed: {e}")
                sys.exit(2)
            except Exception as e:
                print_error(f"Could not initialize TableAuditLogger: {e}")
                sys.exit(2)
            run_id = get_run_id()
        else:
            print(f"{Color.b}{Color.yellow}Audit logging is DISABLED for this run.{Color.r}")
            audit_logger = None
            run_id = None

        # STEP 3: TABLE EXISTENCE
        print(section_header("STEP 3/4: TABLE EXISTENCE & DIFF"))
        try:
            exists = table_exists(spark, fq)
        except Exception as e:
            print_error(f"Could not check table existence: {e}")
            sys.exit(2)

        if not exists:
            print(f"{Color.b}{Color.ivory}Table {Color.aqua_blue}{fq}{Color.ivory} not found.{Color.r}")
            if mode == "diff":
                print_warning(f"[DIFF] Would create table {fq}.")
                return
            elif mode in ("apply", "all"):
                print(f"{Color.b}{Color.ivory}Performing full create of {Color.aqua_blue}{fq}{Color.ivory}.{Color.r}")
                try:
                    loader.create_or_update_table()
                    print_success(f"[SUCCESS] Full create of {fq} completed.")
                except Exception as e:
                    print_error(f"Could not create table: {e}")
                    sys.exit(2)
                # --- AUDIT: Log CREATE ---
                if audit_logger:
                    try:
                        audit_logger.log_changes(
                            diff={"table_created": True},
                            cfg=cfg,
                            fqn=fq,
                            env=env,
                            run_id=run_id,
                        )
                    except Exception as e:
                        print_error(f"Audit log failed after table create: {e}")
                print_success("Workflow complete. Table created and audit logged.")
                return

        # STEP 4: COMPARE & DIFF
        print(section_header("STEP 4/4: METADATA DIFF"))
        try:
            raw_snap   = introspector.snapshot(fq)
            clean_snap = sanitize_snapshot(raw_snap)
            diff       = compute_diff(cfg, clean_snap)
        except Exception as e:
            print_error(f"Error during introspection or diff: {e}")
            sys.exit(2)

        if log_ddl:
            try:
                # pass raw_snap instead of clean_snap for full introspector dump
                log_comparison(yaml_path, cfg, fq, raw_snap, log_ddl)
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

        if diff["type_changes"]:
            print_error("Type changes detected; in-place type change is not supported. Exiting.")
            sys.exit(1)

        schema_changes = (
            diff["added_columns"] or
            diff["dropped_columns"] or
            diff["renamed_columns"]
        )
        if schema_changes:
            print(section_header("SCHEMA EVOLUTION PRE-FLIGHT", color=Color.neon_green))
            try:
                check_type_changes(cfg, raw_snap)
                check_delta_properties(clean_snap["tbl_props"])
            except Exception as e:
                print_error(f"Pre-flight failed: {e}")
                sys.exit(2)
            print_success("Pre-flight checks passed.")
        else:
            print_warning("No schema-evolution needed; skipping pre-flight.")

        if mode in ("apply", "all"):
            print(section_header("APPLYING METADATA CHANGES", color=Color.green))
            try:
                loader.create_or_update_table()
                print_success(f"Updates for {fq} applied.")
            except Exception as e:
                print_error(f"Failed to apply updates: {e}")
                sys.exit(2)
            # --- AUDIT: Log UPDATE ---
            if audit_logger:
                try:
                    audit_logger.log_changes(
                        diff=diff,
                        cfg=cfg,
                        fqn=fq,
                        env=env,
                        run_id=run_id,
                    )
                    print_success("Workflow complete. Updates applied and audit logged.")
                except Exception as e:
                    print_error(f"Audit log failed after update: {e}")
                    print_warning("Workflow completed but audit logging failed.")

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
    audit_log_table = sys.argv[5] if len(sys.argv) > 5 else None
    run_table_load(
        yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode, audit_log_table=audit_log_table
    )

if __name__ == "__main__":
    cli_entry()