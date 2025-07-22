# src/layker/main.py

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
import sys
import getpass
import os

from layker.color import Color
from layker.sanitizer import (
    recursive_sanitize_comments,
    sanitize_metadata,
    sanitize_snapshot,
)
from layker.validator import TableYamlValidator
from layker.introspector import TableIntrospector
from layker.differences import compute_diff
from layker.loader import DatabricksTableLoader
from layker.differences_logger import log_comparison
from layker.yaml_reader import TableSchemaConfig
from layker.audit import TableAuditLogger

# ---- AUDIT CONFIG ----
# Use relative path based on the main.py file location
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIT_TABLE_YAML_PATH = os.path.join(REPO_ROOT, "layker", "schema", "load_table_log.yaml")
AUDIT_TABLE_FQN = "dq_dev.monitoring.load_table_log"   # Should match your audit log table
ACTOR = os.environ.get("USER") or getpass.getuser() or "unknown_actor"

def get_run_id():
    # Optionally implement Databricks context detection here
    return None  # Placeholder (set to None if not available)

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit: bool = True,  # <---- New parameter
) -> None:
    try:
        # --- Step 1: VALIDATE & SANITIZE YAML ---
        if spark is None:
            print(f"{Color.b}{Color.ivory}No SparkSession passed; starting one...{Color.b}")
            spark = SparkSession.builder.getOrCreate()

        print(f"{Color.b}{Color.aqua_blue}=== [Step 1/4] VALIDATING YAML ==={Color.b}")
        ddl_cfg = TableSchemaConfig(yaml_path, env=env)
        raw_cfg = ddl_cfg._config
        cfg     = recursive_sanitize_comments(raw_cfg)
        cfg     = sanitize_metadata(cfg)

        valid, errors = TableYamlValidator.validate_dict(cfg)
        if not valid:
            print(f"{Color.b}{Color.candy_red}Validation failed:{Color.b}")
            for err in errors:
                print(f"{Color.red}- {err}{Color.b}")
            sys.exit(1)
        print(f"{Color.b}{Color.green}YAML validation passed.{Color.b}")

        if mode == "validate":
            print(f"{Color.b}{Color.ivory}Mode 'validate': validation complete. No further action taken.{Color.b}")
            return

        fq = ddl_cfg.full_table_name
        introspector = TableIntrospector(spark)
        loader       = DatabricksTableLoader(cfg, spark, dry_run=dry_run)

        # ---- Init Audit Logger (once per run, only if audit enabled) ----
        if audit:
            audit_logger = TableAuditLogger(
                spark,
                ddl_yaml_path=AUDIT_TABLE_YAML_PATH,
                log_table=AUDIT_TABLE_FQN,
                actor=ACTOR,
            )
            run_id = get_run_id()
        else:
            audit_logger = None
            run_id = None

        print(f"{Color.b}{Color.aqua_blue}=== [Step 2/4] CHECKING TABLE EXISTENCE ==={Color.b}")
        table_exists = introspector.table_exists(fq)
        if not table_exists:
            print(f"{Color.b}{Color.ivory}Table {fq} not found.")
            if mode == "diff":
                print(f"{Color.b}{Color.ivory}[DIFF] Would create table {fq}.{Color.b}")
                return
            elif mode in ("apply", "all"):
                print(f"{Color.b}{Color.ivory}Performing full create of {fq}.{Color.b}")
                loader.create_or_update_table()
                print(f"{Color.b}{Color.green}[SUCCESS] Full create of {fq} completed.{Color.b}")
                # --- AUDIT: Log CREATE ---
                if audit_logger:
                    audit_logger.log_changes(
                        diff={"table_created": True},
                        cfg=cfg,
                        fqn=fq,
                        env=env,
                        run_id=run_id,
                    )
                return

        # --- Step 3: COMPARE & DIFF ---
        print(f"{Color.b}{Color.aqua_blue}=== [Step 3/4] COMPARE & DIFF ==={Color.b}")
        raw_snap   = introspector.snapshot(fq)
        clean_snap = sanitize_snapshot(raw_snap)      # <--- SANITIZE snapshot here
        diff       = compute_diff(cfg, clean_snap)    # <--- Pass sanitized

        if log_ddl:
            log_comparison(yaml_path, cfg, fq, clean_snap, log_ddl)
            print(f"{Color.b}{Color.ivory}Wrote comparison log to {log_ddl}{Color.b}")

        if not any(diff.values()):
            print(f"{Color.b}{Color.ivory}No metadata changes detected; exiting.{Color.b}")
            return

        if mode == "diff":
            print(f"{Color.b}{Color.ivory}[DIFF] Proposed changes for {fq}:{Color.b}")
            for k, v in diff.items():
                if v:
                    print(f"{Color.b}{Color.aqua_blue}{k}:{Color.b} {v}")
            return

        # --- Step 4: TYPE CHANGE CHECK & SCHEMA EVOLUTION ---
        if diff["type_changes"]:
            print(f"{Color.b}{Color.candy_red}Type changes detected; in-place type change is not supported. Exiting.{Color.b}")
            sys.exit(1)

        schema_changes = (
            diff["added_columns"] or
            diff["dropped_columns"] or
            diff["renamed_columns"]
        )
        if schema_changes:
            print(f"{Color.b}{Color.aqua_blue}=== [Step 4/4] SCHEMA EVOLUTION PRE-FLIGHT ==={Color.b}")
            try:
                TableYamlValidator.check_type_changes(cfg, raw_snap)
                TableYamlValidator.check_delta_properties(clean_snap["tbl_props"])
            except Exception as e:
                print(f"{Color.b}{Color.candy_red}Pre-flight failed:{Color.b}\n{Color.red}{e}{Color.b}")
                sys.exit(1)
            print(f"{Color.b}{Color.green}Pre-flight checks passed.{Color.b}")
        else:
            print(f"{Color.b}{Color.ivory}No schema-evolution needed; skipping pre-flight.{Color.b}")

        if mode in ("apply", "all"):
            print(f"{Color.b}{Color.aqua_blue}=== [Step 5/5] APPLYING METADATA CHANGES ==={Color.b}")
            loader.create_or_update_table()
            print(f"{Color.b}{Color.green}[SUCCESS] Updates for {fq} applied.{Color.b}")
            # --- AUDIT: Log UPDATE ---
            if audit_logger:
                audit_logger.log_changes(
                    diff=diff,
                    cfg=cfg,
                    fqn=fq,
                    env=env,
                    run_id=run_id,
                )

    except SystemExit:
        raise
    except Exception as e:
        print(f"{Color.b}{Color.candy_red}Fatal error during run_table_load:{Color.b}\n{Color.red}{e}{Color.b}")
        sys.exit(1)

def cli_entry():
    if len(sys.argv) < 2:
        print(f"{Color.b}{Color.red}Usage: python -m layker <yaml_path> [env] [dry_run] [mode] [audit]{Color.b}")
        sys.exit(1)
    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    audit    = True if len(sys.argv) < 6 else (sys.argv[5].lower() != "false")
    run_table_load(yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode, audit=audit)

if __name__ == "__main__":
    cli_entry()