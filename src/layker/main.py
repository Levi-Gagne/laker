# src/layker/main.py

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
import sys
import getpass
import os

from layker.color import Color as C
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
from layker.audit.logger import TableAuditLogger

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUDIT_TABLE_YAML_PATH = os.path.join(REPO_ROOT, "layker", "audit", "LakerAuditLog.yaml")
ACTOR = os.environ.get("USER") or getpass.getuser() or "unknown_actor"

# --- Optional: Preformatted section headers for extra clarity (reuse freely) ---
def section_header(title, color=C.aqua_blue):
    bar = f"{color}{C.b}" + "═" * 62 + C.r
    title_line = f"{color}{C.b}║ {title.center(58)} ║{C.r}"
    return f"\n{bar}\n{title_line}\n{bar}"

def print_success(msg):
    print(f"{C.b}{C.green}✔ {msg}{C.r}")

def print_warning(msg):
    print(f"{C.b}{C.yellow}! {msg}{C.r}")

def print_error(msg):
    print(f"{C.b}{C.candy_red}✘ {msg}{C.r}")

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
        # STEP 1: VALIDATE & SANITIZE YAML
        if spark is None:
            print_warning("No SparkSession passed; starting a new one.")
            spark = SparkSession.builder.getOrCreate()

        print(section_header("STEP 1/4: VALIDATING YAML"))
        ddl_cfg = TableSchemaConfig(yaml_path, env=env)
        raw_cfg = ddl_cfg._config
        cfg     = recursive_sanitize_comments(raw_cfg)
        cfg     = sanitize_metadata(cfg)

        valid, errors = TableYamlValidator.validate_dict(cfg)
        if not valid:
            print_error("Validation failed:")
            for err in errors:
                print(f"    {C.candy_red}- {err}{C.r}")
            sys.exit(1)
        print_success("YAML validation passed.")

        if mode == "validate":
            print(f"{C.b}{C.ivory}Mode 'validate': validation complete. No further action taken.{C.r}")
            return

        fq = ddl_cfg.full_table_name
        introspector = TableIntrospector(spark)
        loader       = DatabricksTableLoader(cfg, spark, dry_run=dry_run)

        # STEP 2: AUDIT LOGGER INIT
        print(section_header("STEP 2/4: AUDIT LOGGING CONFIGURATION", color=C.sky_blue))
        if audit_log_table:
            print(f"{C.b}{C.ivory}Audit logging is {C.green}ENABLED{C.ivory}; using table: {C.aqua_blue}{audit_log_table}{C.r}")
            audit_logger = TableAuditLogger(
                spark,
                ddl_yaml_path=AUDIT_TABLE_YAML_PATH,
                log_table=audit_log_table,
                actor=ACTOR,
            )
            run_id = get_run_id()
        else:
            print(f"{C.b}{C.yellow}Audit logging is DISABLED for this run.{C.r}")
            audit_logger = None
            run_id = None

        # STEP 3: TABLE EXISTENCE
        print(section_header("STEP 3/4: TABLE EXISTENCE & DIFF"))
        table_exists = introspector.table_exists(fq)
        if not table_exists:
            print(f"{C.b}{C.ivory}Table {C.aqua_blue}{fq}{C.ivory} not found.{C.r}")
            if mode == "diff":
                print_warning(f"[DIFF] Would create table {fq}.")
                return
            elif mode in ("apply", "all"):
                print(f"{C.b}{C.ivory}Performing full create of {C.aqua_blue}{fq}{C.ivory}.{C.r}")
                loader.create_or_update_table()
                print_success(f"[SUCCESS] Full create of {fq} completed.")
                # --- AUDIT: Log CREATE ---
                if audit_logger:
                    audit_logger.log_changes(
                        diff={"table_created": True},
                        cfg=cfg,
                        fqn=fq,
                        env=env,
                        run_id=run_id,
                    )
                print_success("Workflow complete. Table created and audit logged.")
                return

        # STEP 4: COMPARE & DIFF
        print(section_header("STEP 4/4: METADATA DIFF"))
        raw_snap   = introspector.snapshot(fq)
        clean_snap = sanitize_snapshot(raw_snap)
        diff       = compute_diff(cfg, clean_snap)

        if log_ddl:
            log_comparison(yaml_path, cfg, fq, clean_snap, log_ddl)
            print(f"{C.b}{C.ivory}Wrote comparison log to {C.aqua_blue}{log_ddl}{C.r}")

        if not any(diff.values()):
            print_success("No metadata changes detected; exiting cleanly. Everything is up to date.")
            return

        if mode == "diff":
            print_warning(f"[DIFF] Proposed changes for {fq}:")
            for k, v in diff.items():
                if v:
                    print(f"{C.b}{C.aqua_blue}{k}:{C.ivory} {v}{C.r}")
            return

        # TYPE CHANGE CHECK & SCHEMA EVOLUTION
        if diff["type_changes"]:
            print_error("Type changes detected; in-place type change is not supported. Exiting.")
            sys.exit(1)

        schema_changes = (
            diff["added_columns"] or
            diff["dropped_columns"] or
            diff["renamed_columns"]
        )
        if schema_changes:
            print(section_header("SCHEMA EVOLUTION PRE-FLIGHT", color=C.neon_green))
            try:
                TableYamlValidator.check_type_changes(cfg, raw_snap)
                TableYamlValidator.check_delta_properties(clean_snap["tbl_props"])
            except Exception as e:
                print_error(f"Pre-flight failed: {e}")
                sys.exit(1)
            print_success("Pre-flight checks passed.")
        else:
            print_warning("No schema-evolution needed; skipping pre-flight.")

        if mode in ("apply", "all"):
            print(section_header("APPLYING METADATA CHANGES", color=C.green))
            loader.create_or_update_table()
            print_success(f"Updates for {fq} applied.")
            # --- AUDIT: Log UPDATE ---
            if audit_logger:
                audit_logger.log_changes(
                    diff=diff,
                    cfg=cfg,
                    fqn=fq,
                    env=env,
                    run_id=run_id,
                )
            print_success("Workflow complete. Updates applied and audit logged.")

    except SystemExit:
        raise
    except Exception as e:
        print_error(f"Fatal error during run_table_load:\n{e}")
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