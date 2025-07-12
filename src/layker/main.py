# src/layker/main.py

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
import sys

from layker import color
from layker.table_sanitizer import (
    recursive_sanitize_comments,
    sanitize_metadata,
    sanitize_snapshot,
)
from layker.table_validator import TableYamlValidator
from layker.table_introspector import TableIntrospector
from layker.table_differences import compute_diff
from layker.table_loader import DatabricksTableLoader
from layker.table_differences_logger import log_comparison
from layker.table_ddl_config import TableSchemaConfig

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
) -> None:
    try:
        # --- Step 1: VALIDATE & SANITIZE YAML ---
        if spark is None:
            print(f"{color.b}{color.ivory}No SparkSession passed; starting one...{color.b}")
            spark = SparkSession.builder.getOrCreate()

        print(f"{color.b}{color.aqua_blue}=== [Step 1/4] VALIDATING YAML ==={color.b}")
        ddl_cfg = TableSchemaConfig(yaml_path, env=env)
        raw_cfg = ddl_cfg._config
        cfg     = recursive_sanitize_comments(raw_cfg)
        cfg     = sanitize_metadata(cfg)

        valid, errors = TableYamlValidator.validate_dict(cfg)
        if not valid:
            print(f"{color.b}{color.candy_red}Validation failed:{color.b}")
            for err in errors:
                print(f"{color.red}- {err}{color.b}")
            sys.exit(1)
        print(f"{color.b}{color.green}YAML validation passed.{color.b}")

        if mode == "validate":
            print(f"{color.b}{color.ivory}Mode 'validate': validation complete. No further action taken.{color.b}")
            return

        fq = ddl_cfg.full_table_name
        introspector = TableIntrospector(spark)
        loader       = DatabricksTableLoader(cfg, spark, dry_run=dry_run)

        print(f"{color.b}{color.aqua_blue}=== [Step 2/4] CHECKING TABLE EXISTENCE ==={color.b}")
        table_exists = introspector.table_exists(fq)
        if not table_exists:
            print(f"{color.b}{color.ivory}Table {fq} not found.")
            if mode == "diff":
                print(f"{color.b}{color.ivory}[DIFF] Would create table {fq}.{color.b}")
                return
            elif mode in ("apply", "all"):
                print(f"{color.b}{color.ivory}Performing full create of {fq}.{color.b}")
                loader.create_or_update_table()
                print(f"{color.b}{color.green}[SUCCESS] Full create of {fq} completed.{color.b}")
                return

        # --- Step 3: COMPARE & DIFF ---
        print(f"{color.b}{color.aqua_blue}=== [Step 3/4] COMPARE & DIFF ==={color.b}")
        raw_snap   = introspector.snapshot(fq)
        clean_snap = sanitize_snapshot(raw_snap)
        diff       = compute_diff(cfg, raw_snap)

        if log_ddl:
            log_comparison(yaml_path, cfg, fq, clean_snap, log_ddl)
            print(f"{color.b}{color.ivory}Wrote comparison log to {log_ddl}{color.b}")

        if not any(diff.values()):
            print(f"{color.b}{color.ivory}No metadata changes detected; exiting.{color.b}")
            return

        if mode == "diff":
            print(f"{color.b}{color.ivory}[DIFF] Proposed changes for {fq}:{color.b}")
            for k, v in diff.items():
                if v:
                    print(f"{color.b}{color.aqua_blue}{k}:{color.b} {v}")
            return

        # --- Step 4: TYPE CHANGE CHECK & SCHEMA EVOLUTION ---
        if diff["type_changes"]:
            print(f"{color.b}{color.candy_red}Type changes detected; in-place type change is not supported. Exiting.{color.b}")
            sys.exit(1)

        schema_changes = (
            diff["added_columns"] or
            diff["dropped_columns"] or
            diff["renamed_columns"]
        )
        if schema_changes:
            print(f"{color.b}{color.aqua_blue}=== [Step 4/4] SCHEMA EVOLUTION PRE-FLIGHT ==={color.b}")
            try:
                TableYamlValidator.check_type_changes(cfg, raw_snap)
                TableYamlValidator.check_delta_properties(clean_snap["tbl_props"])
            except Exception as e:
                print(f"{color.b}{color.candy_red}Pre-flight failed:{color.b}\n{color.red}{e}{color.b}")
                sys.exit(1)
            print(f"{color.b}{color.green}Pre-flight checks passed.{color.b}")
        else:
            print(f"{color.b}{color.ivory}No schema-evolution needed; skipping pre-flight.{color.b}")

        if mode in ("apply", "all"):
            print(f"{color.b}{color.aqua_blue}=== [Step 5/5] APPLYING METADATA CHANGES ==={color.b}")
            loader.create_or_update_table()
            print(f"{color.b}{color.green}[SUCCESS] Updates for {fq} applied.{color.b}")

    except SystemExit:
        raise
    except Exception as e:
        print(f"{color.b}{color.candy_red}Fatal error during run_table_load:{color.b}\n{color.red}{e}{color.b}")
        sys.exit(1)

def cli_entry():
    if len(sys.argv) < 2:
        print(f"{color.b}{color.red}Usage: python -m layker <yaml_path> [env] [dry_run] [mode]{color.b}")
        sys.exit(1)
    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    run_table_load(yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode)

if __name__ == "__main__":
    cli_entry()