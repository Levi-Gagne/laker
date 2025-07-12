# src/utils/load_table/main.py

import sys
import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

# Ensure “src” (the directory containing “utils”) is on PYTHONPATH
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )
)

from utils.colorConfig import Color as C
from utils.load_table.table_sanitizer import (
    recursive_sanitize_comments,
    sanitize_metadata,
    sanitize_snapshot,
)
from utils.load_table.table_validator import TableYamlValidator
from utils.load_table.table_introspector import TableIntrospector
from utils.load_table.table_differences import compute_diff
from utils.load_table.table_loader import DatabricksTableLoader
from utils.load_table.table_differences_logger import log_comparison
from utils.load_table.table_ddl_config import TableSchemaConfig

def run_table_load(
    yaml_path: str,
    log_ddl: Optional[str] = None,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",  # <-- New parameter
) -> None:
    try:
        # --- Step 1: VALIDATE & SANITIZE YAML ---
        if spark is None:
            print(f"{C.b}{C.ivory}No SparkSession passed; starting one...{C.b}")
            spark = SparkSession.builder.getOrCreate()

        print(f"{C.b}{C.aqua_blue}=== [Step 1/4] VALIDATING YAML ==={C.b}")
        ddl_cfg = TableSchemaConfig(yaml_path, env=env)
        raw_cfg = ddl_cfg._config
        cfg     = recursive_sanitize_comments(raw_cfg)
        cfg     = sanitize_metadata(cfg)

        valid, errors = TableYamlValidator.validate_dict(cfg)
        if not valid:
            print(f"{C.b}{C.candy_red}Validation failed:{C.b}")
            for err in errors:
                print(f"{C.red}- {err}{C.b}")
            sys.exit(1)
        print(f"{C.b}{C.green}YAML validation passed.{C.b}")

        if mode == "validate":
            print(f"{C.b}{C.ivory}Mode 'validate': validation complete. No further action taken.{C.b}")
            return

        fq = ddl_cfg.full_table_name
        introspector = TableIntrospector(spark)
        loader       = DatabricksTableLoader(cfg, spark, dry_run=dry_run)

        print(f"{C.b}{C.aqua_blue}=== [Step 2/4] CHECKING TABLE EXISTENCE ==={C.b}")
        table_exists = introspector.table_exists(fq)
        if not table_exists:
            print(f"{C.b}{C.ivory}Table {fq} not found.")
            if mode == "diff":
                print(f"{C.b}{C.ivory}[DIFF] Would create table {fq}.{C.b}")
                return
            elif mode in ("apply", "all"):
                print(f"{C.b}{C.ivory}Performing full create of {fq}.{C.b}")
                loader.create_or_update_table()
                print(f"{C.b}{C.green}[SUCCESS] Full create of {fq} completed.{C.b}")
                return

        # --- Step 3: COMPARE & DIFF ---
        print(f"{C.b}{C.aqua_blue}=== [Step 3/4] COMPARE & DIFF ==={C.b}")
        raw_snap   = introspector.snapshot(fq)
        clean_snap = sanitize_snapshot(raw_snap)
        diff       = compute_diff(cfg, raw_snap)

        if log_ddl:
            log_comparison(yaml_path, cfg, fq, clean_snap, log_ddl)
            print(f"{C.b}{C.ivory}Wrote comparison log to {log_ddl}{C.b}")

        if not any(diff.values()):
            print(f"{C.b}{C.ivory}No metadata changes detected; exiting.{C.b}")
            return

        if mode == "diff":
            print(f"{C.b}{C.ivory}[DIFF] Proposed changes for {fq}:{C.b}")
            # Pretty-print diff if you have a helper, else raw
            for k, v in diff.items():
                if v:
                    print(f"{C.b}{C.aqua_blue}{k}:{C.b} {v}")
            return

        # --- Step 4: TYPE CHANGE CHECK & SCHEMA EVOLUTION ---
        if diff["type_changes"]:
            print(f"{C.b}{C.candy_red}Type changes detected; in-place type change is not supported. Exiting.{C.b}")
            sys.exit(1)

        schema_changes = (
            diff["added_columns"] or
            diff["dropped_columns"] or
            diff["renamed_columns"]
        )
        if schema_changes:
            print(f"{C.b}{C.aqua_blue}=== [Step 4/4] SCHEMA EVOLUTION PRE-FLIGHT ==={C.b}")
            try:
                TableYamlValidator.check_type_changes(cfg, raw_snap)
                TableYamlValidator.check_delta_properties(clean_snap["tbl_props"])
            except Exception as e:
                print(f"{C.b}{C.candy_red}Pre-flight failed:{C.b}\n{C.red}{e}{C.b}")
                sys.exit(1)
            print(f"{C.b}{C.green}Pre-flight checks passed.{C.b}")
        else:
            print(f"{C.b}{C.ivory}No schema-evolution needed; skipping pre-flight.{C.b}")

        if mode in ("apply", "all"):
            print(f"{C.b}{C.aqua_blue}=== [Step 5/5] APPLYING METADATA CHANGES ==={C.b}")
            loader.create_or_update_table()
            print(f"{C.b}{C.green}[SUCCESS] Updates for {fq} applied.{C.b}")

    except SystemExit:
        raise
    except Exception as e:
        print(f"{C.b}{C.candy_red}Fatal error during run_table_load:{C.b}\n{C.red}{e}{C.b}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"{C.b}{C.red}Usage: python main.py <yaml_path> [env] [dry_run] [mode]{C.b}")
        sys.exit(1)

    yaml_path = sys.argv[1]
    env      = sys.argv[2] if len(sys.argv) > 2 else None
    dry_run  = (len(sys.argv) > 3 and sys.argv[3].lower() == "true")
    mode     = sys.argv[4] if len(sys.argv) > 4 else "apply"
    run_table_load(yaml_path, log_ddl=None, dry_run=dry_run, env=env, mode=mode)