# src/layker/main.py

import os
import sys
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

# --- Steps / Components ---
from layker.snapshot_yaml import validate_and_snapshot_yaml
from layker.snapshot_table import TableSnapshot
from layker.differences import generate_differences
from layker.loader import DatabricksTableLoader
from layker.logger import audit_log_flow

# --- Validators ---
from layker.validators.params import validate_params
from layker.validators.differences import validate_differences

# --- Utils ---
from layker.utils.spark import get_or_create_spark
from layker.utils.color import Color
from layker.utils.printer import (
    section_header,
    print_success,
    print_warning,
    print_error,
    layker_banner,
)

def _has_changes(diff: Dict[str, Any]) -> bool:
    # Treat as "no changes" if add/update/remove missing or empty
    for k in ("add", "update", "remove"):
        v = diff.get(k)
        if isinstance(v, dict) and len(v) > 0:
            return True
    return False

@laker_banner("Run Table Load")
def run_table_load(
    yaml_path: str,
    dry_run: bool = False,
    spark: Optional[SparkSession] = None,
    env: Optional[str] = None,
    mode: str = "apply",
    audit_log_table: Any = False,  # True (use default YAML), False (disable), or str path to YAML
) -> None:
    try:
        if spark is None:
            spark = get_or_create_spark()

        mode, env, audit_log_table = validate_params(
            yaml_path, mode, env, audit_log_table, spark
        )

        print(section_header("STEP 1/4: VALIDATING YAML"))
        # Returns (snapshot_yaml, fully_qualified_table)
        snapshot_yaml, fq = validate_and_snapshot_yaml(yaml_path, env=env, mode=mode)
        if mode == "validate":
            print_success("YAML validation passed.")
            sys.exit(0)

        print(section_header("STEP 2/5: TABLE SNAPSHOT"))
        # Build live table snapshot (None if table doesn't exist)
        table_snapshot = TableSnapshot(spark, fq).build_table_metadata_dict()

        print(section_header("STEP 3/5: COMPUTE DIFFERENCES"))
        # Compute differences using YAML snapshot + table snapshot (None => full create)
        snapshot_diff = generate_differences(snapshot_yaml, table_snapshot)
        # If no changes, exit early (prevents logging on no-ops)
        if not _has_changes(snapshot_diff):
            print_success("No metadata changes detected; exiting cleanly. Everything is up to date.")
            sys.exit(0)

        validate_differences(snapshot_diff, table_snapshot)

        # If mode==diff, print proposed changes and exit
        if mode == "diff":
            print_warning(f"[DIFF] Proposed changes:")
            for k, v in snapshot_diff.items():
                if v:
                    print(f"{Color.b}{Color.aqua_blue}{k}:{Color.ivory} {v}{Color.r}")
            sys.exit(0)

        print(section_header("STEP 4/5: LOAD TABLE"))
        # Apply changes for apply/all (single path for both create/alter)
        if mode in ("apply", "all") and not dry_run:
            print(section_header("APPLYING METADATA CHANGES", color=Color.green))
            DatabricksTableLoader(snapshot_diff, spark, dry_run=dry_run).run()

            # --- Audit after the load (resolve parameter in-place) ---
            if audit_log_table is not False:
                print(section_header("STEP 5/5: LOG TABLE UPDATE"))
                if audit_log_table is True:
                    # Inline default path
                    audit_log_table = "layker/resources/layker_audit.yaml"
                # else it's a user-supplied YAML path string

                audit_log_flow(
                    spark=spark,
                    env=env,
                    before_snapshot=table_snapshot,   # may be None on full create
                    differences=snapshot_diff,         # <-- pass the diff dict
                    target_table_fq=fq,
                    yaml_path=yaml_path,
                    audit_table_yaml_path=audit_log_table,
                    run_id=None,
                    notes=None,
                    snapshot_format="json_pretty",
                )
            else:
                print_success("Table loaded. Audit logging not enabled; exiting script.")
            return

        # If we got here with other modes, fall through to success
        print_success("Completed without applying changes.")

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
        yaml_path, dry_run=dry_run, env=env, mode=mode, audit_log_table=audit_log_table
    )

if __name__ == "__main__":
    cli_entry()