# src/layker/main.py

"""
START: run_table_load  (decorated with @laker_banner → prints START/END banners + elapsed)

|
|-- 0) Normalize inputs & Spark session
|     |-- If `spark` is None → create with get_or_create_spark().
|     |-- validate_params(yaml_path, mode, env, audit_log_table, spark):
|           |-- Confirms YAML path is reachable and ends with .yml/.yaml.
|           |-- Ensures mode ∈ {"validate","diff","apply","all"} (defaults to "apply").
|           |-- Validates env characters.
|           |-- Validates `audit_log_table` ∈ {True, False, <.yml/.yaml path>}.
|           |-- Pings Spark (`spark.sql("SELECT 1")`) to ensure session is usable.
|           |-- Returns cleaned (mode, env, audit_log_table).
|
|-- 1) VALIDATE YAML & BUILD SNAPSHOT
|     |-- (a) validate_and_snapshot_yaml(yaml_path, env, mode):
|           |-- Fully validates YAML structure (top keys, columns 1..N, types, constraints, tags, etc.).
|           |-- Sanitizes comments/properties/tags.
|           |-- Builds `snapshot_yaml` (canonical DDL snapshot) and `fq` (catalog.schema.table).
|     |-- (b) If mode == "validate":
|           |-- Print success and EXIT 0.
|
|-- 2) TABLE SNAPSHOT (live)
|     |-- table_snapshot = TableSnapshot(spark, fq).build_table_metadata_dict()
|           |-- If table doesn’t exist → table_snapshot = None (signals a full CREATE).
|
|-- 3) COMPUTE DIFFERENCES
|     |-- (a) snapshot_diff = generate_differences(snapshot_yaml, table_snapshot)
|           |-- If table_snapshot is None → a complete “add” section for full CREATE.
|           |-- Else → only real deltas in {add | update | remove}.
|     |-- (b) No-op guard:
|           |-- has_changes(snapshot_diff) ?
|                 |-- False → print “No metadata changes …” and EXIT 0
|                       (prevents unnecessary load and prevents audit on no-ops).
|
|-- 4) VALIDATE DIFFERENCES (unified controller)
|     |-- validate_differences(snapshot_diff, table_snapshot)
|           |-- 4.1) Schema-evolution detection (only if table exists; i.e., table_snapshot != None):
|                 |-- Compute flags from `snapshot_diff`:
|                       add_column:
|                         - diff["add"]["columns"] has any index >= 2 with a truthy 'name'
|                         - AND index 1 NOT present (avoid full-create noise)
|                       rename_column:
|                         - any diff["update"]["columns"][idx]['name'] is truthy
|                       drop_column:
|                         - any diff["remove"]["columns"][idx]['name'] is truthy
|                 |-- If NONE of the above are true:
|                       - Print “No schema evolution changes detected; continuing.”
|                       - Proceed to step 5 (or step 6 if mode == "diff").
|                 |-- If ANY of the above are true (schema evolution intent detected):
|                       - Print a pre-flight header with flags (add/rename/drop).
|                       - Validate required Delta table properties from table_snapshot["table_properties"]:
|                             * delta.columnMapping.mode = "name"
|                             * delta.minReaderVersion   = "2"
|                             * delta.minWriterVersion   = "5"
|                       - If ANY required property missing/mismatched:
|                             * Print explicit error, EXIT 2 (do NOT apply changes, do NOT audit).
|                       - Else:
|                             * Print success (“properties present; proceeding”).
|           |-- 4.2) (Reserved) Other validations — currently a no-op.
|
|-- 5) DIFF-ONLY MODE QUICK EXIT
|     |-- If mode == "diff":
|           |-- Pretty-print the `snapshot_diff`.
|           |-- EXIT 0 (no load, no audit).
|
|-- 6) LOAD CHANGES (apply/all)
|     |-- Only if mode ∈ {"apply","all"} AND dry_run is False:
|           |-- Print “APPLYING METADATA CHANGES”.
|           |-- DatabricksTableLoader(snapshot_diff, spark, dry_run=False).run()
|                 |-- If `add.columns` starts at 1 and no opposing sections → CREATE TABLE with all columns,
|                     then post-create column comments/tags and table-level extras (owner, PK/UK, FKs, checks, tags).
|                 |-- Otherwise → ALTER: iterate over {add/update/remove} sections and emit SQL accordingly
|                     (comments/tags/checks/filters/properties/owner/etc.; columns handled per action).
|
|-- 7) AUDIT (only when there WERE changes)
|     |-- If audit_log_table is not False:
|           |-- If audit_log_table is True → default to "layker/resources/layker_audit.yaml".
|           |-- audit_log_flow(
|                 spark=spark,
|                 env=env,
|                 before_snapshot=table_snapshot,   # None on full create
|                 differences=snapshot_diff,         # persisted as JSON in audit row
|                 target_table_fq=fq,
|                 yaml_path=yaml_path,
|                 audit_table_yaml_path=audit_log_table,
|                 run_id=None, notes=None
|              )
|              |-- Ensures audit table exists from its YAML (creates it via loader if missing).
|              |-- Refresh target table if supported by the runtime; skip on serverless where REFRESH not allowed.
|              |-- Capture AFTER snapshot.
|              |-- Append one audit row with:
|                   • change_id (UUID)
|                   • run_id (optional)
|                   • env
|                   • yaml_path
|                   • fqn
|                   • change_category ("create" if before snapshot is None; else "update")
|                   • change_key (per-table sequence):
|                        - If CREATE:  "create-{n}"  (n = prior create count for this fqn + 1)
|                        - If UPDATE:  "create-{max_create}~update-{m}"
|                          (m = prior update count for this fqn + 1; max_create from this fqn’s history)
|                   • differences (JSON; exactly the `snapshot_diff`)
|                   • before_value (JSON; None on full create)
|                   • after_value  (JSON)
|                   • notes
|                   • created_at/by, updated_at/by
|     |-- Else:
|           |-- Print “Audit logging not enabled; exiting script.”
|
|-- END: run_table_load
|     |-- Normal completion after load/audit returns to caller.
|     |-- Early, intentional exits:
|           |-- EXIT 0 on: mode==validate, mode==diff, or no-op (no changes).
|           |-- EXIT 2 on: schema-evolution pre-flight failure (missing required Delta props).
|           |-- EXIT 130 on: KeyboardInterrupt.
|           |-- EXIT 1 on: unexpected exception (after printing traceback).
"""

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
    laker_banner,          # H1 decorator (start/end + timing)
    section_header,        # H2
    subsection_header,     # H3
    print_success,
    print_warning,
    print_error,
)

def _has_changes(diff: Dict[str, Any]) -> bool:
    # diff is considered meaningful only if add/update/remove present
    return any(k in diff and diff[k] for k in ("add", "update", "remove"))

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

        # ----- STEP 1/5 -----
        print(section_header("STEP 1/5: VALIDATING YAML"))
        snapshot_yaml, fq = validate_and_snapshot_yaml(yaml_path, env=env, mode=mode)
        if mode == "validate":
            print_success("YAML validation passed.")
            sys.exit(0)

        # ----- STEP 2/5 -----
        print(section_header("STEP 2/5: TABLE SNAPSHOT"))
        table_snapshot = TableSnapshot(spark, fq).build_table_metadata_dict()

        # ----- STEP 3/5 -----
        print(section_header("STEP 3/5: COMPUTE DIFFERENCES"))
        snapshot_diff = generate_differences(snapshot_yaml, table_snapshot)
        if not _has_changes(snapshot_diff):
            print_success("No metadata changes detected; exiting cleanly. Everything is up to date.")
            sys.exit(0)

        validate_differences(snapshot_diff, table_snapshot)

        if mode == "diff":
            print_warning(f"[DIFF] Proposed changes:")
            for k, v in snapshot_diff.items():
                if v:
                    print(f"{Color.b}{Color.aqua_blue}{k}:{Color.ivory} {v}{Color.r}")
            sys.exit(0)

        # ----- STEP 4/5 -----
        print(section_header("STEP 4/5: LOAD TABLE"))
        if mode in ("apply", "all") and not dry_run:
            # Sub-header for the actual apply work
            print(subsection_header("APPLYING METADATA CHANGES"))
            DatabricksTableLoader(snapshot_diff, spark, dry_run=dry_run).run()

            # ----- STEP 5/5 -----
            if audit_log_table is not False:
                print(section_header("STEP 5/5: LOG TABLE UPDATE"))
                if audit_log_table is True:
                    audit_log_table = "layker/resources/layker_audit.yaml"

                audit_log_flow(
                    spark=spark,
                    env=env,
                    before_snapshot=table_snapshot,   # may be None on full create
                    differences=snapshot_diff,         # pass the diff dict
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