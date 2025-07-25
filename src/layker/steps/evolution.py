# src/layker/steps/evolution.py

import sys
from typing import Any, Dict
from layker.utils.color import Color
from layker.utils.printer import print_error, print_success, print_warning

REQUIRED_DELTA_SCHEMA_EVOLUTION_PROPERTIES: Dict[str, str] = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}

def check_delta_properties(tbl_props: Dict[str, Any]) -> None:
    """
    Require the necessary Delta properties for schema evolution.
    Raise ValueError if any are missing/incorrect.
    Only call if schema evolution is detected.
    """
    missing = {
        k: v for k, v in REQUIRED_DELTA_SCHEMA_EVOLUTION_PROPERTIES.items()
        if tbl_props.get(k) != v
    }
    if missing:
        msg = ", ".join(f"{k}={v}" for k, v in missing.items())
        raise ValueError(
            f"Schema evolution requires these Delta table properties: {msg}"
        )

def validate_schema_evolution(tbl_props: Dict[str, Any]) -> None:
    """
    Prints pre-flight checks and validates schema evolution preconditions.
    Exits on failure.
    """
    print(section_header("SCHEMA EVOLUTION PRE-FLIGHT", color=Color.neon_green))
    try:
        check_delta_properties(tbl_props)
    except Exception as e:
        print_error(f"Pre-flight failed: {e}")
        sys.exit(2)
    print_success("Pre-flight checks passed.")

def handle_schema_evolution(diff: Dict[str, Any], tbl_props: Dict[str, Any]) -> None:
    """
    Helper for main: checks if schema changes exist and runs pre-flight evolution checks.
    Prints and exits as needed.
    """
    schema_changes = (
        diff["added_columns"] or
        diff["dropped_columns"] or
        diff["renamed_columns"]
    )
    if schema_changes:
        validate_schema_evolution(tbl_props)
    else:
        print_warning("No schema-evolution needed; skipping pre-flight.")
