# src/layker/validators/differences.py

import sys
from typing import Any, Dict, Tuple

from layker.utils.color import Color
from layker.utils.printer import (
    section_header,
    print_error,
    print_success,
    print_warning,
)

# =====================================================================================
# Unified validator entrypoint
#   - validate_differences(diff, table_snapshot)
#       * orchestrates all diff-related validations (schema evolution + future checks)
# =====================================================================================

REQUIRED_DELTA_SCHEMA_EVOLUTION_PROPERTIES: Dict[str, str] = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}

# ---------- helpers ----------

def _as_int_keys(d: Dict[Any, Any]) -> Dict[int, Any]:
    out: Dict[int, Any] = {}
    for k, v in (d or {}).items():
        try:
            out[int(k)] = v
        except Exception:
            continue
    return out

def _detect_schema_evolution(diff: Dict[str, Any], table_exists: bool) -> Tuple[bool, Dict[str, bool]]:
    """
    Detect schema-evolution intents from the snapshot diff.

    Rules:
      - ADD column: diff['add']['columns'] has ANY index >= 2 with a truthy 'name'
                    AND index 1 is NOT present (to avoid full-create noise).
      - RENAME column: any diff['update']['columns'][idx]['name'] is truthy.
      - DROP column: any diff['remove']['columns'][idx]['name'] is truthy.

    If table does not exist (full create), we do NOT treat it as evolution.
    """
    if not table_exists:
        return False, {"add_column": False, "rename_column": False, "drop_column": False}

    add_cols = _as_int_keys((diff.get("add") or {}).get("columns") or {})
    upd_cols = _as_int_keys((diff.get("update") or {}).get("columns") or {})
    rem_cols = _as_int_keys((diff.get("remove") or {}).get("columns") or {})

    add_column = (1 not in add_cols) and any(
        (idx >= 2) and bool((col or {}).get("name"))
        for idx, col in add_cols.items()
    )
    rename_column = any(bool((col or {}).get("name")) for col in upd_cols.values())
    drop_column   = any(bool((col or {}).get("name")) for col in rem_cols.values())

    any_change = add_column or rename_column or drop_column
    return any_change, {
        "add_column": add_column,
        "rename_column": rename_column,
        "drop_column": drop_column,
    }

def _check_required_delta_props(tbl_props: Dict[str, Any]) -> None:
    """Raise ValueError if any required property is missing/incorrect."""
    missing = {
        k: v for k, v in REQUIRED_DELTA_SCHEMA_EVOLUTION_PROPERTIES.items()
        if str(tbl_props.get(k, "")).strip() != v
    }
    if missing:
        want = ", ".join(f"{k}={v}" for k, v in missing.items())
        raise ValueError(f"Schema evolution requires these Delta table properties: {want}")

# ---------- sub-process: schema evolution preflight ----------

def _run_schema_evolution_checks(diff: Dict[str, Any], table_snapshot: Dict[str, Any] | None) -> None:
    """
    If evolution ops are present (on an existing table), verify Delta properties.
    On failure, print and exit(2). Otherwise print success and continue.
    """
    table_exists = table_snapshot is not None
    has_evo, flags = _detect_schema_evolution(diff, table_exists)

    if not has_evo:
        print_success("No schema evolution changes detected; continuing.")
        return

    print(section_header("SCHEMA EVOLUTION PRE-FLIGHT", color=Color.neon_green))
    print_warning(
        "Schema evolution changes detected "
        f"(add_column={flags['add_column']}, rename_column={flags['rename_column']}, drop_column={flags['drop_column']})."
    )

    try:
        tbl_props = (table_snapshot or {}).get("table_properties", {}) or {}
        _check_required_delta_props(tbl_props)
    except Exception as e:
        print_error(f"Required Delta properties not present: {e}")
        sys.exit(2)

    print_success("Schema evolution properties present; proceeding.")

# ---------- sub-process: other validations (placeholder for future rules) ----------

def _run_other_validations(diff: Dict[str, Any], table_snapshot: Dict[str, Any] | None) -> None:
    """
    Add additional non-evolution validations here in the future.
    Intentionally a no-op for now.
    """
    return

# ---------- controller ----------

def validate_differences(diff: Dict[str, Any], table_snapshot: Dict[str, Any] | None) -> None:
    """
    Unified controller for diff validations. Add new subprocess calls here.
    """
    _run_schema_evolution_checks(diff, table_snapshot)
    _run_other_validations(diff, table_snapshot)