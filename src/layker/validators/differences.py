# src/layker/validators/differences.py

from typing import Any, Dict
from layker.utils.printer import print_error

def check_type_changes(cfg: Dict[str, Any], table_snapshot: Dict[str, Any]) -> None:
    yaml_cols = [
        (c["name"], c["datatype"].lower())
        for c in cfg["columns"].values()
        if c.get("active", True)
    ]
    table_cols = table_snapshot["columns"]
    for ((y_name, y_type), (t_name, t_type)) in zip(yaml_cols, table_cols):
        if y_name == t_name and y_type != t_type:
            raise ValueError(
                f"Type mismatch for column '{y_name}': table={t_type} vs yaml={y_type}"
            )

def check_no_illegal_drops(diff: Dict[str, Any]) -> None:
    dropped = diff.get("dropped_columns", [])
    if dropped:
        raise ValueError(
            "Detected columns present in the table but missing from the YAML (dropped columns). "
            "Columns must be set inactive in YAML, not physically dropped."
        )

def validate_diff(cfg: Dict[str, Any], table_snapshot: Dict[str, Any], diff: Dict[str, Any]) -> None:
    """
    Runs all difference validations in sequence. Exits on first validation failure.
    """
    try:
        check_type_changes(cfg, table_snapshot)
        check_no_illegal_drops(diff)
    except Exception as e:
        print_error(f"Invalid difference detected: {e}")
        import sys
        sys.exit(1)