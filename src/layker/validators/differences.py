# src/layker/validators/differences.py

from typing import Any, Dict

def check_type_changes(cfg: Dict[str, Any], table_snapshot: Dict[str, Any]) -> None:
    """
    Prevents illegal in-place type changes (col name matches, type does not).
    Only call if table exists and there are schema changes.
    """
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