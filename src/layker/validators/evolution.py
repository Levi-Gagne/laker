# src/layker/validators/evolution.py

from typing import Any, Dict

REQUIRED_DELTA_SCHEMA_EVOLUTION_PROPERTIES: Dict[str, str] = {
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}

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