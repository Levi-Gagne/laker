"""
table_sanitizer.py
------------------
Module‐level functions for:
  - cleaning YAML-driven table definitions
  - normalizing table snapshots for comparison
"""

from typing import Any, Dict

def sanitize_text(text: Any) -> str:
    t = str(text or "")
    clean = t.replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()
    return clean.replace("'", "`")

def recursive_sanitize_comments(obj: Any, path: str = "") -> Any:
    if isinstance(obj, dict):
        for k, v in obj.items():
            curr = f"{path}.{k}" if path else k
            if path.endswith(".columns") and isinstance(v, dict) and "comment" in v:
                if isinstance(v["comment"], str):
                    v["comment"] = sanitize_text(v["comment"])
            else:
                recursive_sanitize_comments(v, curr)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            recursive_sanitize_comments(item, f"{path}[{i}]")
    return obj

def sanitize_metadata(cfg: Dict[str, Any]) -> Dict[str, Any]:
    props = cfg.setdefault("properties", {})

    # preserve multiline table comment structure (trim per-line)
    if "comment" in props:
        raw = props["comment"] or ""
        lines = str(raw).splitlines()
        props["comment"] = "\n".join(line.strip() for line in lines)

    tbl_props = props.setdefault("table_properties", {})
    for k in list(tbl_props):
        tbl_props[k] = sanitize_text(tbl_props[k])

    tags = cfg.setdefault("tags", {})
    for k in list(tags):
        tags[k] = sanitize_text(tags[k])

    # sanitize row filters (table-level RLS)
    if "row_filters" in cfg:
        for rf in cfg["row_filters"].values():
            if "name" in rf:
                rf["name"] = sanitize_text(rf["name"])
            if "expression" in rf:
                rf["expression"] = sanitize_text(rf["expression"])

    # sanitize table-level check constraints
    if "table_check_constraints" in cfg:
        for c in cfg["table_check_constraints"].values():
            if "name" in c:
                c["name"] = sanitize_text(c["name"])
            if "expression" in c:
                c["expression"] = sanitize_text(c["expression"])

    return cfg

def normalize_dict(d: Dict[Any, Any]) -> Dict[str, str]:
    return {str(k).lower(): sanitize_text(v) for k, v in d.items()}

TYPE_SYNONYMS = {
    "long":      "bigint",
    "bigint":    "bigint",
    "int":       "int",
    "integer":   "int",
    "double":    "double",
    "float":     "float",
    "string":    "string",
    "boolean":   "boolean",
    "timestamp": "timestamp",
    "date":      "date",
}

def sanitize_snapshot(snap: Dict[str, Any]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}

    # columns & types
    clean["columns"] = [
        (name, TYPE_SYNONYMS.get(dtype.lower(), dtype.lower()))
        for name, dtype in snap["columns"]
    ]

    # column comments
    clean["comments"] = {
        name: sanitize_text(comment)
        for name, comment in snap["comments"].items()
    }

    # column tags
    clean["col_tags"] = {
        name: normalize_dict(tags)
        for name, tags in snap["col_tags"].items()
    }

    clean["tbl_tags"]  = normalize_dict(snap["tbl_tags"])
    clean["tbl_props"] = normalize_dict(snap["tbl_props"])

    clean["tbl_comment"] = snap.get("tbl_comment", "")

    # Table-level check constraints
    clean["tbl_constraints"] = {
        name: {"expression": sanitize_text(data.get("expression", ""))}
        for name, data in snap.get("tbl_constraints", {}).items()
    }

    # Column-level: add masking, defaults, variable_values, col constraints (NEW)
    # NOTE: this block is for completeness/future diffing logic; 
    # loader may not use them yet, but you wanted apples-to-apples capability

    if "columns" in snap and isinstance(snap["columns"], list):
        clean["col_masking_rules"] = {}
        clean["col_default_values"] = {}
        clean["col_variable_values"] = {}
        clean["col_col_constraints"] = {}

        for name, _ in snap["columns"]:
            # You must collect from your actual sources; here we just pass through if present
            # (You may need to extend introspector to capture these if you want actual diffs)
            clean["col_masking_rules"][name] = sanitize_text(
                snap.get("col_masking_rules", {}).get(name, "")
            )
            clean["col_default_values"][name] = sanitize_text(
                snap.get("col_default_values", {}).get(name, "")
            )
            clean["col_variable_values"][name] = sanitize_text(
                snap.get("col_variable_values", {}).get(name, "")
            )
            # Col-level constraints: dict of {constraint_name: {"expression": ...}}
            ccs = snap.get("col_col_constraints", {}).get(name, {})
            clean["col_col_constraints"][name] = {
                cname: {"expression": sanitize_text(cdict.get("expression", ""))}
                for cname, cdict in ccs.items()
            }

    return clean