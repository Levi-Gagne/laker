# src/layker/differences.py

from typing import Any, Dict, List, Tuple
import yaml
from pathlib import Path

def normalize_dict(d: Dict[Any, Any]) -> Dict[str, str]:
    return {str(k).lower(): str(v) for k, v in d.items()}

def compute_diff(
    cfg: Dict[str, Any],
    snap: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compute metadata diffs between sanitized YAML cfg and a sanitized table snapshot.
    """
    # YAML side
    cols = [c for c in cfg["columns"].values() if c.get("active", True)]
    yaml_cols     = [(c["name"], c["datatype"].lower()) for c in cols]
    yaml_comments = {c["name"]: c.get("comment", "") for c in cols}
    yaml_ctags    = {c["name"]: c.get("tags", {})    for c in cols}
    yaml_col_checks = {c["name"]: c.get("column_check_constraints", {}) for c in cols}

    # Table side (already sanitized)
    snap_cols     = snap["columns"]
    snap_comments = snap["comments"]
    snap_ctags    = snap["col_tags"]
    snap_col_checks = snap.get("column_check_constraints", {})  # expects snapshot to support this
    snap_props    = snap["tbl_props"]
    snap_tags     = snap["tbl_tags"]
    snap_tcomm    = snap["tbl_comment"]

    # --- NEW: Table-level features from YAML ---
    yaml_unique_keys    = cfg.get("unique_keys", [])
    yaml_foreign_keys   = cfg.get("foreign_keys", {})
    yaml_tbl_checks     = cfg.get("table_check_constraints", {})
    yaml_row_filters    = cfg.get("row_filters", {})

    # Table snapshot equivalents (if available, else default empty)
    snap_unique_keys   = snap.get("unique_keys", [])
    snap_foreign_keys  = snap.get("foreign_keys", {})
    snap_tbl_checks    = snap.get("tbl_constraints", {})
    snap_row_filters   = snap.get("row_filters", {})

    # 1) Columns added/dropped
    snap_names = [n for n, _ in snap_cols]
    yaml_names = [n for n, _ in yaml_cols]
    added_columns   = [(n, t) for n, t in yaml_cols if n not in snap_names]
    dropped_columns = [n for n in snap_names if n not in yaml_names]

    # 2) Renames & type-changes (by position)
    renamed_columns: List[Tuple[str, str]] = []
    type_changes:      List[Tuple[str, str, str]] = []
    for (y, s) in zip(yaml_cols, snap_cols):
        y_n, y_t = y
        s_n, s_t = s
        if y_n != s_n:
            if y_t == s_t:
                renamed_columns.append((s_n, y_n))
            else:
                type_changes.append((s_n, s_t, y_t))

    # 3) Table-property diffs (only YAML keys)
    property_changes: List[Tuple[str, Any, Any]] = []
    for k, yv in cfg.get("properties", {}).get("table_properties", {}).items():
        sv = snap_props.get(k)
        if yv != sv:
            property_changes.append((k, sv, yv))

    # 4) Table-tag diffs (only YAML keys)
    table_tag_changes: List[Tuple[str, Any, Any]] = []
    for k, yv in cfg.get("tags", {}).items():
        sv = snap_tags.get(k)
        if yv != sv:
            table_tag_changes.append((k, sv, yv))

    # 5) Column-tag diffs
    column_tag_changes: List[Tuple[str, str, Any, Any]] = []
    for col, ytags in yaml_ctags.items():
        norm_yt = normalize_dict(ytags)
        stags   = snap_ctags.get(col, {})
        for k, yv in norm_yt.items():
            sv = stags.get(k)
            if str(yv) != str(sv):
                column_tag_changes.append((col, k, sv, yv))

    # 6) Column-comment diffs
    column_comment_changes: List[Tuple[str, Any, Any]] = []
    for col, yv in yaml_comments.items():
        sv = snap_comments.get(col, "")
        if yv != sv:
            column_comment_changes.append((col, sv, yv))

    # 7) Table-comment diffs
    table_comment_change = None
    yaml_tcomm = cfg.get("properties", {}).get("comment", "")
    if yaml_tcomm != snap_tcomm:
        table_comment_change = (snap_tcomm, yaml_tcomm)

    # --- NEW: Unique Key diffs ---
    unique_key_changes = []
    if yaml_unique_keys != snap_unique_keys:
        unique_key_changes.append(("unique_keys", snap_unique_keys, yaml_unique_keys))

    # --- NEW: Foreign Key diffs ---
    foreign_key_changes = []
    for fk, yv in yaml_foreign_keys.items():
        sv = snap_foreign_keys.get(fk)
        if yv != sv:
            foreign_key_changes.append((fk, sv, yv))

    # --- NEW: Table-level check constraint diffs (union logic) ---
    all_cnames = set(yaml_tbl_checks.keys()).union(set(snap_tbl_checks.keys()))
    table_check_constraint_changes = []
    for cname in all_cnames:
        yv = yaml_tbl_checks.get(cname)
        sv = snap_tbl_checks.get(cname)
        if yv != sv:
            table_check_constraint_changes.append((cname, sv, yv))

    # --- NEW: Row filter diffs ---
    row_filter_changes = []
    for fname, yv in yaml_row_filters.items():
        sv = snap_row_filters.get(fname)
        if yv != sv:
            row_filter_changes.append((fname, sv, yv))

    # --- NEW: Column-level check constraint diffs ---
    column_check_constraint_changes = []
    for col, y_checks in yaml_col_checks.items():
        s_checks = snap_col_checks.get(col, {}) if snap_col_checks else {}
        for ck, yv in y_checks.items():
            sv = s_checks.get(ck)
            if yv != sv:
                column_check_constraint_changes.append((col, ck, sv, yv))

    return {
        "added_columns":           added_columns,
        "dropped_columns":         dropped_columns,
        "renamed_columns":         renamed_columns,
        "type_changes":            type_changes,
        "property_changes":        property_changes,
        "table_tag_changes":       table_tag_changes,
        "column_tag_changes":      column_tag_changes,
        "column_comment_changes":  column_comment_changes,
        "table_comment_change":    table_comment_change,

        "unique_key_changes":      unique_key_changes,
        "foreign_key_changes":     foreign_key_changes,
        "table_check_constraint_changes": table_check_constraint_changes,
        "row_filter_changes":      row_filter_changes,
        "column_check_constraint_changes": column_check_constraint_changes,
    }

def log_comparison(
    yaml_path: str,
    cfg: Dict[str, Any],
    fq: str,
    raw_snap: Dict[str, Any],
    filepath: str,
    clean_snap: Dict[str, Any] = None
) -> None:
    """
    Write the raw introspector snapshot and the cleaned snapshot (if provided)
    to `filepath` for debugging. Auto-creates parent dirs.
    NOTE: `clean_snap` should be provided by caller—no sanitizer import here.
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        # Dump YAML config
        f.write(f"# YAML from {yaml_path}\n\n")
        yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False)

        # Dump the _raw_ introspector snapshot
        f.write(f"\n\n# --------- RAW introspector snapshot: {fq} ---------\n\n")
        yaml.safe_dump(raw_snap, f, default_flow_style=False, sort_keys=False)

        # Dump the cleaned snapshot if provided
        if clean_snap is not None:
            f.write(f"\n\n# --------- CLEANED snapshot: {fq} ---------\n\n")
            yaml.safe_dump(clean_snap, f, default_flow_style=False, sort_keys=False)

        # Highlight important blocks if present
        blocks = [
            ("Table-level CHECK constraints",   raw_snap.get("tbl_constraints")),
            ("YAML Row Filters",                cfg.get("row_filters")),
            ("Column-level CHECK constraints",  (clean_snap or {}).get("column_check_constraints") if clean_snap else None),
            ("Column Masking Rules",            (clean_snap or {}).get("col_masking_rules") if clean_snap else None),
            ("Column Default Values",           (clean_snap or {}).get("col_default_values") if clean_snap else None),
            ("Column Variable Values",          (clean_snap or {}).get("col_variable_values") if clean_snap else None),
            ("Foreign Keys",                    cfg.get("foreign_keys")),
            ("Unique Keys",                     cfg.get("unique_keys")),
            ("Primary Key",                     cfg.get("primary_key")),
            ("Partitioned By",                  cfg.get("partitioned_by")),
        ]
        for name, val in blocks:
            if val:
                f.write(f"\n# {name}:\n")
                yaml.safe_dump(val, f, default_flow_style=False, sort_keys=False)