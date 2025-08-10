# src/layker/differences.py

# === CANONICAL DIFF TEMPLATE ===
"""
DIFF_TEMPLATE = {
    "full_table_name": "",
    "add": {
        "primary_key": [],
        "partitioned_by": [],
        "unique_keys": [],
        "foreign_keys": {},
        "table_check_constraints": {
            "constraint_1": {"name": "", "expression": ""},
        },
        "row_filters": {
            "row_filter_1": {"name": "", "expression": ""}
        },
        "table_tags": {"tag_1": ""},
        "owner": "",
        "table_comment": "",
        "table_properties": {"property_1": ""},
        "columns": {
            1: {
                "name": "",
                "datatype": "",
                "nullable": True,
                "column_comment": "",
                "column_tags": {"tag_1": ""},
                "column_masking_rule": "",
                "column_check_constraints": {
                    "check_1": {"name": "", "expression": ""}
                }
            }
        }
    },
    "update": {
        "primary_key": [],
        "table_check_constraints": {
            "constraint_1": {"name": "", "expression": ""},
        },
        "row_filters": {
            "row_filter_1": {"name": "", "expression": ""}
        },
        "table_tags": {"tag_1": ""},
        "owner": "",
        "table_comment": "",
        "table_properties": {"property_1": ""},
        "columns": {
            1: {
                "name": "",
                "column_comment": "",
                "column_tags": {"tag_1": ""},
                "column_masking_rule": "",
                "column_check_constraints": {
                    "check_1": {"name": "", "expression": ""}
                }
            }
        }
    },
    "remove": {
        "table_check_constraints": {
            "constraint_1": {"name": "", "expression": ""},
        },
        "row_filters": {
            "row_filter_1": {"name": "", "expression": ""}
        },
        "table_tags": {"tag_1": ""},
        "columns": {
            1: {
                "name": "",
                "column_tags": {"tag_1": ""},
                "column_check_constraints": {
                    "check_1": {"name": "", "expression": ""}
                }
            }
        }
    }
}
"""

"""
semantics = {
    "add": {
        "primary_key": "Add new primary keys only",
        "partitioned_by": "Add new partitions only (cannot update/remove)",
        "unique_keys": "Add new unique keys only",
        "foreign_keys": "Add new foreign keys only",
        "table_check_constraints": "Add new table check constraints only",
        "row_filters": "Add new row filters only",
        "table_tags": "Add new table tags only (no update; removal via 'remove')",
        "owner": "Set or update owner",
        "table_comment": "Overwrite (add or replace comment)",
        "table_properties": "Add new table properties only (no removal or update in v1)",

        "columns": {
            1: {
                "name": "Add new column (create or rename on create)",
                "datatype": "Add new column datatype",
                "nullable": "Add new column nullable status",
                "column_comment": "Add or update column comment",
                "column_tags": "Add new column tags only (remove via 'remove')",
                "column_masking_rule": "Add or update column masking rule",
                "column_check_constraints": "Add new column check constraints only (remove via 'remove')",
            }
        }
    },

    "update": {
        "primary_key": "Update existing primary keys (adding only allowed post-create)",
        "table_check_constraints": "Update table check constraints",
        "row_filters": "Update row filters",
        "table_tags": "Overwrite tags (add new; remove old via 'remove')",
        "owner": "Update owner",
        "table_comment": "Overwrite comment",
        "table_properties": "Update table properties",

        "columns": {
            1: {
                "name": "Rename existing column",
                "column_comment": "Overwrite column comment",
                "column_tags": "Overwrite column tags (add new; remove old via 'remove')",
                "column_masking_rule": "Overwrite column masking rule",
                "column_check_constraints": "Update column check constraints",
            }
        }
    },

    "remove": {
        "table_check_constraints": "Remove listed table check constraints",
        "row_filters": "Remove listed row filters",
        "table_tags": "Remove listed table tags",

        "columns": {
            1: {
                "name": "Remove entire column",
                "column_tags": "Remove listed column tags",
                "column_check_constraints": "Remove listed column check constraints",
            }
        }
    }
}
"""


from typing import Dict, Any, Optional

def diff_primary_key(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add, update):
    y_pk = yaml.get("primary_key", [])
    t_pk = table.get("primary_key", []) if table else []
    if y_pk and y_pk != t_pk:
        if not t_pk:
            add["primary_key"] = y_pk
        else:
            update["primary_key"] = y_pk

def diff_partitioned_by(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add):
    y_pb = yaml.get("partitioned_by", [])
    t_pb = table.get("partitioned_by", []) if table else []
    if y_pb and not t_pb:
        add["partitioned_by"] = y_pb

def diff_unique_keys(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add):
    y_uk = yaml.get("unique_keys", [])
    t_uk = table.get("unique_keys", []) if table else []
    if y_uk and y_uk != t_uk:
        add["unique_keys"] = y_uk

def diff_foreign_keys(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add):
    y_fk = yaml.get("foreign_keys", {})
    t_fk = table.get("foreign_keys", {}) if table else {}
    if y_fk and y_fk != t_fk:
        add["foreign_keys"] = y_fk

def diff_table_check_constraints(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add, update, remove):
    y_tcc = yaml.get("table_check_constraints", {})
    t_tcc = table.get("table_check_constraints", {}) if table else {}
    for k, v in y_tcc.items():
        if k not in t_tcc:
            add["table_check_constraints"][k] = v
        elif t_tcc[k] != v:
            update["table_check_constraints"][k] = v
    for k, v in t_tcc.items():
        if k not in y_tcc:
            remove["table_check_constraints"][k] = v

def diff_row_filters(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add, update, remove):
    y_rf = yaml.get("row_filters", {})
    t_rf = table.get("row_filters", {}) if table else {}
    for k, v in y_rf.items():
        if k not in t_rf:
            add["row_filters"][k] = v
        elif t_rf[k] != v:
            update["row_filters"][k] = v
    for k, v in t_rf.items():
        if k not in y_rf:
            remove["row_filters"][k] = v

def diff_table_tags(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add, remove):
    y_tags = yaml.get("tags", {})
    t_tags = table.get("tags", {}) if table else {}
    for k, v in y_tags.items():
        if k not in t_tags:
            add["table_tags"][k] = v
    for k, v in t_tags.items():
        if k not in y_tags:
            remove["table_tags"][k] = v

def diff_owner(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], update):
    y_owner = yaml.get("owner", "")
    t_owner = table.get("owner", "") if table else ""
    if y_owner and y_owner != t_owner:
        update["owner"] = y_owner

def diff_table_comment(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], update):
    y_comment = (yaml.get("comment", "") or "").strip()
    t_comment = (table.get("comment", "") or "").strip() if table else ""
    if y_comment and y_comment != t_comment:
        update["table_comment"] = y_comment

def diff_table_properties(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add):
    y_props = yaml.get("table_properties", {})
    t_props = table.get("table_properties", {}) if table else {}
    for k, v in y_props.items():
        if k not in t_props:
            add["table_properties"][k] = v

def diff_columns(yaml: Dict[str, Any], table: Optional[Dict[str, Any]], add, update, remove):
    y_cols = yaml.get("columns", {}) or {}
    t_cols = table.get("columns", {}) if table else {}

    # FIX: ensure keys are ints!
    y_cols = {int(k): v for k, v in y_cols.items()}
    t_cols = {int(k): v for k, v in t_cols.items()}

    y_idxs, t_idxs = set(y_cols.keys()), set(t_cols.keys())

    # Add new columns at the end
    max_t_idx = max(t_idxs) if t_idxs else 0
    for idx in y_idxs:
        if idx > max_t_idx:
            y_col = y_cols[idx]
            add["columns"][idx] = {
                "name": y_col.get("name", ""),
                "datatype": y_col.get("datatype", ""),
                "nullable": y_col.get("nullable", True),
                "column_comment": y_col.get("comment", ""),
                "column_tags": y_col.get("tags", {}),
                "column_masking_rule": y_col.get("column_masking_rule", ""),
                "column_check_constraints": y_col.get("column_check_constraints", {}),
            }
    # Remove columns missing in YAML
    for idx in t_idxs:
        if idx not in y_idxs:
            t_col = t_cols[idx]
            remove["columns"][idx] = {
                "name": t_col.get("name", ""),
                "column_tags": t_col.get("tags", {}),
                "column_check_constraints": t_col.get("column_check_constraints", {})
            }
    # Per-column tag/check constraint add/remove
    for idx in y_idxs & t_idxs:
        y_col, t_col = y_cols[idx], t_cols[idx]
        col_update = {}

        # Name change = rename (update)
        if y_col.get("name", "") != t_col.get("name", ""):
            col_update["name"] = y_col.get("name", "")
        # Comment update
        if y_col.get("comment", "") != t_col.get("comment", ""):
            col_update["column_comment"] = y_col.get("comment", "")
        # Masking rule update
        if y_col.get("column_masking_rule", "") != t_col.get("column_masking_rule", ""):
            col_update["column_masking_rule"] = y_col.get("column_masking_rule", "")

        # --- Column tags: only add new, remove missing ---
        y_ctags, t_ctags = y_col.get("tags", {}) or {}, t_col.get("tags", {}) or {}
        tag_add, tag_remove = {}, {}
        for k, v in y_ctags.items():
            if k not in t_ctags:
                tag_add[k] = v
        for k, v in t_ctags.items():
            if k not in y_ctags:
                tag_remove[k] = v
        if tag_add:
            col_update["column_tags"] = tag_add
        if tag_remove:
            remove["columns"].setdefault(idx, {}).setdefault("column_tags", {}).update(tag_remove)

        # --- Column check constraints: add new, remove missing ---
        y_cc, t_cc = y_col.get("column_check_constraints", {}) or {}, t_col.get("column_check_constraints", {}) or {}
        cc_add, cc_remove = {}, {}
        for k, v in y_cc.items():
            if k not in t_cc:
                cc_add[k] = v
        for k, v in t_cc.items():
            if k not in y_cc:
                cc_remove[k] = v
        if cc_add:
            col_update["column_check_constraints"] = cc_add
        if cc_remove:
            remove["columns"].setdefault(idx, {}).setdefault("column_check_constraints", {}).update(cc_remove)

        if col_update:
            update["columns"][idx] = col_update

def generate_differences(
    yaml_snapshot: Dict[str, Any],
    table_snapshot: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compute differences between YAML snapshot and table snapshot, enforcing Layker semantics.
    Handles full create (table_snapshot is None) and incremental changes.
    """
    # FULL CREATE: If table does not exist, return all YAML fields under 'add'
    if table_snapshot is None:
        return {
            "full_table_name": yaml_snapshot.get("full_table_name", ""),
            "add": {k: v for k, v in yaml_snapshot.items() if k != "full_table_name"}
        }

    diffs = {
        "full_table_name": yaml_snapshot.get("full_table_name", ""),
        "add": {
            "primary_key": [],
            "partitioned_by": [],
            "unique_keys": [],
            "foreign_keys": {},
            "table_check_constraints": {},
            "row_filters": {},
            "table_tags": {},
            "owner": "",
            "table_comment": "",
            "table_properties": {},
            "columns": {},
        },
        "update": {
            "primary_key": [],
            "table_check_constraints": {},
            "row_filters": {},
            "table_tags": {},
            "owner": "",
            "table_comment": "",
            "columns": {},
        },
        "remove": {
            "table_check_constraints": {},
            "row_filters": {},
            "table_tags": {},
            "columns": {},
        }
    }

    # Dispatch to rules
    diff_primary_key(yaml_snapshot, table_snapshot, diffs["add"], diffs["update"])
    diff_partitioned_by(yaml_snapshot, table_snapshot, diffs["add"])
    diff_unique_keys(yaml_snapshot, table_snapshot, diffs["add"])
    diff_foreign_keys(yaml_snapshot, table_snapshot, diffs["add"])
    diff_table_check_constraints(yaml_snapshot, table_snapshot, diffs["add"], diffs["update"], diffs["remove"])
    diff_row_filters(yaml_snapshot, table_snapshot, diffs["add"], diffs["update"], diffs["remove"])
    diff_table_tags(yaml_snapshot, table_snapshot, diffs["add"], diffs["remove"])
    diff_owner(yaml_snapshot, table_snapshot, diffs["update"])
    diff_table_comment(yaml_snapshot, table_snapshot, diffs["update"])
    diff_table_properties(yaml_snapshot, table_snapshot, diffs["add"])
    diff_columns(yaml_snapshot, table_snapshot, diffs["add"], diffs["update"], diffs["remove"])

    # Clean up: only keep non-empty sections
    out = {"full_table_name": diffs["full_table_name"]}
    for section in ["add", "update", "remove"]:
        filtered = {k: v for k, v in diffs[section].items() if v and (not isinstance(v, dict) or len(v))}
        if filtered:
            out[section] = filtered

    # >>> IMPORTANT: if there are no changes at all, return {} so `if not snapshot_diff:` works
    if not any(out.get(s) for s in ("add", "update", "remove")):
        return {}

    return out