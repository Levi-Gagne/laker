"""
Module: differences.py
Purpose: Compare two metadata snapshots (from table and YAML) and return a canonical differences dictionary.
Author: Levi Gagne / Laker

Below is the full diff template (for reference only, not used at runtime).
Developers: This shows the *full set* of supported diff keys at each level for add, update, remove.
Copy/paste for onboarding, debugging, or updating. Do not return this entire structure in actual code output.
"""

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

from typing import Dict, Any

def generate_differences(yaml_snapshot: Dict[str, Any], table_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute differences between YAML snapshot and table snapshot,
    returning a canonical dictionary structure for use by the loader.
    Output will be a *subset* of the above template, including only non-empty changes.
    """
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
            "table_properties": {},
            "columns": {},
        },
        "remove": {
            "table_check_constraints": {},
            "row_filters": {},
            "table_tags": {},
            "columns": {},
        }
    }

    # --- TABLE-LEVEL DIFFS ---
    y_pk = yaml_snapshot.get("primary_key", [])
    t_pk = table_snapshot.get("primary_key", [])
    if y_pk and y_pk != t_pk:
        if not t_pk:
            diffs["add"]["primary_key"] = y_pk
        else:
            diffs["update"]["primary_key"] = y_pk

    y_uk = yaml_snapshot.get("unique_keys", [])
    t_uk = table_snapshot.get("unique_keys", [])
    if y_uk and y_uk != t_uk:
        diffs["add"]["unique_keys"] = y_uk

    y_fk = yaml_snapshot.get("foreign_keys", {})
    t_fk = table_snapshot.get("foreign_keys", {})
    if y_fk and y_fk != t_fk:
        diffs["add"]["foreign_keys"] = y_fk

    y_tcc = yaml_snapshot.get("table_check_constraints", {})
    t_tcc = table_snapshot.get("table_check_constraints", {})
    for k, v in y_tcc.items():
        if k not in t_tcc:
            diffs["add"]["table_check_constraints"][k] = v
        elif t_tcc[k] != v:
            diffs["update"]["table_check_constraints"][k] = v
    for k, v in t_tcc.items():
        if k not in y_tcc:
            diffs["remove"]["table_check_constraints"][k] = v

    y_rf = yaml_snapshot.get("row_filters", {})
    t_rf = table_snapshot.get("row_filters", {})
    for k, v in y_rf.items():
        if k not in t_rf:
            diffs["add"]["row_filters"][k] = v
        elif t_rf[k] != v:
            diffs["update"]["row_filters"][k] = v
    for k, v in t_rf.items():
        if k not in y_rf:
            diffs["remove"]["row_filters"][k] = v

    y_tags = yaml_snapshot.get("tags", {})
    t_tags = table_snapshot.get("tags", {})
    for k, v in y_tags.items():
        if k not in t_tags:
            diffs["add"]["table_tags"][k] = v
        elif t_tags[k] != v:
            diffs["remove"]["table_tags"][k] = t_tags[k]
            diffs["add"]["table_tags"][k] = v
    for k, v in t_tags.items():
        if k not in y_tags:
            diffs["remove"]["table_tags"][k] = v

    y_owner = yaml_snapshot.get("owner", "")
    t_owner = table_snapshot.get("owner", "")
    if y_owner != t_owner:
        diffs["update"]["owner"] = y_owner

    y_comment = yaml_snapshot.get("comment", "")
    t_comment = table_snapshot.get("comment", "")
    if y_comment != t_comment:
        diffs["update"]["table_comment"] = y_comment

    y_props = yaml_snapshot.get("table_properties", {})
    t_props = table_snapshot.get("table_properties", {})
    for k, v in y_props.items():
        if k not in t_props:
            diffs["add"]["table_properties"][k] = v
        elif t_props[k] != v:
            diffs["update"]["table_properties"][k] = v

    # --- COLUMN-LEVEL DIFFS ---
    y_cols = yaml_snapshot.get("columns", {})
    t_cols = table_snapshot.get("columns", {})
    max_idx = max(len(y_cols), len(t_cols), 1)

    for idx in range(1, max_idx + 1):
        y_col = y_cols.get(idx)
        t_col = t_cols.get(idx)

        # ADD COLUMN
        if y_col and not t_col:
            diffs["add"]["columns"][idx] = {
                "name": y_col.get("name", ""),
                "datatype": y_col.get("datatype", ""),
                "nullable": y_col.get("nullable", True),
                "column_comment": y_col.get("comment", ""),
                "column_tags": y_col.get("tags", {}),
                "column_masking_rule": y_col.get("column_masking_rule", ""),
                "column_check_constraints": y_col.get("column_check_constraints", {}),
            }
            continue

        # REMOVE COLUMN
        if t_col and not y_col:
            diffs["remove"]["columns"][idx] = {
                "name": t_col.get("name", ""),
                "column_tags": t_col.get("tags", {}),
                "column_check_constraints": t_col.get("column_check_constraints", {})
            }
            continue

        # UPDATE COLUMN
        if y_col and t_col:
            col_update = {}
            if y_col.get("name", "") != t_col.get("name", ""):
                col_update["name"] = y_col.get("name", "")
            if y_col.get("comment", "") != t_col.get("comment", ""):
                col_update["column_comment"] = y_col.get("comment", "")
            if y_col.get("tags", {}) != t_col.get("tags", {}):
                col_update["column_tags"] = y_col.get("tags", {})
            if y_col.get("column_masking_rule", "") != t_col.get("column_masking_rule", ""):
                col_update["column_masking_rule"] = y_col.get("column_masking_rule", "")
            y_checks = y_col.get("column_check_constraints", {})
            t_checks = t_col.get("column_check_constraints", {})
            checks_update = {}
            for k, v in y_checks.items():
                if k not in t_checks or t_checks[k] != v:
                    checks_update[k] = v
            for k in t_checks:
                if k not in y_checks:
                    if "column_check_constraints" not in col_update:
                        col_update["column_check_constraints"] = {}
                    col_update["column_check_constraints"][k] = t_checks[k]
            if checks_update:
                col_update["column_check_constraints"] = checks_update
            if col_update:
                diffs["update"]["columns"][idx] = col_update

    return diffs