# src/layker/snapshot.py

from typing import List, Dict, Any, Optional
import re
from layker.utils.table import parse_fully_qualified_table_name

"""
{
    "table": {
        "fully_qualified_name": "",
        "catalog": "",
        "schema": "",
        "table": "",
        "owner": "",
        "comment": "",
        "table_properties": {},
        "table_tags": {
            "example_tag": ""
        },
        "table_check_constraints": {
            "constraint_1": {
                "name": "",
                "expression": ""
            }
        },
        "row_filters": [
            {
                "filter_name": "",
                "target_columns": ""
            }
        ],
        "partitioned_by": [""],
        "primary_key": [""],
        "columns": {
            1: {
                "column_name": "",
                "datatype": "",
                "comment": "",
                "nullable": None,
                "masking_rule": None,
                "column_tags": {
                    "example_tag": ""
                },
                "column_check_constraints": {
                    "constraint_1": {
                        "name": "",
                        "expression": ""
                    }
                }
            },
            2: {
                "column_name": "",
                "datatype": "",
                "comment": "",
                "nullable": None,
                "masking_rule": None,
                "column_tags": {
                    "example_tag": ""
                },
                "column_check_constraints": {
                    "constraint_1": {
                        "name": "",
                        "expression": ""
                    }
                }
            },
            3: {
                "column_name": "",
                "datatype": "",
                "comment": "",
                "nullable": None,
                "masking_rule": None,
                "column_tags": {
                    "example_tag": ""
                },
                "column_check_constraints": {
                    "constraint_1": {
                        "name": "",
                        "expression": ""
                    }
                }
            }
        }
    }
}
"""

SNAPSHOT_QUERIES = {
    "table_tags": {
        "table": "system.information_schema.table_tags",
        "columns": ["tag_name", "tag_value"],
    },
    "column_tags": {
        "table": "system.information_schema.column_tags",
        "columns": ["column_name", "tag_name", "tag_value"],
    },
    "row_filters": {
        "table": "system.information_schema.row_filters",
        "columns": ["filter_name", "target_columns"],
    },
    "constraint_table_usage": {
        "table": "system.information_schema.constraint_table_usage",
        "columns": ["constraint_name"],
    },
    "constraint_column_usage": {
        "table": "system.information_schema.constraint_column_usage",
        "columns": ["column_name", "constraint_name"],
    },
}

def build_metadata_sql(kind: str, fq_table: str) -> str:
    """
    Builds the SQL string for querying metadata tables using SNAPSHOT_QUERIES config.
    """
    catalog, schema, table = parse_fully_qualified_table_name(fq_table)
    config = SNAPSHOT_QUERIES.get(kind)
    if not config:
        raise ValueError(f"[ERROR] Unsupported metadata query kind: {kind}")

    columns = ", ".join(config["columns"])
    return f"""
        SELECT {columns}
        FROM {config['table']}
        WHERE catalog_name = '{catalog}'
          AND schema_name = '{schema}'
          AND table_name = '{table}'
    """

def extract_columns(describe_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    columns = []
    for row in describe_rows:
        col_name = (row.get("col_name") or "").strip()
        data_type = (row.get("data_type") or "").strip()
        comment = (row.get("comment") or "").strip() if row.get("comment") else None

        if col_name == "" or col_name.startswith("#"):
            if col_name == "# Partition Information":
                break
            continue
        columns.append({
            "name": col_name,
            "datatype": data_type,
            "comment": comment if comment and comment.upper() != "NULL" else "",
        })
    return columns

def extract_partitioned_by(describe_rows: List[Dict[str, Any]]) -> List[str]:
    collecting = False
    partition_cols = []
    for row in describe_rows:
        col_name = (row.get("col_name") or "").strip()
        if col_name == "# Partition Information":
            collecting = True
            continue
        if collecting:
            if not col_name or col_name.startswith("#"):
                break
            if col_name != "# col_name":
                partition_cols.append(col_name)
    return partition_cols

def extract_table_details(describe_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract owner, comment, and table_properties from DESCRIBE EXTENDED output.
    """
    details = {}
    table_properties = {}
    in_details = False
    for row in describe_rows:
        col_name = (row.get("col_name") or "").strip()
        data_type = (row.get("data_type") or "").strip()
        if col_name == "# Detailed Table Information":
            in_details = True
            continue
        if in_details:
            if not col_name or col_name.startswith("#"):
                break
            if col_name == "Owner":
                details["owner"] = data_type
            elif col_name == "Comment":
                details["comment"] = data_type
            elif col_name == "Table Properties":
                for prop in data_type.strip("[]").split(","):
                    if "=" in prop:
                        k, v = prop.split("=", 1)
                        table_properties[k.strip()] = v.strip()
    details["table_properties"] = table_properties
    return details

def extract_constraints(describe_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    constraints = []
    in_constraints = False
    for row in describe_rows:
        col_name = (row.get("col_name") or "").strip()
        data_type = (row.get("data_type") or "").strip()
        if col_name == "# Constraints":
            in_constraints = True
            continue
        if in_constraints:
            if not col_name or col_name.startswith("#"):
                break
            if col_name and data_type:
                constraints.append({"name": col_name, "type": data_type})
    return constraints
