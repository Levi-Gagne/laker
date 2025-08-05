# src/layker/snapshot_table.py

import re
from typing import Any, Dict, List, Optional
from pyspark.sql import SparkSession

from layker.utils.color import Color
from layker.utils.table import table_exists

class TableSnapshot:
    SNAPSHOT_QUERIES = {
        "table_tags": {
            "table": "system.information_schema.table_tags",
            "columns": ["catalog_name", "schema_name", "table_name", "tag_name", "tag_value"],
            "where_keys": [("catalog_name", 0), ("schema_name", 1), ("table_name", 2)],
        },
        "column_tags": {
            "table": "system.information_schema.column_tags",
            "columns": ["catalog_name", "schema_name", "table_name", "column_name", "tag_name", "tag_value"],
            "where_keys": [("catalog_name", 0), ("schema_name", 1), ("table_name", 2)],
        },
        "row_filters": {
            "table": "system.information_schema.row_filters",
            "columns": ["table_catalog", "table_schema", "table_name", "filter_name", "target_columns"],
            "where_keys": [("table_catalog", 0), ("table_schema", 1), ("table_name", 2)],
        },
        "constraint_table_usage": {
            "table": "system.information_schema.constraint_table_usage",
            "columns": ["constraint_catalog", "constraint_schema", "constraint_name"],
            "where_keys": [("table_catalog", 0), ("table_schema", 1), ("table_name", 2)],
        },
        "constraint_column_usage": {
            "table": "system.information_schema.constraint_column_usage",
            "columns": ["column_name", "constraint_name"],
            "where_keys": [("table_catalog", 0), ("table_schema", 1), ("table_name", 2)],
        },
    }

    def __init__(self, spark: SparkSession, fq_table: str):
        self.spark = spark
        self.fq_table = fq_table
        self.catalog, self.schema, self.table = self._parse_fq_table(fq_table)

    def _parse_fq_table(self, fq_table: str):
        parts = fq_table.split(".")
        if len(parts) != 3:
            raise ValueError("Expected format: catalog.schema.table")
        return parts[0], parts[1], parts[2]

    # Removed the old _table_exists method.

    def _build_metadata_sql(self, kind: str) -> str:
        config = self.SNAPSHOT_QUERIES[kind]
        table_vars = [self.catalog, self.schema, self.table]
        where_clauses = [
            f"{col_name} = '{table_vars[idx]}'"
            for col_name, idx in config["where_keys"]
        ]
        columns = ", ".join(config["columns"])
        return f"SELECT {columns} FROM {config['table']} WHERE {' AND '.join(where_clauses)}"

    def _get_metadata_snapshot(self) -> Dict[str, List[Dict[str, Any]]]:
        results = {}
        for kind in self.SNAPSHOT_QUERIES:
            try:
                sql = self._build_metadata_sql(kind)
                df = self.spark.sql(sql)
                results[kind] = [row.asDict() for row in df.collect()]
            except Exception:
                results[kind] = []
        return results

    def _get_describe_rows(self) -> List[Dict[str, Any]]:
        sql = f"DESCRIBE EXTENDED {self.fq_table}"
        df = self.spark.sql(sql)
        return [row.asDict() for row in df.collect()]

    def _extract_columns(self, describe_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        columns = []
        for row in describe_rows:
            col_name = (row.get("col_name") or "").strip()
            data_type = (row.get("data_type") or "").strip()
            comment = (row.get("comment") or "").strip() if row.get("comment") else ""
            if col_name == "" or col_name.startswith("#"):
                if col_name == "# Partition Information":
                    break
                continue
            columns.append({
                "name": col_name,
                "datatype": data_type,
                "comment": comment if comment.upper() != "NULL" else "",
            })
        return columns

    def _extract_partitioned_by(self, describe_rows: List[Dict[str, Any]]) -> List[str]:
        collecting = False
        partition_cols = []
        for row in describe_rows:
            col_name = (row.get("col_name") or "").strip()
            if col_name == "# Partition Information":
                collecting = True
                continue
            if collecting:
                if col_name.startswith("#") and col_name != "# col_name":
                    break
                if col_name == "" or col_name == "# col_name":
                    continue
                partition_cols.append(col_name)
        return partition_cols

    def _extract_table_details(self, describe_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
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

    def _extract_constraints(self, describe_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
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

    def _build_columns(self, columns_raw: List[Dict[str, Any]], col_tags: Dict[str, Dict[str, Any]], col_checks: Dict[str, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        columns = {}
        for idx, col in enumerate(columns_raw, start=1):
            name = col["name"]
            columns[idx] = {
                "name": name,
                "datatype": col["datatype"],
                "nullable": None,  # Could be filled with extended logic
                "active": True,
                "comment": col.get("comment", ""),
                "tags": col_tags.get(name, {}),
                "column_masking_rule": "",  # No masking in snapshot, set empty
                "column_check_constraints": col_checks.get(name, {}),
            }
        return columns
    
    def _get_foreign_keys(self, uc_metadata: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        return {}

    def _get_unique_keys(self, uc_metadata: Dict[str, List[Dict[str, Any]]]) -> List[List[str]]:
        return []

    def build_table_metadata_dict(self) -> Optional[Dict[str, Any]]:
        if not table_exists(self.spark, self.fq_table):
            print(f"{Color.b}{C.ivory}Target table '{C.r}{Color.b}{C.sky_blue}{fq_table}{Color.r}{Color.b}{C.ivory}' doesn't exist; returning '{C.r}{Color.b}{C.blush_pink}None{Color.r}{Color.b}{C.ivory}' to differences for snapshot_table{Color.r}")
            return None
        uc_metadata = self._get_metadata_snapshot()
        describe_rows = self._get_describe_rows()

        catalog, schema, table = self.catalog, self.schema, self.table

        table_tags = {row["tag_name"]: row["tag_value"] for row in uc_metadata.get("table_tags", [])}
        details = self._extract_table_details(describe_rows)
        table_check_constraints = {
            k: {"name": k, "expression": v}
            for k, v in details.get("table_properties", {}).items()
            if k.startswith("delta.constraints")
        }

        row_filters = {}
        for row in uc_metadata.get("row_filters", []):
            fname = row.get("filter_name")
            if fname:
                row_filters[fname] = {
                    "name": fname,
                    "expression": row.get("target_columns", "")
                }

        partitioned_by = self._extract_partitioned_by(describe_rows)

        constraints = self._extract_constraints(describe_rows)
        pk = []
        for c in constraints:
            if "PRIMARY KEY" in c["type"]:
                m = re.search(r"\((.*?)\)", c["type"])
                if m:
                    pk = [col.strip().replace("`", "") for col in m.group(1).split(",")]

        columns_raw = self._extract_columns(describe_rows)
        col_tag_lookup = {}
        for row in uc_metadata.get("column_tags", []):
            col = row["column_name"]
            if col not in col_tag_lookup:
                col_tag_lookup[col] = {}
            col_tag_lookup[col][row["tag_name"]] = row["tag_value"]

        col_constraint_lookup = {}
        for row in uc_metadata.get("constraint_column_usage", []):
            col = row["column_name"]
            cons = row["constraint_name"]
            if col not in col_constraint_lookup:
                col_constraint_lookup[col] = {}
            col_constraint_lookup[col][cons] = {"name": cons}  # no expression parsing here

        columns = self._build_columns(columns_raw, col_tag_lookup, col_constraint_lookup)

        return {
            "full_table_name": self.fq_table,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "primary_key": pk,
            "foreign_keys": self._get_foreign_keys(uc_metadata),
            "unique_keys": self._get_unique_keys(uc_metadata),
            "partitioned_by": partitioned_by,
            "tags": table_tags,
            "row_filters": row_filters,
            "table_check_constraints": table_check_constraints,
            "table_properties": details.get("table_properties", {}),
            "comment": details.get("comment", ""),
            "owner": details.get("owner", ""),
            "columns": columns,
        }

# Usage example:
# spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
# table_snapshot = TableSnapshot(spark, "dq_dev.lmg_sandbox.config_driven_table_example")
# metadata_dict = table_snapshot.build_table_metadata_dict()
# pprint.pprint(metadata_dict, width=120)