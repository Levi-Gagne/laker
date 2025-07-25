# /src/layker/introspector.py

from typing import Any, Dict, List, Tuple
from pyspark.sql import SparkSession

class TableIntrospector:
    """
    Reads live Delta table metadata from Spark / Unity Catalog.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark


    def get_columns_and_types(self, fq: str) -> List[Tuple[str, str]]:
        rows = self.spark.sql(f"DESCRIBE TABLE {fq}").collect()
        seen = set()
        out: List[Tuple[str, str]] = []
        for r in rows:
            name, typ = r["col_name"], r["data_type"]
            if name and not name.startswith("#") and name not in seen:
                out.append((name, typ))
                seen.add(name)
        return out

    def get_column_comments(self, fq: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        rows = self.spark.sql(f"DESCRIBE TABLE EXTENDED {fq}").collect()
        for r in rows:
            n, c = r["col_name"], r["comment"]
            if n and not n.startswith("#"):
                out[n] = c or ""
        return out

    def get_column_tags(self, fq: str) -> Dict[str, Dict[str, str]]:
        tags: Dict[str, Dict[str, str]] = {}
        try:
            rows = self.spark.sql(f"SHOW COLUMN TAGS {fq}").collect()
            for r in rows:
                col = r["column_name"]
                tags.setdefault(col, {})[r["key"]] = r["value"]
        except Exception:
            pass
        return tags

    def get_table_tags(self, fq: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        try:
            rows = self.spark.sql(f"SHOW TABLE TAGS {fq}").collect()
            for r in rows:
                out[r["key"]] = r["value"]
        except Exception:
            pass
        return out

    def get_table_properties(self, fq: str) -> Dict[str, str]:
        """
        Grabs only the properties you explicitly set on this table,
        via SHOW TBLPROPERTIES.
        """
        props: Dict[str, str] = {}
        try:
            rows = self.spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()
            for r in rows:
                props[r["key"].lower()] = r["value"]
        except Exception:
            pass
        return props

    def get_table_comment(self, fq: str) -> str:
        """
        Pulls the table-level COMMENT from DESCRIBE EXTENDED.
        """
        rows = self.spark.sql(f"DESCRIBE TABLE EXTENDED {fq}").collect()
        for r in rows:
            if r["col_name"] and r["col_name"].strip().lower() == "comment":
                return r["comment"] or ""
        return ""

    def get_table_check_constraints(self, fq: str) -> Dict[str, Dict[str, str]]:
        """
        Returns a dict of {constraint_name: {"expression": ...}} for all table-level check constraints,
        merging native and Delta tblproperties.
        """
        constraints: Dict[str, Dict[str, str]] = {}
        # 1. Native CHECK constraints
        try:
            rows = self.spark.sql(f"SHOW TABLE CONSTRAINTS {fq}").collect()
            for r in rows:
                if r["constraint_type"] == "CHECK":
                    constraints[r["name"]] = {"expression": r.get("expression", "")}
        except Exception:
            pass
        # 2. Delta-engine constraints in tblproperties
        try:
            rows = self.spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()
            for r in rows:
                k, v = r["key"], r["value"]
                if k.startswith("delta.constraints.constraint_"):
                    name = k.split("delta.constraints.constraint_")[-1]
                    constraints[name] = {"expression": v}
        except Exception:
            pass
        return constraints

    def get_column_check_constraints(self, fq: str) -> Dict[str, Dict[str, str]]:
        """
        Returns a mapping column_name → {constraint_name: expression}.
        """
        col_checks: Dict[str, Dict[str, str]] = {}
        try:
            rows = self.spark.sql(f"SHOW TABLE CONSTRAINTS {fq}").collect()
            for r in rows:
                if r["constraint_type"].upper() == "CHECK":
                    col = r.get("column_name")
                    name = r["name"]
                    expr = r.get("expression", "")
                    if col:
                        col_checks.setdefault(col, {})[name] = expr
        except Exception:
            pass
        return col_checks

    def snapshot(self, fq: str) -> Dict[str, Any]:
        return {
            "columns":                   self.get_columns_and_types(fq),
            "comments":                  self.get_column_comments(fq),
            "col_tags":                  self.get_column_tags(fq),
            "tbl_tags":                  self.get_table_tags(fq),
            "tbl_props":                 self.get_table_properties(fq),
            "tbl_comment":               self.get_table_comment(fq),
            "tbl_constraints":           self.get_table_check_constraints(fq),
            "column_check_constraints":  self.get_column_check_constraints(fq),
        }