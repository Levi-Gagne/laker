# src/layker/table.py

from typing import List, Dict, Any, Optional
import re
from pyspark.sql import SparkSession
from layker.utils.spark import spark_sql_to_rows
from layker.utils.helpers import parse_fully_qualified_table_name

# ===== DESCRIBE TABLE EXTENDED Parsers =====

def extract_columns(describe_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
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
            if col_name == "Catalog":
                details["catalog"] = data_type
            elif col_name == "Database":
                details["schema"] = data_type
            elif col_name == "Table":
                details["table"] = data_type
            elif col_name == "Owner":
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

def extract_primary_key(describe_rows: List[Dict[str, Any]]) -> Optional[List[str]]:
    cons = extract_constraints(describe_rows)
    for c in cons:
        if "PRIMARY KEY" in c["type"]:
            m = re.search(r"\((.*?)\)", c["type"])
            if m:
                return [col.strip().replace("`", "") for col in m.group(1).split(",")]
    return None

# ===== Info Schema Table Query Functions =====

def get_table_tags(spark: SparkSession, fq_table: str) -> Dict[str, str]:
    try:
        catalog, schema, table = parse_fully_qualified_table_name(fq_table)
        sql = f"""
            SELECT tag_name, tag_value
            FROM system.information_schema.table_tags
            WHERE catalog_name = '{catalog}'
              AND schema_name = '{schema}'
              AND table_name = '{table}'
        """
        rows = spark_sql_to_rows(spark, sql)
        return {row['tag_name']: row['tag_value'] for row in rows}
    except Exception as e:
        print(f"[ERROR] get_table_tags failed: {e}")
        return {}

def get_column_tags(spark: SparkSession, fq_table: str) -> Dict[str, Dict[str, str]]:
    try:
        catalog, schema, table = parse_fully_qualified_table_name(fq_table)
        sql = f"""
            SELECT column_name, tag_name, tag_value
            FROM system.information_schema.column_tags
            WHERE catalog_name = '{catalog}'
              AND schema_name = '{schema}'
              AND table_name = '{table}'
        """
        rows = spark_sql_to_rows(spark, sql)
        col_tags = {}
        for row in rows:
            col = row['column_name']
            tag = row['tag_name']
            val = row['tag_value']
            if col not in col_tags:
                col_tags[col] = {}
            col_tags[col][tag] = val
        return col_tags
    except Exception as e:
        print(f"[ERROR] get_column_tags failed: {e}")
        return {}

def get_row_filters(spark: SparkSession, fq_table: str) -> List[dict]:
    try:
        catalog, schema, table = parse_fully_qualified_table_name(fq_table)
        sql = f"""
            SELECT filter_name, target_columns
            FROM system.information_schema.row_filters
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
        """
        return spark_sql_to_rows(spark, sql)
    except Exception as e:
        print(f"[ERROR] get_row_filters failed: {e}")
        return []

def get_constraint_table_usage(spark: SparkSession, fq_table: str) -> List[dict]:
    try:
        catalog, schema, table = parse_fully_qualified_table_name(fq_table)
        sql = f"""
            SELECT constraint_name
            FROM system.information_schema.constraint_table_usage
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
        """
        return spark_sql_to_rows(spark, sql)
    except Exception as e:
        print(f"[ERROR] get_constraint_table_usage failed: {e}")
        return []

def get_constraint_column_usage(spark: SparkSession, fq_table: str) -> List[dict]:
    try:
        catalog, schema, table = parse_fully_qualified_table_name(fq_table)
        sql = f"""
            SELECT column_name, constraint_name
            FROM system.information_schema.constraint_column_usage
            WHERE table_catalog = '{catalog}'
              AND table_schema = '{schema}'
              AND table_name = '{table}'
        """
        return spark_sql_to_rows(spark, sql)
    except Exception as e:
        print(f"[ERROR] get_constraint_column_usage failed: {e}")
        return []