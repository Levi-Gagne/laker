# src/layker/utils/table.py

from typing import Tuple
from pyspark.sql import SparkSession
from layker.utils.color import Color


def parse_fully_qualified_table_name(fq_table: str) -> Tuple[str, str, str]:
    """
    Splits a fully qualified table name into (catalog, schema, table).
    Example: "dq_dev.lmg_sandbox.table1" → ("dq_dev", "lmg_sandbox", "table1")
    """
    if not isinstance(fq_table, str):
        print(f"{Color.b}{Color.candy_red}[ERROR]{Color.r} fq_table must be a string, got {type(fq_table).__name__}")
        raise TypeError("fq_table must be a string.")

    parts = fq_table.split(".")
    if len(parts) != 3:
        print(f"{Color.b}{Color.candy_red}[ERROR]{Color.r} Expected catalog.schema.table, got: {fq_table!r}")
        raise ValueError("Expected catalog.schema.table format.")

    return parts[0], parts[1], parts[2]


def table_exists(spark: SparkSession, fully_qualified_table: str) -> bool:
    """
    Returns True if the table exists in the Spark catalog, else False.
    """
    try:
        exists: bool = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        print(f"[ERROR] Exception in table_exists({fully_qualified_table}): {e}")
        return False


def refresh_table(spark: SparkSession, fully_qualified_table: str) -> None:
    """
    Refresh the table metadata in the Spark catalog.
    """
    try:
        spark.catalog.refreshTable(fully_qualified_table)
        print(f"[REFRESH] Table {fully_qualified_table} refreshed.")
    except Exception as e:
        print(f"[REFRESH][ERROR] Could not refresh table {fully_qualified_table}: {e}")