# src/layker/utils/table.py

from typing import Tuple, List
from pyspark.sql import SparkSession, DataFrame
from layker.utils.color import Color

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

def spark_sql_to_df(spark: SparkSession, sql: str) -> DataFrame:
    """
    Run Spark SQL and return the resulting DataFrame.
    """
    try:
        return spark.sql(sql)
    except Exception as e:
        print(f"[ERROR] spark_sql_to_df failed: {e}\nSQL: {sql}")
        raise

def spark_df_to_rows(df: DataFrame) -> List[dict]:
    """
    Convert a Spark DataFrame to a list of dictionaries (rows).
    """
    try:
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"[ERROR] spark_df_to_rows failed: {e}")
        raise 