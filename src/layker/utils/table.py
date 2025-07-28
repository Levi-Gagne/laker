# src/layker/utils/table.py

from pyspark.sql import SparkSession

def table_exists(
    spark: SparkSession, 
    fully_qualified_table: str
) -> bool:
    """
    Returns True if the table exists in the Spark catalog, else False.
    """
    try:
        exists: bool = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        print(f"[ERROR] Exception in table_exists({fully_qualified_table}): {e}")
        return False

def refresh_table(
    spark: SparkSession,
    fully_qualified_table: str
) -> None:
    """
    Refresh the table metadata in the Spark catalog.
    """
    try:
        spark.catalog.refreshTable(fully_qualified_table)
        print(f"[REFRESH] Table {fully_qualified_table} refreshed.")
    except Exception as e:
        print(f"[REFRESH][ERROR] Could not refresh table {fully_qualified_table}: {e}")