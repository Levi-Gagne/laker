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
        exists = spark.catalog.tableExists(fully_qualified_table)
        return bool(exists)
    except Exception as e:
        # Could log the error here, or raise if you want strict failure
        # For CLI/dev: print, for prod: raise or use logging
        print(f"[ERROR] Exception in table_exists({fully_qualified_table}): {e}")
        return False