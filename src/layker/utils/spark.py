# src/layker/utils/spark.py

from pyspark.sql import SparkSession
import sys
from layker.utils.color import Color

# Try to import newer Spark error classes for specific handling if available
try:
    from pyspark.errors import PySparkRuntimeError, PySparkException
except ImportError:
    PySparkRuntimeError = PySparkException = Exception  # fallback if not available

def get_or_create_spark() -> SparkSession:
    """
    Get or create a SparkSession, with color-coded output for robust UX.
    Exits with code 2 on failure.
    """
    try:
        print(f"{Color.b}{Color.yellow}! No SparkSession passed; starting a new one.{Color.r}")
        spark: SparkSession = SparkSession.builder.getOrCreate()
        return spark
    except ImportError as e:
        print(f"{Color.b}{Color.candy_red}✘ PySpark is not installed or not found: {e}{Color.r}")
        sys.exit(2)
    except OSError as e:
        print(f"{Color.b}{Color.candy_red}✘ OS error during SparkSession init: {e}{Color.r}")
        sys.exit(2)
    except PySparkRuntimeError as e:
        print(f"{Color.b}{Color.candy_red}✘ PySpark runtime error: {e}{Color.r}")
        sys.exit(2)
    except PySparkException as e:
        print(f"{Color.b}{Color.candy_red}✘ PySpark error: {e}{Color.r}")
        sys.exit(2)
    except Exception as e:
        print(f"{Color.b}{Color.candy_red}✘ Could not start SparkSession: {e}{Color.r}")
        sys.exit(2)
