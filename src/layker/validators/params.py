# src/layker/validators/params.py

import re
from pathlib import Path
from typing import Optional, Tuple, Any
from pyspark.sql import SparkSession

def validate_yaml_path(yaml_path: str) -> str:
    if not yaml_path:
        raise ValueError("yaml_path is required.")
    p = Path(yaml_path)
    if p.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError(f"yaml_path must end in .yml/.yaml, got {yaml_path!r}")
    return str(p)

def validate_log_ddl(log_ddl: Optional[str]) -> Optional[str]:
    if not log_ddl:
        return None
    ld = Path(log_ddl)
    if ld.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError(f"log_ddl must end in .yml/.yaml, got {log_ddl!r}")
    return str(ld)

def validate_mode(mode: Optional[str]) -> str:
    if not mode:
        return "apply"
    m = mode.lower().strip()
    allowed = {"validate", "diff", "apply", "all"}
    if m not in allowed:
        raise ValueError(f"mode must be one of {allowed}, got {mode!r}")
    return m

def validate_env(env: Optional[str]) -> Optional[str]:
    if not env:
        return None
    e = env.strip()
    if e and not re.fullmatch(r"[A-Za-z0-9_]+", e):
        raise ValueError(f"env may only contain letters, numbers, or underscores, got {env!r}")
    return e

def validate_audit_log_table(audit_log_table: Any) -> Any:
    # Accept True, False, None, str (YAML path or catalog.schema.table)
    if audit_log_table in (True, False, None):
        return audit_log_table
    if isinstance(audit_log_table, str):
        a = audit_log_table.strip()
        if a.lower().endswith((".yml", ".yaml")):
            return a
        elif re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", a):
            return a
        else:
            raise ValueError(
                f"audit_log_table must be True, False, a .yml/.yaml path, or catalog.schema.table (got {audit_log_table!r})"
            )
    raise ValueError(
        f"audit_log_table must be True, False, a string path, or catalog.schema.table (got type {type(audit_log_table).__name__})"
    )

def validate_spark(spark: Any) -> SparkSession:
    if not isinstance(spark, SparkSession):
        raise ValueError(f"spark must be a pyspark.sql.SparkSession, got {type(spark).__name__}")
    return spark

def validate_params(
    yaml_path: str,
    log_ddl: Optional[str],
    mode: Optional[str],
    env: Optional[str],
    audit_log_table: Any,
    spark: Any
) -> Tuple[str, Optional[str], Any]:
    """
    Validate & normalize run_table_load parameters.
    Returns (mode, env, audit_log_table) cleaned.
    Raises ValueError on invalid input.
    - yaml_path, log_ddl, mode, env, audit_log_table, spark
    """
    validate_yaml_path(yaml_path)
    validate_log_ddl(log_ddl)
    m = validate_mode(mode)
    e = validate_env(env)
    a = validate_audit_log_table(audit_log_table)
    validate_spark(spark)
    return m, e, a