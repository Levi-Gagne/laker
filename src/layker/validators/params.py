# src/layker/validators/params.py

import re
from pathlib import Path
from typing import Optional, Tuple, Any

from layker.utils.paths import file_on_disk, resolve_resource_path, list_resource_files


def validate_yaml_path(yaml_path: str) -> str:
    """
    Validates the yaml_path as a .yml/.yaml file.
    - If it matches a resource in layker/resources, returns the resolved resource path.
    - Otherwise, treats as a disk path (local, workspace, or volume) and validates existence.
    """
    if not yaml_path:
        raise ValueError("yaml_path is required.")

    # Look for resource match FIRST
    resource_files = list_resource_files("layker.resources")
    if yaml_path in resource_files:
        resolved = resolve_resource_path("layker.resources", yaml_path)
        if resolved and Path(resolved).suffix.lower() in {".yml", ".yaml"}:
            return resolved
        else:
            raise ValueError(f"yaml_path '{yaml_path}' found in resources but could not be resolved.")

    # Otherwise, treat as disk path
    disk_path = file_on_disk(yaml_path)
    if disk_path and Path(disk_path).suffix.lower() in {".yml", ".yaml"}:
        return disk_path

    raise ValueError(
        f"yaml_path '{yaml_path}' not found as a packaged resource or file on disk."
    )


def validate_mode(mode: Optional[str]) -> str:
    """
    Ensures mode is one of 'validate', 'diff', 'apply', 'all'.
    Defaults to 'apply'.
    """
    if not mode:
        return "apply"
    m = mode.lower().strip()
    allowed = {"validate", "diff", "apply", "all"}
    if m not in allowed:
        raise ValueError(f"mode must be one of {allowed}, got {mode!r}")
    return m


def validate_env(env: Optional[str]) -> Optional[str]:
    """
    Validates env for allowed characters (alphanumeric or underscore).
    """
    if not env:
        return None
    e = env.strip()
    if e and not re.fullmatch(r"[A-Za-z0-9_]+", e):
        raise ValueError(
            f"env may only contain letters, numbers, or underscores, got {env!r}"
        )
    return e


def validate_audit_log_table(audit_log_table: Any) -> Any:
    """
    Validates audit_log_table parameter. Accepts:
      - True, False, None
      - str ending in .yml/.yaml (path to the audit DDL YAML)
    Returns value as-is if valid.
    """
    if audit_log_table in (True, False, None):
        return audit_log_table
    if isinstance(audit_log_table, str):
        a = audit_log_table.strip()
        if a.lower().endswith((".yml", ".yaml")):
            return a
        else:
            raise ValueError(
                f"audit_log_table must be True, False, or a .yml/.yaml path (got {audit_log_table!r})"
            )
    raise ValueError(
        f"audit_log_table must be True, False, or a .yml/.yaml path (got type {type(audit_log_table).__name__})"
    )


def validate_spark(spark: Any) -> Any:
    """
    Minimal, environment-agnostic check that `spark` is an ACTIVE session.
    Works with classic SparkSession and Spark Connect.
    """
    if spark is None:
        raise ValueError("spark must not be None")

    # must expose a .sql(...) we can call
    sql_fn = getattr(spark, "sql", None)
    if not callable(sql_fn):
        raise ValueError("spark must expose a callable .sql(...) method")

    # quick 'ping' query to ensure it's alive
    try:
        df = sql_fn("SELECT 1")
        # Try a lightweight action; handle both classic and connect
        if hasattr(df, "limit") and callable(getattr(df, "limit")):
            df.limit(1).collect()
        elif hasattr(df, "collect") and callable(getattr(df, "collect")):
            df.collect()
        else:
            _ = getattr(df, "schema", None)
    except Exception as e:
        raise ValueError(f"spark session is not active/usable: {e.__class__.__name__}: {e}") from e

    return spark


def validate_params(
    yaml_path: str,
    mode: Optional[str],
    env: Optional[str],
    audit_log_table: Any,
    spark: Any,
) -> Tuple[str, Optional[str], Any]:
    """
    Validates and normalizes all run_table_load parameters.
    Returns (mode, env, audit_log_table) cleaned.
    Raises ValueError on invalid input.
    """
    validate_yaml_path(yaml_path)
    m = validate_mode(mode)
    e = validate_env(env)
    a = validate_audit_log_table(audit_log_table)
    validate_spark(spark)
    return m, e, a