# src/layker/validators/params.py

from pathlib import Path
import re
from typing import Optional, Tuple
from pyspark.sql import SparkSession

def validate_params(
    yaml_path: str,
    log_ddl: Optional[str],
    mode: str,
    env: Optional[str],
    audit_log_table: Optional[str],
    spark: SparkSession
) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Validate & normalize run_table_load parameters.
    Returns (mode, env, audit_log_table) cleaned.
    Raises ValueError or FileNotFoundError on invalid input.
    """
    # 1) yaml_path: must end .yml/.yaml and exist or be a mount
    p = Path(yaml_path)
    if p.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError(f"yaml_path must end in .yml/.yaml, got {yaml_path!r}")
    if not p.is_file() and not str(p).startswith(("/mnt/", "dbfs:/")):
        raise FileNotFoundError(f"YAML file not found: {yaml_path!r}")

    # 2) log_ddl: optional, must end .yml/.yaml; ensure parent dir exists
    if log_ddl:
        ld = Path(log_ddl)
        if ld.suffix.lower() not in {".yml", ".yaml"}:
            raise ValueError(f"log_ddl must end in .yml/.yaml, got {log_ddl!r}")
        ld.parent.mkdir(parents=True, exist_ok=True)

    # 3) mode: one of the allowed set
    m = mode.lower().strip()
    allowed = {"validate", "diff", "apply", "all"}
    if m not in allowed:
        raise ValueError(f"mode must be one of {allowed}, got {mode!r}")

    # 4) env: optional label, only alphanum + underscore
    e = env.strip() if env else None
    if e and not re.fullmatch(r"[A-Za-z0-9_]+", e):
        raise ValueError(f"env may only contain letters, numbers, or underscores, got {env!r}")

    # 5) audit_log_table: optional, must be catalog.schema.table & exist
    a = audit_log_table.strip() if audit_log_table else None
    if a:
        if not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", a):
            raise ValueError(f"audit_log_table must be catalog.schema.table, got {audit_log_table!r}")
        if not spark._jsparkSession.sessionState().catalog().tableExists(a):
            raise ValueError(f"Audit log table not found: {a!r}")

    return m, e, a
