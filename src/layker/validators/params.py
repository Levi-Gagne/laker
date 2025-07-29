# src/layker/validators/params.py

import re
from pathlib import Path
from typing import Optional, Tuple, Any

def validate_yaml_path(yaml_path: str) -> str:
    p = Path(yaml_path)
    if p.suffix.lower() not in {".yml", ".yaml"}:
        raise ValueError(f"yaml_path must end in .yml/.yaml, got {yaml_path!r}")
    return str(p)

def validate_log_ddl(log_ddl: Optional[str]) -> Optional[str]:
    if log_ddl:
        ld = Path(log_ddl)
        if ld.suffix.lower() not in {".yml", ".yaml"}:
            raise ValueError(f"log_ddl must end in .yml/.yaml, got {log_ddl!r}")
        return str(ld)
    return None

def validate_mode(mode: str) -> str:
    m = mode.lower().strip()
    allowed = {"validate", "diff", "apply", "all"}
    if m not in allowed:
        raise ValueError(f"mode must be one of {allowed}, got {mode!r}")
    return m

def validate_env(env: Optional[str]) -> Optional[str]:
    e = env.strip() if env else None
    if e and not re.fullmatch(r"[A-Za-z0-9_]+", e):
        raise ValueError(f"env may only contain letters, numbers, or underscores, got {env!r}")
    return e

def validate_audit_log_table(audit_log_table: Any) -> Any:
    """
    Acceptable:
      - True: use default YAML path (handled downstream)
      - False: no audit logging
      - str: path ending .yml/.yaml (format only, not existence)
      - str: catalog.schema.table (format only, not existence)
    """
    if audit_log_table is True or audit_log_table is False or audit_log_table is None:
        return audit_log_table
    if isinstance(audit_log_table, str):
        a = audit_log_table.strip()
        # YAML path
        if a.lower().endswith((".yml", ".yaml")):
            return a
        # catalog.schema.table
        elif re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", a):
            return a
        else:
            raise ValueError(
                f"audit_log_table must be True, False, a .yml/.yaml path, or catalog.schema.table (got {audit_log_table!r})"
            )
    raise ValueError(
        f"audit_log_table must be True, False, a string path, or catalog.schema.table (got type {type(audit_log_table).__name__})"
    )

def validate_params(
    yaml_path: str,
    log_ddl: Optional[str],
    mode: str,
    env: Optional[str],
    audit_log_table: Any
) -> Tuple[str, Optional[str], Any]:
    """
    Validate & normalize run_table_load parameters.
    Returns (mode, env, audit_log_table) cleaned.
    Raises ValueError on invalid input.
    """
    validate_yaml_path(yaml_path)
    validate_log_ddl(log_ddl)
    m = validate_mode(mode)
    e = validate_env(env)
    a = validate_audit_log_table(audit_log_table)
    return m, e, a