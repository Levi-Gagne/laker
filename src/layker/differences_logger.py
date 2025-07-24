# src/layker/differences_logger.py

import yaml
from pathlib import Path
from typing import Any, Dict

from layker.sanitizer import sanitize_snapshot

def log_comparison(
    yaml_path: str,
    cfg: Dict[str, Any],
    fq: str,
    raw_snap: Dict[str, Any],
    filepath: str
) -> None:
    """
    Write the raw introspector snapshot and the cleaned snapshot to `filepath` for debugging.
    Auto-creates parent dirs.
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        # Dump YAML config
        f.write(f"# YAML from {yaml_path}\n\n")
        yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False)

        # Dump the _raw_ introspector snapshot
        f.write(f"\n\n# --------- RAW introspector snapshot: {fq} ---------\n\n")
        yaml.safe_dump(raw_snap, f, default_flow_style=False, sort_keys=False)

        # Dump the cleaned snapshot too
        clean_snap = sanitize_snapshot(raw_snap)
        f.write(f"\n\n# --------- CLEANED snapshot: {fq} ---------\n\n")
        yaml.safe_dump(clean_snap, f, default_flow_style=False, sort_keys=False)

        # Highlight important blocks if present
        blocks = [
            ("Table-level CHECK constraints",   raw_snap.get("tbl_constraints")),
            ("YAML Row Filters",                cfg.get("row_filters")),
            ("Column-level CHECK constraints",  clean_snap.get("column_check_constraints")),
            ("Column Masking Rules",            clean_snap.get("col_masking_rules")),
            ("Column Default Values",           clean_snap.get("col_default_values")),
            ("Column Variable Values",          clean_snap.get("col_variable_values")),
            ("Foreign Keys",                    cfg.get("foreign_keys")),
            ("Unique Keys",                     cfg.get("unique_keys")),
            ("Primary Key",                     cfg.get("primary_key")),
            ("Partitioned By",                  cfg.get("partitioned_by")),
        ]
        for name, val in blocks:
            if val:
                f.write(f"\n# {name}:\n")
                yaml.safe_dump(val, f, default_flow_style=False, sort_keys=False)