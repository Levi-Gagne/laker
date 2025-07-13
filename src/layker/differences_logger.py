# src/layker/differences_logger.py

import yaml
from pathlib import Path
from typing import Any, Dict

def log_comparison(
    yaml_path: str,
    cfg: Dict[str, Any],
    fq: str,
    clean_snap: Dict[str, Any],
    filepath: str
) -> None:
    """
    Write the sanitized YAML and sanitized table snapshot to `filepath` for debugging.
    Adds feature block separators if special sections exist. Auto-creates parent dirs.
    """
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        # Dump YAML config
        f.write(f"# YAML from {yaml_path}\n\n")
        yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False)

        # Dump snapshot
        f.write(f"\n\n# --------- Table snapshot: {fq} ---------\n\n")
        yaml.safe_dump(clean_snap, f, default_flow_style=False, sort_keys=False)

        # Highlight important blocks (if present, dump in order)
        blocks = [
            ("Table-level CHECK constraints",   clean_snap.get("tbl_constraints")),
            ("YAML Row Filters",                cfg.get("row_filters")),
            ("Column-level CHECK constraints",  clean_snap.get("col_col_constraints")),
            ("Column Masking Rules",            clean_snap.get("col_masking_rules")),
            ("Column Default Values",           clean_snap.get("col_default_values")),
            ("Column Variable Values",          clean_snap.get("col_variable_values")),
            ("Foreign Keys",                    cfg.get("foreign_keys")),
            ("Unique Keys",                     cfg.get("unique_keys")),
            ("Primary Key",                     cfg.get("primary_key")),
            ("Partitioned By",                  cfg.get("partitioned_by")),
        ]
        _ = [f.write(f"\n# {name}:\n") or yaml.safe_dump(val, f, default_flow_style=False, sort_keys=False)
             for name, val in blocks if val]

# No extra code, everything is covered and output is always formatted and readable.
