# src/layker/steps/loader.py

from typing import Any
from layker.loader import DatabricksTableLoader
from layker.utils.printer import print_success, print_error

def apply_loader_step(
    cfg: dict,
    spark: Any,
    dry_run: bool,
    fq: str,
    action_desc: str = "Operation"
) -> None:
    """
    Wrapper for creating or updating a table, with error handling and success output.
    """
    try:
        loader = DatabricksTableLoader(cfg, spark, dry_run=dry_run)
        loader.create_or_update_table()
        print_success(f"[SUCCESS] {action_desc} for {fq} completed.")
    except Exception as e:
        print_error(f"Could not {action_desc.lower()} for {fq}: {e}")
        import sys; sys.exit(2)