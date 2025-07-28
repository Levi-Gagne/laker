# src/layker/steps/differences.py

from typing import Any, Dict, Tuple, Optional
from layker.introspector import TableIntrospector
from layker.sanitizer import sanitize_snapshot
from layker.differences import compute_diff, log_comparison
from layker.utils.color import Color
from layker.utils.printer import print_success, print_warning, print_error
from layker.validators.differences import validate_diff
from layker.validators.evolution import handle_schema_evolution
import sys

def introspect_and_diff(
    spark: Any,
    cfg: Dict[str, Any],
    fq: str,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    introspector = TableIntrospector(spark)
    raw_snap = introspector.snapshot(fq)
    clean_snap = sanitize_snapshot(raw_snap)
    diff = compute_diff(cfg, clean_snap)
    return raw_snap, clean_snap, diff

def maybe_log_ddl(
    yaml_path: Optional[str],
    cfg: Dict[str, Any],
    fq: str,
    raw_snap: Dict[str, Any],
    log_ddl: Optional[str],
    clean_snap: Optional[Dict[str, Any]] = None,
) -> None:
    if log_ddl:
        try:
            log_comparison(yaml_path, cfg, fq, raw_snap, log_ddl, clean_snap=clean_snap)
            print(f"{Color.b}{Color.ivory}Wrote comparison log to {Color.aqua_blue}{log_ddl}{Color.r}")
        except Exception as e:
            print_error(f"Could not write diff log: {e}")

def exit_if_no_changes(diff: Dict[str, Any]) -> None:
    if not any(diff.values()):
        print_success("No metadata changes detected; exiting cleanly. Everything is up to date.")
        sys.exit(0)

def exit_if_mode_diff(diff: Dict[str, Any], mode: str) -> None:
    if mode == "diff":
        print_warning(f"[DIFF] Proposed changes:")
        for k, v in diff.items():
            if v:
                print(f"{Color.b}{Color.aqua_blue}{k}:{Color.ivory} {v}{Color.r}")
        sys.exit(0)

def compute_and_validate_diff_controller(
    spark: Any,
    cfg: Dict[str, Any],
    fq: str,
    mode: str,
    yaml_path: Optional[str] = None,
    log_ddl: Optional[str] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Orchestrates the difference/validation micro-steps.
    Returns (diff, clean_snap).
    """
    try:
        raw_snap, clean_snap, diff = introspect_and_diff(spark, cfg, fq)
    except Exception as e:
        print_error(f"Error during introspection or diff: {e}")
        sys.exit(2)

    maybe_log_ddl(yaml_path, cfg, fq, raw_snap, log_ddl, clean_snap)
    exit_if_no_changes(diff)
    exit_if_mode_diff(diff, mode)

    validate_diff(cfg, clean_snap, diff)
    handle_schema_evolution(diff, clean_snap.get("tbl_props", {}))

    return diff, clean_snap