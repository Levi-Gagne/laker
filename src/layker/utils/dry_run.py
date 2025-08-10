# src/layker/utils/dry_run.py

from __future__ import annotations

from typing import List, Optional, Dict, Any

from layker.utils.printer import section_header, print_success, print_warning, print_error
from layker.utils.color import Color


class DryRun:
    """
    Capture and summarize dry-run output.

    - add(command):            log an arbitrary action/DDL/info line
    - add_sql(sql, desc):      convenience to log SQL with an optional description
    - add_check(...):          log a 'rule' check (name + SQL + optional counts/error)
    - summary():               pretty print everything
    - as_dict():               export a structured dict for testing or external reporting

    Parameters
    ----------
    echo : bool
        If True (default), prints each message as it's added (with [DRY RUN] prefix).
        If False, only prints during .summary().
    """

    def __init__(self, echo: bool = True) -> None:
        self.echo: bool = echo
        self.commands: List[str] = []
        self.rule_checks: List[Dict[str, Any]] = []

    # ---------- logging APIs ----------

    def add(self, command: str) -> None:
        """Log a generic dry-run action (SQL, DDL, info)."""
        msg = f"[DRY RUN] {command}"
        self.commands.append(msg)
        if self.echo:
            print(msg)

    def add_sql(self, sql: str, desc: Optional[str] = None) -> None:
        """Log a SQL statement with optional description."""
        line = f"SQL: {sql}" if not desc else f"{desc} | SQL: {sql}"
        self.add(line)

    def add_check(
        self,
        rule_name: str,
        sql: str,
        num_violations: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """
        Log a dry-run check for a rule (e.g., DQ constraint preview).
        """
        entry = {
            "rule_name": rule_name,
            "sql": sql,
            "violations": num_violations,
            "error": error,
        }
        self.rule_checks.append(entry)

        msg = f"[DRY RUN - RULE] {rule_name} | SQL: {sql}"
        if num_violations is not None:
            msg += f" | Violations: {num_violations}"
        if error:
            msg += f" | ERROR: {error}"
        if self.echo:
            print(msg)

    # ---------- utilities ----------

    def set_echo(self, echo: bool) -> None:
        """Enable/disable immediate printing."""
        self.echo = bool(echo)

    def merge(self, other: "DryRun") -> None:
        """Merge logs from another DryRun instance."""
        if not isinstance(other, DryRun):
            return
        self.commands.extend(other.commands)
        self.rule_checks.extend(other.rule_checks)

    def as_dict(self) -> Dict[str, Any]:
        """Export a structured representation (useful for tests)."""
        return {
            "commands": list(self.commands),
            "rule_checks": [dict(rc) for rc in self.rule_checks],
        }

    # ---------- output ----------

    def summary(self) -> None:
        """Pretty-print a full dry-run summary."""
        print(section_header("DRY RUN SUMMARY", color=Color.sky_blue))

        if not self.commands and not self.rule_checks:
            print_warning("No dry-run actions captured.")
            return

        if self.commands:
            print(f"{Color.b}{Color.sky_blue}-- Actions / SQL --{Color.r}")
            for cmd in self.commands:
                print(cmd)
        else:
            print_warning("No generic actions recorded.")

        if self.rule_checks:
            print(f"\n{Color.b}{Color.sky_blue}-- Rule Checks --{Color.r}")
            for rc in self.rule_checks:
                rule = rc.get("rule_name", "<unnamed>")
                sql = rc.get("sql", "")
                v   = rc.get("violations")
                err = rc.get("error")
                print(f"Rule: {rule}")
                print(f"  SQL: {sql}")
                print(f"  Violations: {v if v is not None else 'N/A'}")
                if err:
                    print_error(f"  ERROR: {err}")
        else:
            print_warning("No rule checks recorded.")

        print_success("End dry-run summary.")


# --- Backward-compatibility aliases (optional; safe to remove later) ---
DryRunLogger = DryRun
RuleDryRunLogger = DryRun