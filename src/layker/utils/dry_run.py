# src/layker/dry_run.py

from typing import List, Tuple, Optional

class DryRunLogger:
    """
    Base dry run logger for capturing dry-run actions, SQL, or messages.
    """
    def __init__(self):
        self.commands: List[str] = []

    def add(self, command: str) -> None:
        """
        Log a generic dry-run action (SQL, DDL, info).
        """
        print(f"[DRY RUN] {command}")
        self.commands.append(command)

    def summary(self) -> None:
        """
        Print all captured dry run commands/messages.
        """
        print("\n--- Dry Run Summary ---")
        for cmd in self.commands:
            print(cmd)
        print("--- End Summary ---")

class RuleDryRunLogger(DryRunLogger):
    """
    Specialized dry run logger for DQ rule checks (e.g., SQL constraints).
    Captures SQL and violation counts for each rule.
    """
    def __init__(self):
        super().__init__()
        self.rule_checks: List[Tuple[str, str, Optional[int], Optional[str]]] = []

    def add_check(self, rule_name: str, sql: str, num_violations: Optional[int] = None, error: Optional[str] = None) -> None:
        """
        Log a dry run check for a DQ rule.
        """
        message = f"[DRY RUN - RULE] {rule_name} | SQL: {sql}"
        if num_violations is not None:
            message += f" | Violations: {num_violations}"
        if error:
            message += f" | ERROR: {error}"
        print(message)
        self.rule_checks.append((rule_name, sql, num_violations, error))

    def summary(self) -> None:
        super().summary()
        print("\n--- Dry Run Rule Checks ---")
        for rule_name, sql, num_violations, error in self.rule_checks:
            print(f"Rule: {rule_name}\n  SQL: {sql}\n  Violations: {num_violations if num_violations is not None else 'N/A'}")
            if error:
                print(f"  ERROR: {error}")
        print("--- End Rule Checks ---")