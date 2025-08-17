# src/layker/cli.py

from __future__ import annotations
import argparse, sys
from typing import Optional

from layker.main import run_table_load
from layker.steps.yaml_table_dump import dump_scope_to_yaml
from importlib import resources

def _cmd_apply_like(mode: str, ns: argparse.Namespace) -> None:
    run_table_load(
        yaml_path=ns.yaml,
        dry_run=ns.dry_run,
        env=ns.env,
        mode=mode,
        audit_log_table=ns.audit,
    )

def _cmd_dump(ns: argparse.Namespace) -> None:
    dump_scope_to_yaml(
        catalog=ns.catalog,
        schema=ns.schema,
        table=ns.table,
        output_dir=ns.output,
        filename_style=ns.filename_style,
        include_views=ns.include_views,
        overwrite=ns.overwrite,
    )

def _cmd_template(ns: argparse.Namespace) -> None:
    # Copy packaged template to destination
    tpl = resources.files("layker.resources").joinpath("table_yaml_template.yaml")
    text = tpl.read_text(encoding="utf-8")
    with open(ns.output, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"WROTE: {ns.output}")

def main(argv: Optional[list[str]] = None) -> None:
    p = argparse.ArgumentParser(prog="layker", description="Layker CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    # apply/diff/validate share flags
    for cmd in ("apply", "diff", "validate"):
        sp = sub.add_parser(cmd)
        sp.add_argument("--yaml", required=True, help="Path to table YAML")
        sp.add_argument("--env", default=None)
        sp.add_argument("--dry-run", action="store_true")
        sp.add_argument("--audit", default=False, help="True|False|path-to-audit-yaml")
        sp.set_defaults(_fn=lambda ns, m=cmd: _cmd_apply_like(m, ns))

    dp = sub.add_parser("dump")
    scope = dp.add_mutually_exclusive_group(required=True)
    scope.add_argument("--catalog")
    scope.add_argument("--schema")
    scope.add_argument("--table")
    dp.add_argument("--output", required=True)
    dp.add_argument("--filename-style", default="table", choices=("table","schema_table","fqn"))
    dp.add_argument("--include-views", action="store_true")
    dp.add_argument("--overwrite", action="store_true")
    dp.set_defaults(_fn=_cmd_dump)

    tp = sub.add_parser("template")
    tp.add_argument("--output", required=True)
    tp.set_defaults(_fn=_cmd_template)

    ns = p.parse_args(argv)
    ns._fn(ns)

if __name__ == "__main__":
    main(sys.argv[1:])