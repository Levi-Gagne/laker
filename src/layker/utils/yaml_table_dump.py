# src/layker/utils/yaml_table_dump.py

"""
START: create_yaml_exports

|
|-- 0) Normalize + banner
|     |-- Ensure Spark session (get_or_create_spark).
|     |-- Print START banner (Print.banner_timer decorator).
|
|-- 1) Resolve scope (exactly one of: table | tables | schema | catalog | pipeline)
|     |-- If input invalid → raise ValueError with {ERROR}.
|     |-- If table:
|     |     |-- Require FQN 'catalog.schema.table' (else ValueError).
|     |     |-- targets = [table]
|     |
|     |-- If tables:
|     |     |-- Keep only FQNs; non-FQNs are skipped with {WARN}.
|     |     |-- targets = unique FQNs
|     |
|     |-- If schema ('cat.schema'):
|     |     |-- Parse + validate.
|     |     |-- List tables:
|     |           * Prefer SDK: TableDiscovery.discover_schema_tables(list_columns=False)
|     |           * Fallback: Spark information_schema.tables
|     |     |-- targets = FQNs (views included only if include_views==True)
|     |
|     |-- If catalog:
|     |     |-- List schemas, then tables (SDK first; Spark fallback).
|     |     |-- targets = FQNs
|     |
|     |-- If pipeline:
|     |     |-- SDK required:
|     |           * TableDiscovery.discover_pipeline_tables(list_columns=False, assume_schema=...)
|     |     |-- If any names are not FQN and assume_schema not set → {WARN} and skip those.
|     |     |-- targets = FQNs only
|
|-- 2) Short-circuit if no targets
|     |-- Print {WARN} "No tables found for the requested scope. Nothing to do."
|     |-- EXIT cleanly (return [])
|
|-- 3) Snapshot loop (for each FQN in targets)
|     |-- Call TableSnapshot(spark, fqn).build_table_metadata_dict()
|     |     |-- Internally:
|     |           * table_exists() check
|     |           * DESCRIBE EXTENDED + information_schema reads
|     |           * Build canonical dict: columns, tags, checks, properties, owner, partitioning, PK/UK…
|     |-- If snapshot is None (table missing / not visible):
|     |     |-- Print {WARN} "SKIP: <fqn> not found / not visible."
|     |     |-- continue
|
|-- 4) YAML render
|     |-- Convert snapshot dict → CLA YAML (ruamel.yaml)
|     |-- Columns section preserves 1..N ordering and fields
|
|-- 5) Write file
|     |-- Ensure output_dir (local, /Volumes, or dbfs:/)
|     |-- Filename policy:
|     |     |-- "table"        → <table>.yaml
|     |     |-- "schema_table" → <schema>__<table>.yaml
|     |     |-- "fqn"          → <catalog>__<schema>__<table>.yaml
|     |-- Respect overwrite flag (refuse if False and exists).
|     |-- Print {SUCCESS} "Wrote <path>"
|
|-- 6) Summary
|     |-- Print {INFO} "Done. Created <N> YAML file(s) → <output_dir>"
|
|-- END: create_yaml_exports
|     |-- Banner shows total elapsed time.
"""

from __future__ import annotations

import os, io
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from pyspark.sql import SparkSession
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString

from layker.snapshot_table import TableSnapshot
from layker.utils.spark import get_or_create_spark
from layker.utils.printer import Print
from layker.utils.table import (
    parse_catalog_schema_fqn,
    is_fully_qualified_table_name,
)

# Prefer your TableDiscovery; fall back to None (we'll degrade for catalog/schema only)
try:
    # If you vend it under layker:
    from layker.utils.table_discovery import TableDiscovery  # type: ignore
except Exception:
    try:
        # Your original repo path:
        from datalake_pipeline_expectations.package.utils.table_discovery import TableDiscovery  # type: ignore
    except Exception:
        TableDiscovery = None  # type: ignore


# ---------------------------
# YAML helpers
# ---------------------------

def _yaml() -> YAML:
    y = YAML()
    y.indent(mapping=2, sequence=4, offset=2)
    y.preserve_quotes = True
    y.default_flow_style = False
    return y

def _block(s: Optional[str]) -> PreservedScalarString:
    return PreservedScalarString(s or "")


# ---------------------------
# IO helpers (local + DBFS)
# ---------------------------

def _ensure_dir(path: str) -> None:
    if path.startswith("dbfs:"):
        try:
            dbutils.fs.mkdirs(path)  # type: ignore[name-defined]
        except NameError:
            raise RuntimeError("dbutils not available. Use a /Volumes path or run in Databricks.")
    else:
        os.makedirs(path, exist_ok=True)

def _write_text(path: str, text: str, overwrite: bool = False) -> None:
    if path.startswith("dbfs:"):
        try:
            dbutils.fs.put(path, text, overwrite=overwrite)  # type: ignore[name-defined]
        except NameError:
            raise RuntimeError("dbutils not available. Use a /Volumes path or run in Databricks.")
    else:
        if (not overwrite) and os.path.exists(path):
            raise FileExistsError(f"Refusing to overwrite existing file: {path}")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)


# ---------------------------
# Fallback listing (Spark information_schema) — used only if TableDiscovery missing
# ---------------------------

def _list_fq_tables_via_infoschema(
    spark: SparkSession,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    include_views: bool = False,
) -> List[str]:
    base = "system.information_schema.tables"
    tfilter = "IN ('BASE TABLE','VIEW')" if include_views else "= 'BASE TABLE'"
    if schema:
        cat, sch = parse_catalog_schema_fqn(schema)
        q = f"""
          SELECT table_catalog, table_schema, table_name
          FROM {base}
          WHERE table_type {tfilter}
            AND table_catalog = '{cat}'
            AND table_schema  = '{sch}'
        """
    elif catalog:
        q = f"""
          SELECT table_catalog, table_schema, table_name
          FROM {base}
          WHERE table_type {tfilter}
            AND table_catalog = '{catalog}'
        """
    else:
        q = f"""
          SELECT table_catalog, table_schema, table_name
          FROM {base}
          WHERE table_type {tfilter}
        """
    return [f"{r.table_catalog}.{r.table_schema}.{r.table_name}" for r in spark.sql(q).collect()]


# ---------------------------
# Filename policy
# ---------------------------

def _filename(fqn: str, style: str) -> str:
    cat, sch, tbl = fqn.split(".")
    if style == "table":
        return f"{tbl}.yaml"
    if style == "schema_table":
        return f"{sch}__{tbl}.yaml"
    if style == "fqn":
        return f"{cat}__{sch}__{tbl}.yaml"
    raise ValueError("filename_style must be one of: table|schema_table|fqn")


# ---------------------------
# CLA YAML renderer (unchanged shape)
# ---------------------------

def _render_yaml(meta: Dict[str, Any]) -> str:
    doc = {
        "catalog": meta["catalog"],
        "schema": meta["schema"],
        "table": meta["table"],
        "primary_key": meta.get("primary_key", []) or [],
        "partitioned_by": meta.get("partitioned_by", []) or [],
        "unique_keys": meta.get("unique_keys", []) or [],
        "foreign_keys": meta.get("foreign_keys", {}) or {},
        "table_check_constraints": meta.get("table_check_constraints", {}) or {},
        "row_filters": meta.get("row_filters", {}) or {},
        "tags": meta.get("table_tags", {}) or {},
        "owner": meta.get("owner", "") or "",
        "table_comment": _block(meta.get("comment", "") or ""),
        "table_properties": meta.get("table_properties", {}) or {},
    }
    cols = {}
    for i in sorted(map(int, (meta.get("columns") or {}).keys())):
        c = meta["columns"][i]
        cols[i] = {
            "name": c["name"],
            "datatype": c["datatype"],
            "nullable": True if c.get("nullable", None) in (None, True) else False,
            "comment": c.get("comment", "") or "",
            "tags": c.get("tags", {}) or {},
            "column_masking_rule": c.get("column_masking_rule", "") or "",
            "default_value": c.get("default_value", None),
            "variable_value": c.get("variable_value", None),
            "allowed_values": c.get("allowed_values", []) or [],
            "column_check_constraints": c.get("column_check_constraints", {}) or {},
            "active": True if c.get("active", True) else False,
        }
    doc["columns"] = {i: cols[i] for i in sorted(cols.keys())}
    buf = io.StringIO(); _yaml().dump(doc, buf)
    return buf.getvalue()


# ---------------------------
# Exporter
# ---------------------------

class TableYamlExporter:
    """
    Build CLA-style YAMLs from live UC table metadata (via TableSnapshot).
    Scope can be a single table, a schema, a catalog, or a pipeline.

    • For pipeline discovery, we use TableDiscovery (SDK) and require FQN resolution. If you pass
      `assume_schema='catalog.schema'`, bare names from events are qualified before snapshotting.

    Parameters (ctor):
      - spark: SparkSession (optional; created if None)
      - output_dir: where YAML files are written (local, /Volumes, or dbfs:/)
      - filename_style: one of {"table","schema_table","fqn"}
      - include_views: include views in catalog/schema listing (default False)
      - overwrite: allow overwriting existing YAMLs (default False)
      - exclude_prefixes: list[str] of table name prefixes to skip (case-insensitive)
      - excludeTableRegex: simple case-insensitive prefix string to skip (no regex syntax)
      - include_views_for_discovery: pass-through to discovery for uniform behavior (default = include_views)

    Optional SDK discovery knobs:
      - sdk_include_views: include views when using TableDiscovery (defaults to include_views)
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        output_dir: str = "",
        filename_style: str = "table",
        include_views: bool = False,
        overwrite: bool = False,
        *,
        exclude_prefixes: Optional[Iterable[str]] = None,
        excludeTableRegex: Optional[str] = None,
        sdk_include_views: Optional[bool] = None,
    ):
        self.spark = spark or get_or_create_spark()
        self.output_dir = output_dir
        self.filename_style = filename_style
        self.include_views = include_views
        self.overwrite = overwrite

        self.exclude_prefixes = list(exclude_prefixes or [])
        self.excludeTableRegex = excludeTableRegex
        self.sdk_include_views = include_views if sdk_include_views is None else sdk_include_views

        self._disc: Optional["TableDiscovery"] = None
        if TableDiscovery is not None:
            try:
                self._disc = TableDiscovery(
                    include_views=self.sdk_include_views,
                    exclude_prefixes=self.exclude_prefixes,
                    excludeTableRegex=self.excludeTableRegex,
                    spark=self.spark,  # for DESCRIBE fallback when resolving columns (not needed here)
                )
            except Exception as e:
                print(f"{Print.WARN}Failed to initialize TableDiscovery; falling back to Spark for listing. ({e})")

    # ---------- Target resolution ----------

    def _list_targets_catalog(self, catalog: str) -> List[str]:
        if self._disc:
            try:
                return self._disc.discover_catalog_tables(catalog, list_columns=False)  # type: ignore[attr-defined]
            except Exception as e:
                print(f"{Print.WARN}SDK catalog listing failed for {catalog}; using information_schema. ({e})")
        return _list_fq_tables_via_infoschema(self.spark, catalog=catalog, include_views=self.include_views)

    def _list_targets_schema(self, schema_fqn: str) -> List[str]:
        if self._disc:
            try:
                return self._disc.discover_schema_tables(schema_fqn, list_columns=False)  # type: ignore[attr-defined]
            except Exception as e:
                print(f"{Print.WARN}SDK schema listing failed for {schema_fqn}; using information_schema. ({e})")
        return _list_fq_tables_via_infoschema(self.spark, schema=schema_fqn, include_views=self.include_views)

    def _list_targets_pipeline(self, pipeline: str, *, assume_schema: Optional[str]) -> List[str]:
        if not self._disc:
            raise RuntimeError("Pipeline discovery requires databricks-sdk. Install and retry.")
        # May return bare names without assume_schema; we will filter to FQNs and warn on the rest.
        names = self._disc.discover_pipeline_tables(  # type: ignore[attr-defined]
            pipeline_name=pipeline,
            list_columns=False,
            assume_schema=assume_schema,
        )
        fqdns = [n for n in names if is_fully_qualified_table_name(n)]
        non_fqn = sorted(set(n for n in names if not is_fully_qualified_table_name(n)))
        if non_fqn:
            if assume_schema:
                # In theory we qualified everything; this only happens for odd event names. Skip them.
                print(f"{Print.WARN}Skipping {len(non_fqn)} non-FQN names from pipeline events: {non_fqn}")
            else:
                print(f"{Print.WARN}Found {len(non_fqn)} non-FQN names in pipeline events; pass assume_schema='cat.schema' to qualify. Skipping: {non_fqn}")
        return fqdns

    def list_targets(
        self,
        *,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,   # 'cat.schema'
        table: Optional[str] = None,    # FQN 'cat.schema.tbl'
        tables: Optional[Iterable[str]] = None,
        pipeline: Optional[str] = None,
        assume_schema: Optional[str] = None,
    ) -> List[str]:
        # Exactly one of {table, tables, schema, catalog, pipeline}
        picks = [x is not None for x in (table, tables, schema, catalog, pipeline)]
        if sum(picks) != 1:
            raise ValueError("Provide exactly one of: table | tables | schema | catalog | pipeline")

        if table:
            if not is_fully_qualified_table_name(table):
                raise ValueError("table must be fully-qualified: 'catalog.schema.table'")
            return [table]

        if tables:
            out: List[str] = []
            for t in tables:
                if not is_fully_qualified_table_name(t):
                    print(f"{Print.WARN}Skipping non-FQN identifier in 'tables': {t!r}")
                    continue
                out.append(t)
            return sorted(set(out))

        if schema:
            # validate parse; function raises on bad input
            _ = parse_catalog_schema_fqn(schema)
            return self._list_targets_schema(schema)

        if catalog:
            return self._list_targets_catalog(catalog)

        # pipeline
        return self._list_targets_pipeline(pipeline, assume_schema=assume_schema)  # type: ignore[arg-type]

    # ---------- Snapshot + write ----------

    def snapshot(self, fqn: str) -> Optional[Dict[str, Any]]:
        return TableSnapshot(self.spark, fqn).build_table_metadata_dict()

    def write(self, fqn: str, yaml_text: str) -> str:
        _ensure_dir(self.output_dir)
        path = f"{self.output_dir.rstrip('/')}/{_filename(fqn, self.filename_style)}"
        _write_text(path, yaml_text, overwrite=self.overwrite)
        print(f"{Print.SUCCESS}Wrote {path}")
        return path

    @Print.banner_timer("Create YAML from Live Table(s)", kind="app")
    def run(
        self,
        *,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        tables: Optional[Iterable[str]] = None,
        pipeline: Optional[str] = None,
        assume_schema: Optional[str] = None,  # for pipeline bare-name qualification
    ) -> List[str]:
        # Resolve scope → target FQNs
        targets = self.list_targets(
            catalog=catalog,
            schema=schema,
            table=table,
            tables=tables,
            pipeline=pipeline,
            assume_schema=assume_schema,
        )
        if not targets:
            print(f"{Print.WARN}No tables found for the requested scope. Nothing to do.")
            return []

        print(f"{Print.INFO}Snapshotting {len(targets)} table(s)…")
        written: List[str] = []
        for fqn in targets:
            meta = self.snapshot(fqn)
            if not meta:
                print(f"{Print.WARN}SKIP: {fqn} not found / not visible.")
                continue
            yaml_text = _render_yaml(meta)
            written.append(self.write(fqn, yaml_text))

        print(f"{Print.INFO}Done. Created {len(written)} YAML file(s) → {self.output_dir or '.'}")
        return written


# ---------------------------
# Thin functional wrapper (optional)
# ---------------------------

def dump_scope_to_yaml(
    *,
    spark: Optional[SparkSession] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    tables: Optional[Iterable[str]] = None,
    pipeline: Optional[str] = None,
    assume_schema: Optional[str] = None,
    output_dir: str,
    filename_style: str = "table",
    include_views: bool = False,
    overwrite: bool = False,
    exclude_prefixes: Optional[Iterable[str]] = None,
    excludeTableRegex: Optional[str] = None,
) -> List[str]:
    """
    One-liner functional entry (mirrors TableYamlExporter).
    """
    return TableYamlExporter(
        spark=spark,
        output_dir=output_dir,
        filename_style=filename_style,
        include_views=include_views,
        overwrite=overwrite,
        exclude_prefixes=exclude_prefixes,
        excludeTableRegex=excludeTableRegex,
        sdk_include_views=include_views,
    ).run(
        catalog=catalog,
        schema=schema,
        table=table,
        tables=tables,
        pipeline=pipeline,
        assume_schema=assume_schema,
    )


# ---------------------------
# Notebook quick hits (commented)
# ---------------------------
# Example 1 — Catalog scope (schemas → tables), write to Volumes, table-name filenames
# TableYamlExporter(output_dir="/Volumes/team/ddl", filename_style="table").run(catalog="dq_dev")
#
# Example 2 — Schema scope with views included, FQN filenames
# TableYamlExporter(output_dir="/Volumes/team/ddl", filename_style="fqn", include_views=True).run(schema="dq_dev.lmg_sandbox")
#
# Example 3 — Single table
# TableYamlExporter(output_dir="/Volumes/team/ddl").run(table="dq_dev.lmg_sandbox.some_table")
#
# Example 4 — Pipeline scope (qualify bare names with assume_schema), skip tamarack* via exclude
# TableYamlExporter(
#     output_dir="/Volumes/team/ddl",
#     exclude_prefixes=["tamarack_"],   # case-insensitive 'startswith' filter
# ).run(pipeline="my_cdc_pipeline", assume_schema="dq_dev.lmg_sandbox")
#
# Example 5 — Functional wrapper (same as Example 2)
# dump_scope_to_yaml(output_dir="/Volumes/team/ddl", schema="dq_dev.lmg_sandbox", filename_style="fqn", include_views=True)