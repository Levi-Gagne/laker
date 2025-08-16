# -*- coding: utf-8 -*-
from __future__ import annotations

import os, io
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString

from layker.snapshot_table import TableSnapshot
from layker.utils.spark import get_or_create_spark
from layker.utils.color import Color

def _yaml() -> YAML:
    y = YAML()
    y.indent(mapping=2, sequence=4, offset=2)
    y.preserve_quotes = True
    y.default_flow_style = False
    return y

def _block(s: Optional[str]) -> PreservedScalarString:
    return PreservedScalarString(s or "")

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

def _parse_schema(schema: str) -> Tuple[str, str]:
    parts = schema.split(".")
    if len(parts) != 2:
        raise ValueError("schema must be 'catalog.schema'")
    return parts[0], parts[1]

def _list_fq_tables(
    spark: SparkSession,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    include_views: bool = False,
) -> List[str]:
    base = "system.information_schema.tables"
    tfilter = "IN ('BASE TABLE','VIEW')" if include_views else "= 'BASE TABLE'"
    if schema:
        cat, sch = _parse_schema(schema)
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
        q = f"SELECT table_catalog, table_schema, table_name FROM {base} WHERE table_type {tfilter}"
    return [f"{r.table_catalog}.{r.table_schema}.{r.table_name}" for r in spark.sql(q).collect()]

def _filename(fqn: str, style: str) -> str:
    cat, sch, tbl = fqn.split(".")
    if style == "table":
        return f"{tbl}.yaml"
    if style == "schema_table":
        return f"{sch}__{tbl}.yaml"
    if style == "fqn":
        return f"{cat}__{sch}__{tbl}.yaml"
    raise ValueError("filename_style must be one of: table|schema_table|fqn")

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

class TableYamlExporter:
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        output_dir: str = "",
        filename_style: str = "table",
        include_views: bool = False,
        overwrite: bool = False,
    ):
        self.spark = spark or get_or_create_spark()
        self.output_dir = output_dir
        self.filename_style = filename_style
        self.include_views = include_views
        self.overwrite = overwrite

    def list_targets(
        self,
        *,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        tables: Optional[Iterable[str]] = None,
    ) -> List[str]:
        if table:
            return [table]
        if tables:
            return list(tables)
        if schema or catalog:
            return _list_fq_tables(self.spark, catalog=catalog, schema=schema, include_views=self.include_views)
        raise ValueError("Provide one of: table, tables, schema, or catalog")

    def snapshot(self, fqn: str) -> Optional[Dict[str, Any]]:
        return TableSnapshot(self.spark, fqn).build_table_metadata_dict()

    def write(self, fqn: str, yaml_text: str) -> str:
        _ensure_dir(self.output_dir)
        path = f"{self.output_dir.rstrip('/')}/{_filename(fqn, self.filename_style)}"
        _write_text(path, yaml_text, overwrite=self.overwrite)
        print(f"{Color.green}WROTE{Color.r}: {path}")
        return path

    def run(
        self,
        *,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        tables: Optional[Iterable[str]] = None,
    ) -> List[str]:
        targets = self.list_targets(catalog=catalog, schema=schema, table=table, tables=tables)
        written: List[str] = []
        for fqn in targets:
            meta = self.snapshot(fqn)
            if not meta:
                print(f"{Color.yellow}SKIP{Color.r}: {fqn} not found or not visible.")
                continue
            yaml_text = _render_yaml(meta)
            written.append(self.write(fqn, yaml_text))
        print(f"{Color.cyan}DONE{Color.r}: {len(written)} YAML file(s) -> {self.output_dir}")
        return written

# Thin functional wrapper (keeps your current imports stable if you prefer)
def dump_scope_to_yaml(
    *,
    spark: Optional[SparkSession] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
    tables: Optional[Iterable[str]] = None,
    output_dir: str,
    filename_style: str = "table",
    include_views: bool = False,
    overwrite: bool = False,
) -> List[str]:
    return TableYamlExporter(
        spark=spark,
        output_dir=output_dir,
        filename_style=filename_style,
        include_views=include_views,
        overwrite=overwrite,
    ).run(catalog=catalog, schema=schema, table=table, tables=tables)