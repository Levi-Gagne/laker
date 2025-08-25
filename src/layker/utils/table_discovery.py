# src/layker/utils/table_discovery.py

"""
START: table_discovery
|
|-- 0) Entry
|     |-- Called via one-liner: TableDiscovery.tree(source, …)
|     |-- OR use instance methods: discover_catalog_tables / discover_schema_tables / discover_pipeline_tables / discover_tables
|     |
|     |-- Normalized inputs / options:
|           |-- is_pipeline: treat `source` as DLT pipeline name (no dots allowed)
|           |-- list_columns: also fetch {column: dtype} for each table
|           |-- include_views: include UC VIEW / MATERIALIZED_VIEW when listing
|           |-- exclude_prefixes / exclude_prefix: case-insensitive startswith filters on **table names** (not schemas)
|           |-- assume_schema: "catalog.schema" used to qualify bare names emitted by pipelines
|           |-- construct_tree: if True, render ASCII tree; if False, return structured payload only
|           |-- save_tree: False | True | "path-or-file.txt" (supports local paths and dbfs:/…)
|
|-- 1) Resolve "source" kind
|     |-- If is_pipeline == True:
|     |     |-- 1.a) Validate: error if `source` contains a "."
|     |     |-- 1.b) SDK: list pipelines → find one with name == source
|     |     |         |-- If none → [INFO] "Pipeline … not found" → return [] or {} (based on list_columns)
|     |     |-- 1.c) SDK: fetch latest update id; list pipeline events (paged)
|     |     |         |-- Collect origin.flow_name for events with the latest update id
|     |     |         |-- Result set = bare names or sometimes FQNs
|     |     |-- 1.d) Qualify bare names when possible:
|     |               |-- If assume_schema provided → ensure_fully_qualified(name, default_schema_fqn=assume_schema)
|     |               |-- Else keep as bare (unqualified)
|     |     |-- 1.e) Apply excludes (exclude_prefixes / exclude_prefix) to **table names** (final segment)
|     |     |-- 1.f) If list_columns:
|     |               |-- For resolvable FQNs only → fetch columns (see step 3); keep bare names with {} to signal unresolved
|     |     |-- 1.g) Build tree shape for render:
|     |               |-- Group by schema for FQNs:  {schema → {table → {}|{col:dtype}}}
|     |               |-- Group bare names under "<unqualified>"
|     |     |-- 1.h) Payload:
|     |               |-- construct_tree=True  → render ASCII + optional save; return (ascii, saved_path|None)
|     |               |-- construct_tree=False → return (list[str] | dict[str, dict], None)
|     |
|     |-- Else (catalog/schema/table inferred by dot-count):
|           |-- Count dots in `source`
|           |
|           |-- 2 dots  → TABLE mode (catalog.schema.table)
|           |     |-- Validate FQN format; no listing needed
|           |     |-- If list_columns → get columns (step 3) and build {schema → {table → {col:dtype}}}
|           |     |-- Else → {schema → {table → {}}}
|           |
|           |-- 1 dot   → SCHEMA mode (catalog.schema)
|           |     |-- Validate schema exists via SDK
|           |     |-- SDK: list tables in schema → filter out views (unless include_views) → apply excludes
|           |     |-- If list_columns → fetch per table (step 3)
|           |     |-- Build nested: {schema → {table → {}|{col:dtype}}}
|           |
|           |-- 0 dots  → CATALOG mode (catalog)
|                 |-- Validate catalog exists via SDK
|                 |-- SDK: list schemas → for each, list tables → filter views (unless include_views) → apply excludes
|                 |-- If list_columns → fetch per table (step 3)
|                 |-- Build nested: {schema → {table → {}|{col:dtype}}}
|
|-- 2) Exclusions & view handling
|     |-- exclude_prefixes: list[str] → drop table if tbl_name.lower().startswith(any prefix)
|     |-- exclude_prefix: single shorthand string → same rule as above
|     |-- include_views=False (default): drop UC VIEW and MATERIALIZED_VIEW (uses table_type)
|
|-- 3) Column materialization (single DRY point)
|     |-- Try SDK first: tables.get(name=|full_name=).columns
|     |     |-- Build {col_name: type_text|type_name}
|     |-- Fallback: Spark DESCRIBE <fqn> (only for FQNs)
|     |     |-- Parse rows until "# Partition Information"; {col_name: data_type}
|     |-- Failure to fetch → return {} for that table (keeps flow resilient)
|
|-- 4) ASCII rendering (only when construct_tree=True)
|     |-- Input nested dict: {schema → {table → {} or {col:dtype}}}
|     |-- Walk and print with guides:
|     |     |-- schemas/tables end with "/"
|     |     |-- leaf columns print "name : dtype"
|     |-- "spacious=True" inserts vertical spacer bars between sibling branches
|     |-- root label is the original `source` string
|
|-- 5) Saving (optional)
|     |-- save_tree == True  → write "./tree.txt"
|     |-- save_tree == "name_or_path.txt" → write to that path
|     |-- dbfs:/ paths supported (uses dbutils.fs.put); else local filesystem
|     |-- Overwrite = True (by design) — adjust here if you want guard rails
|
|-- 6) Returns
|     |-- construct_tree=True  → (ascii_text, saved_path|None)
|     |-- construct_tree=False → (tables_or_columns_payload, None)
|           |-- list_columns=False → List[str] of FQNs (pipeline may include bare names)
|           |-- list_columns=True  → Dict[str, Dict[col->dtype]]  (unqualified pipeline names map to `{}`)
|
|-- 7) Logging & error handling
|     |-- Permission/403 detection → [WARN] and skip where possible (schema/table listing); raise on pipeline list/events
|     |-- "not found" (catalog/schema/pipeline) → [INFO] and return empty result
|     |-- Other exceptions bubble up with context; DESCRIBE fallback failures print [WARN] and return {}
|
|-- 8) Performance notes
|     |-- SDK-first for enumeration (fast, metadata-only)
|     |-- Spark is used only for DESCRIBE fallback (per-table), minimizing expensive queries
|     |-- Exclude filters applied early to reduce downstream work
|
END: table_discovery
"""


from __future__ import annotations

from typing import Iterable, List, Optional, Tuple, Dict, Union
from pathlib import Path
import sys

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from utils.printer import Print
from utils.table import (
    is_view,
    is_fully_qualified_table_name,
    parse_catalog_schema_fqn,
    ensure_fully_qualified,
)

class TableDiscovery:
    """
    UC discovery with optional column materialization and optional ASCII tree rendering.
    - Primary API: discover_* methods (return lists/dicts)
    - Optional: tree(...) → print/save ASCII, or return structured data when construct_tree=False

    source rules (inferred):
      • "catalog"                  → catalog walk
      • "catalog.schema"           → schema walk
      • "catalog.schema.table"     → single table
      • "pipeline-name" + is_pipeline=True → pipeline walk
    """

    # -----------------------------------
    # ctor
    # -----------------------------------
    def __init__(
        self,
        *,
        sdk_client: Optional[WorkspaceClient] = None,
        include_views: bool = False,
        exclude_prefixes: Optional[Iterable[str]] = None,  # list of prefixes; case-insensitive
        exclude_prefix: Optional[str] = None,              # single shorthand; case-insensitive
        spark: Optional[SparkSession] = None,
    ) -> None:
        self.w = sdk_client or WorkspaceClient()
        self.spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        self.include_views = include_views
        self.exclude_prefixes = [p.lower() for p in (exclude_prefixes or [])]
        self.exclude_prefix_single = (exclude_prefix or "").lower().strip()

        # tolerate SDK surface differences
        self._tables   = getattr(self.w, "tables",  None) or getattr(getattr(self.w, "unity_catalog", None), "tables",  None)
        self._schemas  = getattr(self.w, "schemas", None) or getattr(getattr(self.w, "unity_catalog", None), "schemas", None)
        self._catalogs = getattr(self.w, "catalogs",None) or getattr(getattr(self.w, "unity_catalog", None), "catalogs",None)
        if not self._tables or not self._schemas:
            raise RuntimeError("WorkspaceClient missing UC tables/schemas API.")

    # -----------------------------------
    # One-liner (single entry point)
    # -----------------------------------
    @classmethod
    def tree(
        cls,
        source: str,
        *,
        is_pipeline: bool = False,
        list_columns: bool = False,
        construct_tree: bool = True,
        save_tree: Union[bool, str] = False,
        output_dir: Optional[Union[str, Path]] = None,
        assume_schema: Optional[str] = None,     # only used to qualify bare pipeline names
        include_views: bool = False,
        exclude_prefixes: Optional[Iterable[str]] = None,
        exclude_prefix: Optional[str] = None,
        spacious: bool = True,
        spark: Optional[SparkSession] = None,
        sdk_client: Optional[WorkspaceClient] = None,
    ):
        """
        If construct_tree=True → prints ASCII and (optionally) saves to disk/DBFS; returns (ascii, saved_path|None).
        If construct_tree=False → returns (structured_payload, None):
           - list_columns=False: List[str] tables
           - list_columns=True : Dict[str, Dict[col->dtype]]
        """
        td = cls(
            sdk_client=sdk_client,
            include_views=include_views,
            exclude_prefixes=exclude_prefixes,
            exclude_prefix=exclude_prefix,
            spark=spark,
        )

        root_label, nested, payload = td._build_from_source(
            source,
            is_pipeline=is_pipeline,
            list_columns=list_columns,
            assume_schema=assume_schema,
        )

        if not construct_tree:
            return payload, None

        text = td._render_ascii(root_label, nested, spacious=spacious)
        print(text)
        saved: Optional[Path] = None
        if save_tree:
            fname = td._normalize_txt_name(save_tree)
            out_dir = Path(output_dir) if output_dir else Path.cwd()
            saved = td._write_text(out_dir / fname, text, overwrite=True)
        return text, saved

    # -----------------------------------
    # Public: discovery (primary API)
    # -----------------------------------
    def discover_catalog_tables(self, catalog: str, *, list_columns: bool = False):
        self._assert_catalog_exists(catalog)
        schemas = self._list_schema_names(catalog)
        if not schemas:
            print(f"{Print.INFO}Catalog '{catalog}' has no visible schemas.")
            return {} if list_columns else []
        all_tables: List[str] = []
        for sch in schemas:
            try:
                all_tables.extend(self._list_tables_for_schema(catalog, sch))
            except Exception as e:
                if self._is_perm_error(str(e)):
                    print(f"{Print.WARN}Skipping {catalog}.{sch} (permission): {e}")
                    continue
                print(f"{Print.WARN}Skipping {catalog}.{sch}: {e}")
        all_tables = sorted(set(all_tables))
        if not all_tables:
            print(f"{Print.INFO}No tables found under catalog {catalog} (include_views={self.include_views}).")
            return {} if list_columns else []
        return self._materialize_columns(all_tables) if list_columns else all_tables

    def discover_schema_tables(self, schema_fqn: str, *, list_columns: bool = False):
        catalog, schema = parse_catalog_schema_fqn(schema_fqn)
        self._assert_schema_exists(catalog, schema)
        fqdns = self._list_tables_for_schema(catalog, schema)
        if not fqdns:
            print(f"{Print.INFO}No tables found under schema {catalog}.{schema} (include_views={self.include_views}).")
            return {} if list_columns else []
        return self._materialize_columns(fqdns) if list_columns else fqdns

    def discover_pipeline_tables(
        self,
        pipeline_name: str,
        *,
        list_columns: bool = False,
        assume_schema: Optional[str] = None,
        page_size: int = 250,
        empty_page_tolerance: int = 2,
    ):
        # pipelines list
        try:
            pipelines = list(self.w.pipelines.list_pipelines())
        except Exception as e:
            if self._is_perm_error(str(e)):
                raise RuntimeError(f"Permission error listing pipelines: {e}") from e
            raise
        pl = next((p for p in pipelines if p.name == pipeline_name), None)
        if not pl:
            print(f"{Print.INFO}Pipeline '{pipeline_name}' not found via SDK.")
            return {} if list_columns else []

        if not getattr(pl, "latest_updates", None):
            print(f"{Print.INFO}Pipeline '{pipeline_name}' has no recorded updates.")
            return {} if list_columns else []
        latest_update = pl.latest_updates[0].update_id

        # events
        try:
            events = self.w.pipelines.list_pipeline_events(pipeline_id=pl.pipeline_id, max_results=page_size)
        except Exception as e:
            if self._is_perm_error(str(e)):
                raise RuntimeError(f"Permission error listing events for pipeline '{pipeline_name}': {e}") from e
            raise

        names, empty = set(), 0
        it = iter(events)
        while True:
            page = []
            try:
                for _ in range(page_size): page.append(next(it))
            except StopIteration:
                pass
            page_names = {
                getattr(ev.origin, "flow_name", None)
                for ev in page
                if getattr(ev.origin, "update_id", None) == latest_update and getattr(ev.origin, "flow_name", None)
            }
            page_names.discard(None)
            if page_names:
                names |= page_names
                empty = 0
            else:
                empty += 1
            if empty >= empty_page_tolerance or not page:
                break

        if not names:
            print(f"{Print.INFO}No output tables found for pipeline '{pipeline_name}' via event logs.")
            return {} if list_columns else []

        # qualify bare names if possible
        out: List[str] = []
        for n in sorted(names):
            if is_fully_qualified_table_name(n):
                out.append(n)
            elif assume_schema:
                try:
                    out.append(ensure_fully_qualified(n, default_schema_fqn=assume_schema))
                except Exception as e:
                    print(f"{Print.WARN}Could not qualify '{n}' with {assume_schema}: {e}")
                    out.append(n)
            else:
                out.append(n)

        # excludes
        out = [t for t in out if self._keep_table_name(t.split(".")[-1])]

        if not list_columns:
            if not out:
                print(f"{Print.INFO}No tables remained after applying excludes for pipeline '{pipeline_name}'.")
            return out

        resolvable = [t for t in out if is_fully_qualified_table_name(t)]
        cols = {t: self._columns_for_table(t) for t in resolvable}
        return {t: cols.get(t, {}) for t in out}

    def discover_tables(self, tables: Iterable[str], *, list_columns: bool = False):
        """Accept explicit identifiers; keep only FQNs and apply excludes."""
        fqdns = [
            t for t in tables
            if is_fully_qualified_table_name(t) and self._keep_table_name(t.split(".")[-1])
        ]
        if not fqdns:
            print(f"{Print.INFO}No resolvable fully-qualified tables were provided.")
        return fqdns if not list_columns else {t: self._columns_for_table(t) for t in fqdns}

    # -----------------------------------
    # Internals: source → nested + payload (unified walker)
    # -----------------------------------
    def _build_from_source(
        self,
        source: str,
        *,
        is_pipeline: bool,
        list_columns: bool,
        assume_schema: Optional[str],
    ) -> Tuple[str, Dict[str, Dict[str, Union[Dict[str, str], str]]], Union[List[str], Dict[str, Dict[str, str]]]]:
        """
        Returns (root_label, nested_dict_for_tree, payload_for_non_tree).
        Nested dict shape is uniform: { schema -> { table -> {} or {col:dtype} } }
        For unresolved pipeline names, group under "<unqualified>".
        """
        if is_pipeline:
            if "." in source:
                raise ValueError("Pipeline names must not contain '.' when is_pipeline=True.")
            payload = self.discover_pipeline_tables(
                source, list_columns=list_columns, assume_schema=assume_schema
            )
            # payload can be list[str] or dict[str, dict]
            if list_columns:
                grouped: Dict[str, Dict[str, Dict[str, str]]] = {}
                for name, cmap in payload.items():  # type: ignore[assignment]
                    if is_fully_qualified_table_name(name):
                        _, schema, table = name.split(".")
                        grouped.setdefault(schema, {})[table] = dict(sorted(cmap.items()))
                    else:
                        grouped.setdefault("<unqualified>", {})[name] = dict(sorted(cmap.items()))
                nested = {s: dict(sorted(grouped[s].items())) for s in sorted(grouped)}
                return source, nested, payload
            else:
                grouped: Dict[str, Dict[str, Dict[str, str]]] = {}
                for name in payload:  # type: ignore[assignment]
                    if is_fully_qualified_table_name(name):
                        _, schema, table = name.split(".")
                        grouped.setdefault(schema, {})[table] = {}
                    else:
                        grouped.setdefault("<unqualified>", {})[name] = {}
                nested = {s: dict(sorted(grouped[s].items())) for s in sorted(grouped)}
                return source, nested, payload

        # not pipeline: infer from dot count
        dots = source.count(".")
        if dots == 2:  # table
            if not is_fully_qualified_table_name(source):
                raise ValueError("Expected fully-qualified 'catalog.schema.table'.")
            _, schema, table = source.split(".")
            if list_columns:
                cmap = self._columns_for_table(source)
                nested = {schema: {table: dict(sorted(cmap.items()))}}
                return source, nested, {source: cmap}
            else:
                nested = {schema: {table: {}}}
                return source, nested, [source]

        if dots == 1:  # schema
            if list_columns:
                cols = self.discover_schema_tables(source, list_columns=True)
                _, schema = source.split(".", 1)
                nested = {schema: {}}
                for fqn, cmap in cols.items():
                    table = fqn.split(".")[2]
                    nested[schema][table] = dict(sorted(cmap.items()))
                nested[schema] = dict(sorted(nested[schema].items()))
                return source, nested, cols
            else:
                tabs = self.discover_schema_tables(source, list_columns=False)
                _, schema = source.split(".", 1)
                nested = {schema: {}}
                for fqn in tabs:
                    nested[schema][fqn.split(".")[2]] = {}
                nested[schema] = dict(sorted(nested[schema].items()))
                return source, nested, tabs

        if dots == 0:  # catalog
            if list_columns:
                cols = self.discover_catalog_tables(source, list_columns=True)
                out: Dict[str, Dict] = {}
                for fqn, cmap in cols.items():
                    _, schema, table = fqn.split(".")
                    out.setdefault(schema, {})[table] = dict(sorted(cmap.items()))
                nested = {s: dict(sorted(out[s].items())) for s in sorted(out)}
                return source, nested, cols
            else:
                tabs = self.discover_catalog_tables(source, list_columns=False)
                out: Dict[str, Dict] = {}
                for fqn in tabs:
                    _, schema, table = fqn.split(".")
                    out.setdefault(schema, {})[table] = {}
                nested = {s: dict(sorted(out[s].items())) for s in sorted(out)}
                return source, nested, tabs

        raise ValueError("Invalid source. Use catalog | catalog.schema | catalog.schema.table, or set is_pipeline=True.")

    # -----------------------------------
    # Internals: render ASCII
    # -----------------------------------
    def _render_ascii(self, root_label: str, nested: Dict, *, spacious: bool) -> str:
        lines: List[str] = [f"{root_label}/"]

        def walk(node: Dict, prefix: str = ""):
            items = sorted(node.items(), key=lambda kv: kv[0].lower())
            for i, (name, child) in enumerate(items):
                is_last = i == len(items) - 1
                branch = "└── " if is_last else "├── "
                is_leaf_cols = isinstance(child, dict) and all(isinstance(v, str) for v in child.values())
                label = f"{name}/" if not is_leaf_cols else name
                lines.append(prefix + branch + label)
                next_prefix = prefix + ("    " if is_last else "│   ")
                if isinstance(child, dict):
                    if is_leaf_cols:
                        cols = sorted(child.items(), key=lambda kv: kv[0].lower())
                        for j, (cname, dtype) in enumerate(cols):
                            c_last = j == len(cols) - 1
                            c_branch = "└── " if c_last else "├── "
                            lines.append(next_prefix + c_branch + f"{cname} : {dtype}")
                        if spacious and not is_last:
                            lines.append(prefix + "│")
                    else:
                        walk(child, next_prefix)
                        if spacious and not is_last:
                            lines.append(prefix + "│")

        walk(nested)
        return "\n".join(lines) + "\n"

    # -----------------------------------
    # Internals: listing + columns
    # -----------------------------------
    def _assert_catalog_exists(self, catalog: str) -> None:
        try:
            if self._catalogs is not None:
                if not any(c.name == catalog for c in self._catalogs.list()):  # type: ignore[attr-defined]
                    raise RuntimeError(f"Catalog not found: {catalog}")
            else:
                if not any(True for _ in self._schemas.list(catalog_name=catalog)):  # type: ignore[attr-defined]
                    raise RuntimeError(f"Catalog not found: {catalog}")
        except Exception as e:
            if self._is_perm_error(str(e)):
                raise RuntimeError(f"Permission error verifying catalog '{catalog}': {e}") from e
            raise RuntimeError(f"Failed to verify catalog '{catalog}': {e}") from e

    def _assert_schema_exists(self, catalog: str, schema: str) -> None:
        try:
            if not any(s.name == schema for s in self._schemas.list(catalog_name=catalog)):  # type: ignore[attr-defined]
                raise RuntimeError(f"Schema not found: {catalog}.{schema}")
        except Exception as e:
            if self._is_perm_error(str(e)):
                raise RuntimeError(f"Permission error verifying schema '{catalog}.{schema}': {e}") from e
            raise RuntimeError(f"Failed to verify schema '{catalog}.{schema}': {e}") from e

    def _list_schema_names(self, catalog: str) -> List[str]:
        try:
            return [s.name for s in self._schemas.list(catalog_name=catalog)]  # type: ignore[attr-defined]
        except Exception as e:
            if self._is_perm_error(str(e)):
                raise RuntimeError(f"Permission error listing schemas for catalog '{catalog}': {e}") from e
            raise RuntimeError(f"Failed to list schemas for catalog '{catalog}': {e}") from e

    def _keep_table_name(self, tbl_name: str) -> bool:
        n = tbl_name.lower()
        if self.exclude_prefixes and any(n.startswith(p) for p in self.exclude_prefixes):
            return False
        if self.exclude_prefix_single and n.startswith(self.exclude_prefix_single):
            return False
        return True

    def _list_tables_for_schema(self, catalog: str, schema: str) -> List[str]:
        out: List[str] = []
        try:
            itr = self._tables.list(catalog_name=catalog, schema_name=schema)  # type: ignore[attr-defined]
        except Exception as e:
            if self._is_perm_error(str(e)):
                print(f"{Print.WARN}Permission error listing tables for {catalog}.{schema}: {e}")
                return out
            raise
        for t in itr:
            tname = getattr(t, "name", None)
            ttype = getattr(t, "table_type", None)
            if not tname:
                continue
            if (not self.include_views) and is_view(ttype):
                continue
            if not self._keep_table_name(tname):
                continue
            out.append(f"{t.catalog_name}.{t.schema_name}.{tname}")
        return out

    # ---- columns (single DRY point) ----
    def _tables_get(self, fqn: str):
        get = getattr(self._tables, "get", None)
        if get is None:
            raise RuntimeError("SDK client missing tables.get")
        try:
            return get(name=fqn)  # type: ignore[misc]
        except TypeError:
            return get(full_name=fqn)  # type: ignore[misc]

    def _columns_for_table(self, fqn: str) -> Dict[str, str]:
        # SDK
        try:
            info = self._tables_get(fqn)
            cols = getattr(info, "columns", None)
            if cols:
                out = {
                    getattr(c, "name"): str(getattr(c, "type_text", None) or getattr(c, "type_name", None))
                    for c in cols
                    if getattr(c, "name", None) and (getattr(c, "type_text", None) or getattr(c, "type_name", None))
                }
                if out:
                    return out
        except Exception:
            pass
        # Spark DESCRIBE
        try:
            rows = self.spark.sql(f"DESCRIBE {fqn}").collect()
        except Exception as e:
            print(f"{Print.WARN}DESCRIBE fallback failed for {fqn}: {e}")
            return {}
        out: Dict[str, str] = {}
        for r in rows:
            col = (getattr(r, "col_name", None) or "").strip()
            dt  = (getattr(r, "data_type", None) or "").strip()
            if not col or col.startswith("#"):
                if col == "# Partition Information":
                    break
                continue
            out[col] = dt
        return out

    def _materialize_columns(self, tables: Iterable[str]) -> Dict[str, Dict[str, str]]:
        return {t: self._columns_for_table(t) for t in tables}

    # ---- per-class utils ----
    @staticmethod
    def _normalize_txt_name(name: Union[str, bool, None]) -> str:
        if name is True or not name:
            return "tree.txt"
        p = Path(str(name).strip())
        return p.name if p.suffix.lower() == ".txt" else f"{p.stem}.txt"

    @staticmethod
    def _write_text(path: Union[str, Path], text: str, overwrite: bool = True) -> Path:
        s = str(path)
        if s.startswith("dbfs:"):
            try:
                dbutils.fs.put(s, text, overwrite=overwrite)  # type: ignore[name-defined]
                return Path(s)
            except NameError:
                print(f"{Print.WARN}dbutils not available; use a local/Volumes path instead.")
                raise RuntimeError("dbutils not available; use a local/Volumes path instead.")
        p = Path(s)
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.exists() and not overwrite:
            raise FileExistsError(f"Refusing to overwrite: {p}")
        p.write_text(text, encoding="utf-8")
        return p

    @staticmethod
    def _is_perm_error(msg: str) -> bool:
        m = msg.upper()
        return ("403" in m) or ("PERMISSION" in m) or ("UNAUTHORIZED" in m) or ("ACCESS DENIED" in m)
        


"""
# --- QUICK EXAMPLES ----------------------------------------------------------
from layker.utils.table_discovery import TableDiscovery

# 1) EVERYTHING ON: catalog → schemas → tables → columns(dtypes); print + save
TableDiscovery.tree(
    "dq_dev",
    list_columns=True,
    construct_tree=True,
    save_tree=True,                 # True => writes ./tree.txt ; or pass "dbfs:/tmp/foo.txt"
    include_views=False,
    exclude_prefixes=["tmp_", "stg_"],
    exclude_prefix="tama",          # single prefix shorthand
    spacious=True
)

# 2) PIPELINE (treat 'source' as pipeline name); qualify bare names via schema
TableDiscovery.tree(
    "my_dlt_pipeline",
    is_pipeline=True,
    list_columns=True,
    construct_tree=True,
    assume_schema="dq_dev.analytics"
)

# 3) STRUCTURED (no tree): get {table_fqn -> {col: dtype}} for a schema
data, _ = TableDiscovery.tree(
    "dq_dev.lmg_sandbox",
    list_columns=True,
    construct_tree=False
)
print(f"tables: {len(data)}")
# -----------------------------------------------------------------------------
"""     