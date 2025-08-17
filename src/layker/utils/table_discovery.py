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


