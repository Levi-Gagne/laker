# src/layker/steps/yaml_table_dump.py

from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString
from collections import OrderedDict
from typing import Any, Dict, Optional

def dict_to_cla_table_ddl_yaml(table_meta: Dict[str, Any], file_path: Optional[str] = None) -> str:
    """
    Convert table metadata dict from TableSnapshot into CLA-style YAML table DDL.
    If file_path is provided, writes YAML to that file. Always returns YAML as string.
    """
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.preserve_quotes = True
    yaml.default_flow_style = False

    # --- 1. Build the OrderedDict in CLA field order ---
    od = OrderedDict()
    od['catalog'] = table_meta['catalog']
    od['schema'] = table_meta['schema']
    od['table'] = table_meta['table']
    od['primary_key'] = table_meta.get('primary_key', []) or []
    od['partitioned_by'] = table_meta.get('partitioned_by', []) or []
    od['unique_keys'] = table_meta.get('unique_keys', []) or []

    # Foreign keys
    fk = table_meta.get('foreign_keys') or {}
    od['foreign_keys'] = fk if fk else {}

    # Table-level CHECK constraints
    od['table_check_constraints'] = table_meta.get('table_check_constraints', {}) or {}

    # Row-level filters
    od['row_filters'] = table_meta.get('row_filters', {}) or {}

    # Table-level tags
    tags = table_meta.get('table_tags', {}) or {}
    od['tags'] = tags

    od['owner'] = table_meta.get('owner', "")

    # Table properties/comments
    props_od = OrderedDict()
    if table_meta.get('comment'):
        props_od['comment'] = PreservedScalarString(table_meta['comment'])
    else:
        props_od['comment'] = ""
    if table_meta.get('table_properties'):
        props_od['table_properties'] = table_meta['table_properties']
    od['properties'] = props_od

    # ---- 2. Columns section ----
    # Sorted by key (1,2,3,...) if present, or by insertion order
    raw_columns = table_meta['columns']
    columns_od = OrderedDict()
    for k in sorted(raw_columns.keys(), key=int):
        col = raw_columns[k]
        col_od = OrderedDict()
        col_od['name'] = col['name']
        col_od['datatype'] = col['datatype']
        # nullable may be None, default True if missing
        col_od['nullable'] = bool(col.get('nullable', True))
        if 'comment' in col and col['comment']:
            col_od['comment'] = col['comment']
        if col.get('tags'):
            col_od['tags'] = col['tags']
        col_od['column_masking_rule'] = col.get('column_masking_rule', "")
        col_od['default_value'] = col.get('default_value', None)
        col_od['variable_value'] = col.get('variable_value', None)
        col_od['allowed_values'] = col.get('allowed_values', []) or []
        col_od['column_check_constraints'] = col.get('column_check_constraints', {}) or {}
        col_od['active'] = col.get('active', True)
        columns_od[k] = col_od
    od['columns'] = columns_od

    # ---- 3. Dump YAML ----
    import io
    buf = io.StringIO()
    yaml.dump(od, buf)
    text = buf.getvalue()

    if file_path:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(text)
    return text