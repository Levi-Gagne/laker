# src/layker/validators/yaml.py

import yaml
import re
from typing import Any, Dict, List, Tuple, Optional

class TableYamlValidator:
    """
    Validates table YAML for Databricks/Delta:
    - YAML structure, spelling, and config logic (YAML only)
    - Optionally: validates schema evolution requirements (only if table exists AND there are column changes)
    """

    # ---- Core YAML Format/Spelling Rules ----
    REQUIRED_TOP_KEYS = ["catalog", "schema", "table", "columns", "properties"]
    OPTIONAL_TOP_KEYS = [
        "primary_key", "partitioned_by", "unique_keys", "foreign_keys",
        "table_check_constraints", "row_filters", "tags", "owner"
    ]

    REQUIRED_COL_KEYS = {"name", "datatype", "nullable", "active"}
    ALLOWED_OPTIONAL_COL_KEYS = {
        "comment", "tags", "column_masking_rule", "default_value", "variable_value", "column_check_constraints"
    }

    # Delta schema evolution props (required only for add/drop/rename/reorder ops)
    REQUIRED_DELTA_PROPERTIES = {
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    }

    DISALLOWED_COMMENT_CHARS = ["\n", "\r", "\t", "'"]

    # Spark type patterns
    COMPLEX_TYPE_PATTERNS = [
        r"^array<.+>$", r"^struct<.+>$", r"^map<.+>$"
    ]

    ALLOWED_SPARK_TYPES = {
        "string": str, "int": int, "double": float, "float": float,
        "bigint": int, "boolean": bool, "binary": bytes,
        "date": str, "timestamp": str, "decimal": float,
        # complex types checked with regex below
    }

    # --- Internal helpers ---
    @staticmethod
    def _is_valid_sql_identifier(name: str) -> bool:
        return bool(re.match(r"^[a-z][a-z0-9_]*$", name.strip()))

    @classmethod
    def _is_valid_spark_type(cls, dt: str) -> bool:
        dt_lc = dt.lower()
        if dt_lc in cls.ALLOWED_SPARK_TYPES:
            return True
        return any(re.match(p, dt_lc) for p in cls.COMPLEX_TYPE_PATTERNS)

    @staticmethod
    def _is_fully_qualified_table(ref: str) -> bool:
        return ref.count('.') == 2

    # ---- YAML Structure Validation ----
    @classmethod
    def validate(cls, yaml_path: str) -> Tuple[bool, List[str]]:
        """
        Validate YAML from file, structure/logic only. Does not check any live table or evolution.
        """
        try:
            with open(yaml_path, "r") as f:
                docs = list(yaml.safe_load_all(f))
        except Exception as e:
            return False, [f"YAML syntax error: {e}"]

        if len(docs) != 1:
            return False, ["YAML must contain exactly one document."]
        return cls._validate_dict(docs[0])

    @classmethod
    def validate_dict(cls, cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate YAML structure/logic from already-loaded dict.
        """
        return cls._validate_dict(cfg)

    @classmethod
    def _validate_dict(cls, cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        # 1. Required top-level keys
        for key in cls.REQUIRED_TOP_KEYS:
            if key not in cfg or cfg[key] in (None, ""):
                errors.append(f"Missing top-level key: '{key}'")

        # 2. Table/catalog/schema identifier validity
        for k in ("catalog", "schema", "table"):
            v = cfg.get(k, "")
            if v and not cls._is_valid_sql_identifier(v.replace("_", "a").replace(".", "a")):
                errors.append(f"Invalid {k} name: '{v}'")

        # 3. Columns 1..N
        raw = cfg.get("columns", {})
        cols = {str(k): v for k, v in raw.items()}
        try:
            nums = sorted(map(int, cols.keys()))
            if nums != list(range(1, len(nums) + 1)):
                raise ValueError
        except Exception:
            errors.append(f"Column keys must be continuous 1..N, got {list(cols.keys())}")
            nums = []

        seen_names = set()
        all_col_names = []
        for i in nums:
            col = cols[str(i)]
            # Required keys
            missing = cls.REQUIRED_COL_KEYS - set(col.keys())
            if missing:
                errors.append(f"Column {i} missing keys: {sorted(missing)}")
            name = col.get("name")
            if not name or not cls._is_valid_sql_identifier(name):
                errors.append(f"Column {i} name '{name}' invalid")
            if name in seen_names:
                errors.append(f"Duplicate column name: '{name}'")
            seen_names.add(name)
            all_col_names.append(name)
            dt = col.get("datatype")
            if not dt or not cls._is_valid_spark_type(dt):
                errors.append(f"Column {i} datatype '{dt}' not allowed")
            if not isinstance(col.get("active"), bool):
                errors.append(f"Column {i} 'active' must be boolean")
            dv = col.get("default_value")
            dt_lc = dt.lower() if dt else ""
            if dt and dv not in (None, "") and dt_lc in cls.ALLOWED_SPARK_TYPES and dt_lc not in ("date", "timestamp"):
                exp = cls.ALLOWED_SPARK_TYPES.get(dt_lc)
                if dt_lc == "boolean":
                    if not isinstance(dv, bool) and not (isinstance(dv, str) and dv.lower() in ("true", "false")):
                        errors.append(f"Column {i} default '{dv}' invalid for boolean")
                else:
                    if not isinstance(dv, exp):
                        errors.append(f"Column {i} default '{dv}' does not match {dt}")
            cm = col.get("comment", "")
            bad = [ch for ch in cls.DISALLOWED_COMMENT_CHARS if ch in cm]
            if bad:
                errors.append(f"Column {i} comment contains {bad}")

            # ----- Column check constraints validation -----
            ccc = col.get("column_check_constraints", {})
            if ccc:
                if not isinstance(ccc, dict):
                    errors.append(f"Column {i} column_check_constraints must be a dict")
                else:
                    seen_constraint_names = set()
                    for cname, cdict in ccc.items():
                        if not isinstance(cdict, dict):
                            errors.append(f"Column {i} constraint '{cname}' must be a dict")
                        else:
                            if "name" not in cdict or "expression" not in cdict:
                                errors.append(f"Column {i} constraint '{cname}' missing 'name' or 'expression'")
                            name_val = cdict.get("name")
                            if name_val in seen_constraint_names:
                                errors.append(f"Column {i} has duplicate column_check_constraint name '{name_val}'")
                            seen_constraint_names.add(name_val)

        # 4. PK, partition, unique keys, etc: columns must exist
        def validate_columns_exist(field, value):
            for col in value:
                if col not in all_col_names:
                    errors.append(f"Field '{field}' references unknown column '{col}'")

        if "primary_key" in cfg:
            pk = cfg["primary_key"]
            pk_cols = pk if isinstance(pk, list) else [pk]
            validate_columns_exist("primary_key", pk_cols)
        if "partitioned_by" in cfg:
            pb = cfg["partitioned_by"]
            pb_cols = pb if isinstance(pb, list) else [pb]
            validate_columns_exist("partitioned_by", pb_cols)
        if "unique_keys" in cfg:
            uk = cfg["unique_keys"]
            if not isinstance(uk, list):
                errors.append("unique_keys must be a list of lists")
            else:
                for idx, group in enumerate(uk):
                    if not isinstance(group, list):
                        errors.append(f"unique_keys entry {idx} must be a list")
                        continue
                    validate_columns_exist(f"unique_keys[{idx}]", group)
        if "foreign_keys" in cfg:
            fks = cfg["foreign_keys"]
            if not isinstance(fks, dict):
                errors.append("foreign_keys must be a dict")
            else:
                for fk_name, fk in fks.items():
                    required_fk_keys = {"columns", "reference_table", "reference_columns"}
                    missing_fk = required_fk_keys - set(fk)
                    if missing_fk:
                        errors.append(f"Foreign key '{fk_name}' missing keys: {missing_fk}")
                        continue
                    validate_columns_exist(f"foreign_keys.{fk_name}.columns", fk["columns"])
                    ref_tbl = fk["reference_table"]
                    if not isinstance(ref_tbl, str) or not cls._is_fully_qualified_table(ref_tbl):
                        errors.append(f"Foreign key '{fk_name}' reference_table '{ref_tbl}' must be fully qualified (catalog.schema.table)")
                    ref_cols = fk["reference_columns"]
                    if not isinstance(ref_cols, list) or not all(isinstance(x, str) for x in ref_cols):
                        errors.append(f"Foreign key '{fk_name}' reference_columns must be a list of strings")
        # ----- Table-level check constraints -----
        if "table_check_constraints" in cfg:
            tcc = cfg["table_check_constraints"]
            if not isinstance(tcc, dict):
                errors.append("table_check_constraints must be a dict")
            else:
                names_seen = set()
                for cname, cdict in tcc.items():
                    if not isinstance(cdict, dict):
                        errors.append(f"table_check_constraints '{cname}' must be a dict")
                        continue
                    if "name" not in cdict or "expression" not in cdict:
                        errors.append(f"table_check_constraints '{cname}' missing 'name' or 'expression'")
                    name_val = cdict.get("name")
                    if name_val in names_seen:
                        errors.append(f"Duplicate table_check_constraint name: '{name_val}'")
                    names_seen.add(name_val)
        # ----- Row filters -----
        if "row_filters" in cfg:
            rf = cfg["row_filters"]
            if not isinstance(rf, dict):
                errors.append("row_filters must be a dict")
            else:
                names_seen = set()
                for fname, fdict in rf.items():
                    if not isinstance(fdict, dict):
                        errors.append(f"row_filters '{fname}' must be a dict")
                        continue
                    if "name" not in fdict or "expression" not in fdict:
                        errors.append(f"row_filters '{fname}' missing 'name' or 'expression'")
                    name_val = fdict.get("name")
                    if name_val in names_seen:
                        errors.append(f"Duplicate row_filter name: '{name_val}'")
                    names_seen.add(name_val)
        if "tags" in cfg and not isinstance(cfg["tags"], dict):
            errors.append("Top-level 'tags' must be a dict")
        if "owner" in cfg and not (cfg["owner"] is None or isinstance(cfg["owner"], str)):
            errors.append("'owner' must be a string or null")
        tbl_props = (cfg.get("properties", {}) or {}).get("table_properties", {})
        for k, v in cls.REQUIRED_DELTA_PROPERTIES.items():
            if tbl_props.get(k) != v:
                errors.append(f"Table property '{k}' must be '{v}', found '{tbl_props.get(k)}'")
        return (len(errors) == 0, errors)

    # ---- SCHEMA EVOLUTION CHECKS (only call these if table exists AND changes detected) ----
    @classmethod
    def check_type_changes(cls, cfg: Dict[str, Any], table_snapshot: Dict[str, Any]) -> None:
        """
        Prevents illegal in-place type changes (col name matches, type does not).
        Only call if table exists and there are schema changes.
        """
        yaml_cols = [
            (c["name"], c["datatype"].lower())
            for c in cfg["columns"].values()
            if c.get("active", True)
        ]
        table_cols = table_snapshot["columns"]
        for ((y_name, y_type), (t_name, t_type)) in zip(yaml_cols, table_cols):
            if y_name == t_name and y_type != t_type:
                raise ValueError(
                    f"Type mismatch for column '{y_name}': table={t_type} vs yaml={y_type}"
                )

    @classmethod
    def check_delta_properties(cls, tbl_props: Dict[str, Any]) -> None:
        """
        Require the necessary Delta properties for schema evolution.
        Raise ValueError if any are missing/incorrect.
        Only call if schema evolution is detected.
        """
        missing = {
            k: v for k, v in cls.REQUIRED_DELTA_PROPERTIES.items()
            if tbl_props.get(k) != v
        }
        if missing:
            msg = ", ".join(f"{k}={v}" for k, v in missing.items())
            raise ValueError(
                f"Schema evolution requires these Delta table properties: {msg}"
            )
