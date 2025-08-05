import sys
import yaml
import re
from typing import Any, Dict, List, Tuple, Optional

# ---- VALIDATOR ----

class TableYamlValidator:
    REQUIRED_TOP_KEYS = ["catalog", "schema", "table", "columns", "properties"]
    OPTIONAL_TOP_KEYS = [
        "primary_key", "partitioned_by", "unique_keys", "foreign_keys",
        "table_check_constraints", "row_filters", "tags", "owner"
    ]

    REQUIRED_COL_KEYS = {"name", "datatype", "nullable", "active"}
    ALLOWED_OPTIONAL_COL_KEYS = {
        "comment", "tags", "column_masking_rule", "default_value", "variable_value", "column_check_constraints"
    }

    DISALLOWED_COMMENT_CHARS = ["\n", "\r", "\t", "'"]

    COMPLEX_TYPE_PATTERNS = [
        r"^array<.+>$", r"^struct<.+>$", r"^map<.+>$"
    ]

    ALLOWED_SPARK_TYPES = {
        "string": str, "int": int, "double": float, "float": float,
        "bigint": int, "boolean": bool, "binary": bytes,
        "date": str, "timestamp": str, "decimal": float,
    }

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

    @classmethod
    def validate_dict(cls, cfg: Dict[str, Any]) -> Tuple[bool, List[str]]:
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
        if not raw:
            errors.append("No columns defined. At least one column is required.")
            cols = {}
            nums = []
        else:
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

            # Column check constraints
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
        # Table-level check constraints
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
        # Row filters
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
        return (len(errors) == 0, errors)

# ---- SANITIZER ----

def sanitize_text(text: Any) -> str:
    t = str(text or "")
    clean = t.replace("\n", " ").replace("\r", " ").replace("\t", " ").strip()
    return clean.replace("'", "`")

def recursive_sanitize_comments(obj: Any, path: str = "") -> Any:
    if isinstance(obj, dict):
        for k, v in obj.items():
            curr = f"{path}.{k}" if path else k
            if path.endswith(".columns") and isinstance(v, dict) and "comment" in v:
                if isinstance(v["comment"], str):
                    v["comment"] = sanitize_text(v["comment"])
            else:
                recursive_sanitize_comments(v, curr)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            recursive_sanitize_comments(item, f"{path}[{i}]")
    return obj

def sanitize_metadata(cfg: Dict[str, Any]) -> Dict[str, Any]:
    props = cfg.setdefault("properties", {})
    if "comment" in props:
        raw = props["comment"] or ""
        lines = str(raw).splitlines()
        props["comment"] = "\n".join(line.strip() for line in lines)
    tbl_props = props.setdefault("table_properties", {})
    for k in list(tbl_props):
        tbl_props[k] = sanitize_text(tbl_props[k])
    tags = cfg.setdefault("tags", {})
    for k in list(tags):
        tags[k] = sanitize_text(tags[k])
    if "row_filters" in cfg:
        for rf in cfg["row_filters"].values():
            if "name" in rf:
                rf["name"] = sanitize_text(rf["name"])
            if "expression" in rf:
                rf["expression"] = sanitize_text(rf["expression"])
    if "table_check_constraints" in cfg:
        for c in cfg["table_check_constraints"].values():
            if "name" in c:
                c["name"] = sanitize_text(c["name"])
            if "expression" in c:
                c["expression"] = sanitize_text(c["expression"])
    return cfg

# ---- SNAPSHOT YAML BUILDER ----

def build_snapshot_yaml(cfg: Dict[str, Any], env: Optional[str] = None) -> Tuple[Dict[str, Any], str]:
    def _get_catalog():
        cat = cfg.get("catalog", "").strip()
        if cat.endswith("_") and env:
            return f"{cat}{env}"
        return cat

    def _get_schema(): return cfg.get("schema", "").strip()
    def _get_table(): return cfg.get("table", "").strip()

    fq = f"{_get_catalog()}.{_get_schema()}.{_get_table()}"

    def _get_tags(): return cfg.get("tags", {})
    def _get_props(): return cfg.get("properties", {})
    def _get_comment(): return _get_props().get("comment", "")

    def _get_columns_dict():
        cols_dict = cfg.get("columns", {})
        cols_dict_str = {str(k): v for k, v in cols_dict.items()}
        sorted_keys = sorted(map(int, cols_dict_str.keys()))
        col_result = {}
        for k in sorted_keys:
            col = cols_dict_str[str(k)]
            col_result[str(k)] = {
                "name": col.get("name", ""),
                "datatype": col.get("datatype", ""),
                "nullable": col.get("nullable", True),
                "active": col.get("active", True),
                "comment": col.get("comment", ""),
                "tags": col.get("tags", {}),
                "column_masking_rule": col.get("column_masking_rule", ""),
                "column_check_constraints": col.get("column_check_constraints", {}),
            }
        return col_result

    snapshot = {
        "full_table_name": fq,
        "catalog": _get_catalog(),
        "schema": _get_schema(),
        "table": _get_table(),
        "primary_key": cfg.get("primary_key", []),
        "foreign_keys": cfg.get("foreign_keys", {}),
        "unique_keys": cfg.get("unique_keys", []),
        "partitioned_by": cfg.get("partitioned_by", []),
        "table_tags": _get_tags(),
        "row_filters": cfg.get("row_filters", {}),
        "table_check_constraints": cfg.get("table_check_constraints", {}),
        "table_properties": _get_props().get("table_properties", {}),
        "comment": _get_comment(),
        "owner": cfg.get("owner", ""),
        "columns": _get_columns_dict(),
    }
    return snapshot, fq

# ---- MAIN ENTRY ----

def validate_and_snapshot_yaml(yaml_path: str, env: Optional[str] = None, mode: str = "all") -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    # 1. Load YAML
    try:
        with open(yaml_path, "r") as f:
            raw_cfg = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"YAML file not found: {yaml_path}")
        sys.exit(2)
    except yaml.YAMLError as e:
        print(f"YAML syntax error in {yaml_path}: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"Error loading or parsing YAML: {e}")
        sys.exit(2)

    # 2. Validate
    valid, errors = TableYamlValidator.validate_dict(raw_cfg)
    if not valid:
        print("Validation failed:")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    # 3. Sanitize
    cfg_clean = recursive_sanitize_comments(raw_cfg)
    cfg_clean = sanitize_metadata(cfg_clean)

    if mode == "validate":
        print("YAML validation passed.")
        sys.exit(0)

    # 4. Build snapshot
    snapshot_yaml, fq_table = build_snapshot_yaml(cfg_clean, env=env)
    return snapshot_yaml, fq_table