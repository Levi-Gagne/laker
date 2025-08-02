# src/layker/yaml.py

import yaml
from typing import Any, Dict, List, Optional

class TableSchemaConfig:
    """
    Loader for YAML DDL config files. Exposes all config blocks with a clean API.
    Handles dynamic env, catalog suffixes, and nested constraints/keys.
    """

    def __init__(self, config_path: str, env: Optional[str] = None):
        self.config_path = config_path
        self._env = env
        self._config: Dict[str, Any] = {}
        self.load_config()

    def load_config(self) -> None:
        try:
            with open(self.config_path, "r") as f:
                self._config = yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise ValueError(f"Error loading YAML configuration from {self.config_path}: {e}")

    @property
    def catalog(self) -> str:
        return self._config.get("catalog", "")

    @property
    def schema(self) -> str:
        return self._config.get("schema", "")

    @property
    def table(self) -> str:
        return self._config.get("table", "")

    @property
    def env(self) -> Optional[str]:
        return self._env

    @property
    def full_table_name(self) -> str:
        cat = self.catalog.strip()
        sch = self.schema.strip()
        tbl = self.table.strip()
        env = self.env
        if cat.endswith("_") and env:
            cat_full = f"{cat}{env}"
        else:
            cat_full = cat
        return f"{cat_full}.{sch}.{tbl}"

    @property
    def owner(self) -> str:
        return self._config.get("owner", "")

    @property
    def tags(self) -> Dict[str, Any]:
        return self._config.get("tags", {})

    @property
    def properties(self) -> Dict[str, Any]:
        return self._config.get("properties", {})

    @property
    def table_comment(self) -> str:
        return self.properties.get("comment", "")

    @property
    def table_properties(self) -> Dict[str, Any]:
        return self.properties.get("table_properties", {})

    @property
    def primary_key(self) -> List[str]:
        pk = self._config.get("primary_key", [])
        return pk if isinstance(pk, list) else [pk]

    @property
    def partitioned_by(self) -> List[str]:
        pb = self._config.get("partitioned_by", [])
        return pb if isinstance(pb, list) else [pb]

    @property
    def unique_keys(self) -> List[List[str]]:
        return self._config.get("unique_keys", [])

    @property
    def foreign_keys(self) -> Dict[str, Any]:
        return self._config.get("foreign_keys", {})

    @property
    def table_check_constraints(self) -> Dict[str, Any]:
        return self._config.get("table_check_constraints", {})

    @property
    def row_filters(self) -> Dict[str, Any]:
        return self._config.get("row_filters", {})

    @property
    def columns(self) -> List[Dict[str, Any]]:
        cols_dict = self._config.get("columns", {})
        cols_dict_str = {str(k): v for k, v in cols_dict.items()}
        sorted_keys = sorted(map(int, cols_dict_str.keys()))
        return [cols_dict_str[str(k)] for k in sorted_keys]

    def build_table_metadata_dict(self) -> Dict[str, Any]:
        # Return dict with keys in the exact order you want — relies on Python 3.7+ insertion order preservation
        result = {
            "full_table_name": self.full_table_name,
            "catalog": self.catalog,
            "schema": self.schema,
            "table": self.table,
            "primary_key": self.primary_key if self.primary_key else [],
            "foreign_keys": self.foreign_keys if self.foreign_keys else {},
            "unique_keys": self.unique_keys if self.unique_keys else [],
            "partitioned_by": self.partitioned_by if self.partitioned_by else [],
            "tags": self.tags if self.tags else {},
            "row_filters": self.row_filters if self.row_filters else {},
            "table_check_constraints": self.table_check_constraints if self.table_check_constraints else {},
            "table_properties": self.table_properties if self.table_properties else {},
            "comment": self.table_comment,
            "owner": self.owner,
            "columns": {},
        }

        # Numbered columns with requested keys & order
        for idx, col in enumerate(self.columns, 1):
            result["columns"][idx] = {
                "name": col.get("name", ""),
                "datatype": col.get("datatype", ""),
                "nullable": col.get("nullable", True),
                "active": col.get("active", True),
                "comment": col.get("comment", ""),
                "tags": col.get("tags", {}),
                "column_masking_rule": col.get("column_masking_rule", ""),
                "column_check_constraints": col.get("column_check_constraints", {}),
            }

        return result

    def describe(self) -> None:
        # Helper for dev/test use only
        print(f"Table: {self.full_table_name}")
        print(f"  Owner: {self.owner}")
        print(f"  Tags: {self.tags}")
        print(f"  Primary Key: {self.primary_key}")
        print(f"  Partitioned By: {self.partitioned_by}")
        print(f"  Unique Keys: {self.unique_keys}")
        print(f"  Foreign Keys: {self.foreign_keys}")
        print(f"  Table Check Constraints: {self.table_check_constraints}")
        print(f"  Row Filters: {self.row_filters}")
        print(f"  Table Properties: {self.table_properties}")
        print(f"  Columns:")
        for i, col in enumerate(self.columns, 1):
            print(
                f"    {i}: {col.get('name','')} ({col.get('datatype','')}, nullable={col.get('nullable', True)}) | "
                f"comment={col.get('comment','')}, tags={col.get('tags',{})}, active={col.get('active', True)}"
            )
            ccc = col.get("column_check_constraints", {})
            if ccc:
                print(f"      Column Check Constraints: {ccc}")