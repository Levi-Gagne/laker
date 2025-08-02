# src/layker/yaml.py

import yaml
from typing import Any, Dict, List, Optional, Union

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
    def owner(self) -> Optional[str]:
        return self._config.get("owner")

    @property
    def tags(self) -> Dict[str, Any]:
        return self._config.get("tags", {})

    @property
    def properties(self) -> Dict[str, Any]:
        return self._config.get("properties", {})

    @property
    def table_comment(self) -> Optional[str]:
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
        # Always return all possible keys, filling blanks as needed
        result = {
            "catalog": self.catalog,
            "schema": self.schema,
            "table": self.table,
            "full_table_name": self.full_table_name,
            "owner": self.owner if self.owner is not None else "",
            "tags": self.tags if self.tags else {},
            "properties": self.properties if self.properties else {},
            "table_comment": self.table_comment if self.table_comment is not None else "",
            "table_properties": self.table_properties if self.table_properties else {},
            "primary_key": self.primary_key if self.primary_key else [],
            "partitioned_by": self.partitioned_by if self.partitioned_by else [],
            "unique_keys": self.unique_keys if self.unique_keys else [],
            "foreign_keys": self.foreign_keys if self.foreign_keys else {},
            "table_check_constraints": self.table_check_constraints if self.table_check_constraints else {},
            "row_filters": self.row_filters if self.row_filters else {},
            "columns": [],
        }
        # Always show all columns, with expected keys
        for col in self.columns:
            result["columns"].append({
                "name": col.get("name", ""),
                "datatype": col.get("datatype", ""),
                "nullable": col.get("nullable", True),
                "active": col.get("active", True),
                "comment": col.get("comment", ""),
                "tags": col.get("tags", {}),
                "column_masking_rule": col.get("column_masking_rule", ""),
                "column_check_constraints": col.get("column_check_constraints", {}),
            })
        return result

    def describe(self) -> None:
        # Helper for dev/test use, not called in prod
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
                


#######################################


# Example usage in notebook or test script

from layker.yaml import TableSchemaConfig

# Path to your YAML in the resources folder
yaml_path = "/Workspace/Users/levi.gagne@claconnect.com/resources/example.yaml"

cfg = TableSchemaConfig(yaml_path, env="dev")
table_meta = cfg.build_table_metadata_dict()

import pprint
pprint.pprint(table_meta, width=120)