# src/layker/yaml.py

import yaml
from typing import Any, Dict, List, Optional, Union
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, ArrayType, MapType, BinaryType, ByteType, DecimalType
)

SPARK_TYPE_MAP = {
    "string": StringType(),
    "str": StringType(),
    "varchar": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "bigint": LongType(),
    "long": LongType(),
    "smallint": IntegerType(),
    "short": IntegerType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "decimal": DecimalType(38, 18),
    "date": DateType(),
    "timestamp": TimestampType(),
    "array": ArrayType(StringType()),
    "map": MapType(StringType(), StringType()),
    "binary": BinaryType(),
    "byte": ByteType(),
}

class TableSchemaConfig:
    """
    Loader for YAML DDL config files. Exposes all config blocks with clean API.
    All logic for dynamic env, catalog, and nested constraints/keys is handled here.
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

    def get_foreign_key(self, name: str) -> Optional[Dict[str, Any]]:
        return self.foreign_keys.get(name)

    @property
    def table_check_constraints(self) -> Dict[str, Any]:
        return self._config.get("table_check_constraints", {})

    def get_check_constraint(self, key: str) -> Optional[Dict[str, Any]]:
        return self.table_check_constraints.get(key)

    @property
    def row_filters(self) -> Dict[str, Any]:
        return self._config.get("row_filters", {})

    def get_row_filter(self, key: str) -> Optional[Dict[str, Any]]:
        return self.row_filters.get(key)

    @property
    def columns(self) -> List[Dict[str, Any]]:
        cols_dict = self._config.get("columns", {})
        cols_dict_str = {str(k): v for k, v in cols_dict.items()}
        sorted_keys = sorted(map(int, cols_dict_str.keys()))
        return [cols_dict_str[str(k)] for k in sorted_keys]

    @property
    def column_names(self) -> List[str]:
        return [col["name"] for col in self.columns]

    @property
    def column_by_name(self) -> Dict[str, Dict[str, Any]]:
        return {col["name"]: col for col in self.columns}

    def get_column(self, col_name: str) -> Optional[Dict[str, Any]]:
        return self.column_by_name.get(col_name)

    @property
    def column_default_values(self) -> Dict[str, Any]:
        return {col["name"]: col.get("default_value", None) for col in self.columns}

    @property
    def column_variable_values(self) -> Dict[str, Any]:
        return {col["name"]: col.get("variable_value", None) for col in self.columns}

    @property
    def column_allowed_values(self) -> Dict[str, List[Any]]:
        return {col["name"]: col.get("allowed_values", []) for col in self.columns}

    def get_allowed_values(self, col_name: str) -> List[Any]:
        col = self.get_column(col_name)
        return col.get("allowed_values", []) if col else []

    def get_column_check_constraints(self, col_name: str, key: Optional[str] = None) -> Union[Dict[str, Any], Optional[Dict[str, Any]]]:
        col = self.get_column(col_name)
        ccs = col.get("column_check_constraints", {}) if col else {}
        if key:
            return ccs.get(key)
        return ccs

    @property
    def spark_struct_fields(self) -> List[StructField]:
        fields = []
        for col in self.columns:
            col_type = col["datatype"].lower()
            spark_type = SPARK_TYPE_MAP.get(col_type)
            if not spark_type:
                raise ValueError(f"Unsupported catalog datatype '{col_type}' in column '{col['name']}'.")
            fields.append(
                StructField(
                    col["name"],
                    spark_type,
                    col.get("nullable", True)
                )
            )
        return fields

    @property
    def spark_schema(self) -> StructType:
        return StructType(self.spark_struct_fields)

    def describe(self) -> None:
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
                f"    {i}: {col['name']} ({col['datatype']}, nullable={col.get('nullable', True)}) | "
                f"comment={col.get('comment', '')}, tags={col.get('tags', {})}, active={col.get('active', True)}"
            )
            allowed = col.get("allowed_values", [])
            if allowed:
                print(f"      Allowed Values: {allowed}")
            ccc = col.get("column_check_constraints", {})
            if ccc:
                print(f"      Column Check Constraints: {ccc}")