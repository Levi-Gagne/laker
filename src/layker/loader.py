# src/layker/loader.py

from typing import Any, Dict
from pyspark.sql import SparkSession

# ---- Centralized Loader Config ----
LOADER_CONFIG = {
    "add": {
        "primary_key": {
            "sql": "ALTER TABLE {fq} ADD PRIMARY KEY ({cols})",
            "desc": "ADD primary key: {cols}"
        },
        "partitioned_by": {
            "sql": "ALTER TABLE {fq} ADD PARTITIONED BY ({cols})",
            "desc": "ADD partition: {cols}"
        },
        "unique_keys": {
            "sql": "ALTER TABLE {fq} ADD CONSTRAINT uq_{key} UNIQUE ({cols})",
            "desc": "ADD unique key: {cols}"
        },
        "foreign_keys": {
            "sql": (
                "ALTER TABLE {fq} ADD CONSTRAINT {name} "
                "FOREIGN KEY ({cols}) REFERENCES {ref_tbl} ({ref_cols})"
            ),
            "desc": "ADD foreign key: {name} ({cols})"
        },
        "table_check_constraints": {
            "sql": "ALTER TABLE {fq} ADD CONSTRAINT {name} CHECK ({expression})",
            "desc": "ADD table check constraint: {name}"
        },
        "row_filters": {
            "sql": "ALTER TABLE {fq} ADD ROW FILTER {name} WHERE {expression}",
            "desc": "ADD row filter: {name}"
        },
        "table_tags": {
            "sql": "ALTER TABLE {fq} SET TAGS ('{key}' = '{val}')",
            "desc": "ADD table tag: {key}={val}"
        },
        "owner": {
            "sql": "ALTER TABLE {fq} OWNER TO `{owner}`",
            "desc": "SET owner: {owner}"
        },
        "table_comment": {
            "sql": "ALTER TABLE {fq} SET COMMENT '{comment}'",
            "desc": "SET table comment"
        },
        "table_properties": {
            "sql": "ALTER TABLE {fq} SET TBLPROPERTIES ('{key}' = '{val}')",
            "desc": "ADD table property: {key}={val}"
        },
        "columns": {
            "sql": None  # Handled separately or via create
        }
    },
    "update": {
        "primary_key": {
            "sql": "ALTER TABLE {fq} ALTER PRIMARY KEY ({cols})",
            "desc": "UPDATE primary key: {cols}"
        },
        "table_check_constraints": {
            "sql": "ALTER TABLE {fq} ALTER CONSTRAINT {name} CHECK ({expression})",
            "desc": "UPDATE table check constraint: {name}"
        },
        "row_filters": {
            "sql": "ALTER TABLE {fq} ALTER ROW FILTER {name} WHERE {expression}",
            "desc": "UPDATE row filter: {name}"
        },
        "table_tags": {
            "sql": "ALTER TABLE {fq} SET TAGS ('{key}' = '{val}')",
            "desc": "UPDATE table tag: {key}={val}"
        },
        "owner": {
            "sql": "ALTER TABLE {fq} OWNER TO `{owner}`",
            "desc": "UPDATE owner: {owner}"
        },
        "table_comment": {
            "sql": "ALTER TABLE {fq} SET COMMENT '{comment}'",
            "desc": "UPDATE table comment"
        },
        "table_properties": {
            "sql": "ALTER TABLE {fq} SET TBLPROPERTIES ('{key}' = '{val}')",
            "desc": "UPDATE table property: {key}={val}"
        },
        "columns": {
            "sql": None  # Handled separately
        }
    },
    "remove": {
        "table_check_constraints": {
            "sql": "ALTER TABLE {fq} DROP CONSTRAINT {name}",
            "desc": "REMOVE table check constraint: {name}"
        },
        "row_filters": {
            "sql": "ALTER TABLE {fq} DROP ROW FILTER {name}",
            "desc": "REMOVE row filter: {name}"
        },
        "table_tags": {
            "sql": "ALTER TABLE {fq} UNSET TAGS ('{key}')",
            "desc": "REMOVE table tag: {key}"
        },
        "columns": {
            "sql": None  # Handled separately
        }
    }
}


class DatabricksTableLoader:
    """
    Loads, updates, or removes table metadata using a differences dictionary.
    Handles CREATE TABLE if the add block implies creation.
    Applies column comments and tags after CREATE for all columns.
    """
    def __init__(self, diff_dict: Dict[str, Any], spark: SparkSession, dry_run: bool = False):
        self.diff = diff_dict
        self.spark = spark
        self.dry_run = dry_run
        self.fq = diff_dict["full_table_name"]
        self.log = []

    def run(self):
        add = self.diff.get("add", {})
        update = self.diff.get("update", {})
        remove = self.diff.get("remove", {})
        columns = add.get("columns", {})

        # --- CREATE TABLE path ---
        if columns and str(min(map(int, columns.keys()))) == "1" and not update and not remove:
            self._create_table(add)
            print("[SUMMARY] Table CREATE complete:")
            for entry in self.log:
                print(f"  - {entry}")
            return

        # --- ALTER TABLE path ---
        for action in ["add", "update", "remove"]:
            section = self.diff.get(action, {})
            if section:
                self._handle_section(action, section)
        print("[SUMMARY] Table modifications complete:")
        for entry in self.log:
            print(f"  - {entry}")

    def _create_table(self, add_section):
        cols = add_section["columns"]
        col_sqls = []
        for idx in sorted(cols, key=lambda x: int(x)):
            col = cols[idx]
            name = col["name"]
            datatype = col["datatype"]
            nullable = col.get("nullable", True)
            col_sql = f"`{name}` {datatype}{' NOT NULL' if not nullable else ''}"
            col_sqls.append(col_sql)
        columns_sql = ",\n  ".join(col_sqls)

        # Partitioning
        partitioned_by = add_section.get("partitioned_by", [])
        partition_sql = f"\nPARTITIONED BY ({', '.join(partitioned_by)})" if partitioned_by else ""

        # Properties
        tbl_props = add_section.get("table_properties", {})
        tbl_props_sql = ""
        if tbl_props:
            props = [f"'{k}' = '{v}'" for k, v in tbl_props.items()]
            tbl_props_sql = f"\nTBLPROPERTIES ({', '.join(props)})"

        # Table comment
        tbl_comment = add_section.get("table_comment", "")
        tbl_comment_sql = f"\nCOMMENT '{tbl_comment}'" if tbl_comment else ""

        sql = f"CREATE TABLE {self.fq} (\n  {columns_sql}\n){partition_sql}{tbl_comment_sql}{tbl_props_sql}"

        self._run(sql, "CREATE TABLE")
        self.log.append(f"CREATE TABLE with columns: {list(c['name'] for c in cols.values())}")

        # --- ENSURE column comments/tags are always applied post-create ---
        self._handle_column_comments_and_tags(cols)

        # Handle table tags, owner, PK, unique, FKs, checks, etc. as ALTER TABLE after creation.
        self._handle_post_create(add_section)

    def _handle_column_comments_and_tags(self, cols):
        for idx in sorted(cols, key=lambda x: int(x)):
            col = cols[idx]
            name = col["name"]

            # Column comment
            comment = col.get("comment", "")
            if comment:
                sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} COMMENT '{comment}'"
                self._run(sql, f"ADD comment to {name}")

            # Column tags
            tags = col.get("tags") or {}
            for tag, value in tags.items():
                sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} SET TAGS ('{tag}' = '{value}')"
                self._run(sql, f"ADD tag {tag} to {name}")

    def _handle_post_create(self, add_section):
        for key, val in add_section.items():
            if key in ("columns", "table_properties", "table_comment", "partitioned_by"):
                continue
            meta = LOADER_CONFIG["add"].get(key)
            if not meta or not val:
                continue
            sql_template = meta.get("sql")
            if sql_template is None:
                continue  # Only columns handled separately, rest should all have sql
            if key == "primary_key" or key == "partitioned_by":
                sql = sql_template.format(fq=self.fq, cols=", ".join(val))
                self._run(sql, meta["desc"].format(cols=", ".join(val)))
            elif key == "unique_keys":
                for group in val:
                    sql = sql_template.format(fq=self.fq, key="_".join(group), cols=", ".join(group))
                    self._run(sql, meta["desc"].format(cols=", ".join(group), key="_".join(group)))
            elif key == "foreign_keys":
                for fk_name, fk in val.items():
                    sql = sql_template.format(
                        fq=self.fq,
                        name=fk_name,
                        cols=", ".join(fk.get("columns", [])),
                        ref_tbl=fk.get("reference_table", ""),
                        ref_cols=", ".join(fk.get("reference_columns", [])),
                    )
                    self._run(sql, meta["desc"].format(name=fk_name, cols=", ".join(fk.get("columns", []))))
            elif key == "table_check_constraints":
                for cname, cdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=cname, expression=cdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=cname))
            elif key == "row_filters":
                for fname, fdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=fname, expression=fdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=fname))
            elif key == "table_tags":
                for k, v in val.items():
                    sql = sql_template.format(fq=self.fq, key=k, val=v)
                    self._run(sql, meta["desc"].format(key=k, val=v))
            elif key == "owner":
                sql = sql_template.format(fq=self.fq, owner=val)
                self._run(sql, meta["desc"].format(owner=val))
            # No table_comment/table_properties/columns here

    def _handle_section(self, action: str, section_dict: Dict[str, Any]):
        config = LOADER_CONFIG[action]
        for key, meta in config.items():
            val = section_dict.get(key)
            if not val:
                continue
            sql_template = meta.get("sql")
            if sql_template is None:
                self._handle_columns(action, val)
                continue
            if key == "primary_key" or key == "partitioned_by":
                sql = sql_template.format(fq=self.fq, cols=", ".join(val))
                self._run(sql, meta["desc"].format(cols=", ".join(val)))
            elif key == "unique_keys":
                for group in val:
                    sql = sql_template.format(fq=self.fq, key="_".join(group), cols=", ".join(group))
                    self._run(sql, meta["desc"].format(cols=", ".join(group), key="_".join(group)))
            elif key == "foreign_keys":
                for fk_name, fk in val.items():
                    sql = sql_template.format(
                        fq=self.fq,
                        name=fk_name,
                        cols=", ".join(fk.get("columns", [])),
                        ref_tbl=fk.get("reference_table", ""),
                        ref_cols=", ".join(fk.get("reference_columns", [])),
                    )
                    self._run(sql, meta["desc"].format(name=fk_name, cols=", ".join(fk.get("columns", []))))
            elif key == "table_check_constraints":
                for cname, cdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=cname, expression=cdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=cname))
            elif key == "row_filters":
                for fname, fdict in val.items():
                    sql = sql_template.format(fq=self.fq, name=fname, expression=fdict.get("expression"))
                    self._run(sql, meta["desc"].format(name=fname))
            elif key == "table_tags":
                for k, v in val.items():
                    sql = sql_template.format(fq=self.fq, key=k, val=v)
                    self._run(sql, meta["desc"].format(key=k, val=v))
            elif key == "table_properties":
                for k, v in val.items():
                    sql = sql_template.format(fq=self.fq, key=k, val=v)
                    self._run(sql, meta["desc"].format(key=k, val=v))
            elif key == "owner":
                sql = sql_template.format(fq=self.fq, owner=val)
                self._run(sql, meta["desc"].format(owner=val))
            elif key == "table_comment":
                sql = sql_template.format(fq=self.fq, comment=val)
                self._run(sql, meta["desc"])
            else:
                sql = sql_template.format(fq=self.fq, val=val)
                self._run(sql, f"{action.upper()} {key}: {val}")

    def _handle_columns(self, action: str, columns: Dict[int, Dict[str, Any]]):
        if action == "add":
            for idx, col in columns.items():
                name = col.get("name")
                datatype = col.get("datatype")
                if not name or not datatype:
                    continue
                ddl = f"`{name}` {datatype}"
                if not col.get("nullable", True):
                    ddl += " NOT NULL"
                sql = f"ALTER TABLE {self.fq} ADD COLUMNS ({ddl})"
                self._run(sql, f"ADD column {name}")
                if col.get("comment"):
                    sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} COMMENT '{col['comment']}'"
                    self._run(sql, f"ADD comment to {name}")
                for tag, value in (col.get("tags") or {}).items():
                    sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} SET TAGS ('{tag}' = '{value}')"
                    self._run(sql, f"ADD tag {tag} to {name}")
                if col.get("column_masking_rule"):
                    self.log.append(f"ADD masking rule for {name} (not supported)")
                for cc_name, cc_def in (col.get("column_check_constraints") or {}).items():
                    expr = cc_def.get("expression", "")
                    self.log.append(f"ADD check constraint {cc_name} on {name}: {expr}")
        elif action == "update":
            for idx, col in columns.items():
                name = col.get("name")
                if not name:
                    continue
                if col.get("comment"):
                    sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} COMMENT '{col['comment']}'"
                    self._run(sql, f"UPDATE comment for {name}")
                for tag, value in (col.get("tags") or {}).items():
                    sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} SET TAGS ('{tag}' = '{value}')"
                    self._run(sql, f"UPDATE tag {tag} for {name}")
                if col.get("column_masking_rule"):
                    self.log.append(f"UPDATE masking rule for {name} (not supported)")
                for cc_name, cc_def in (col.get("column_check_constraints") or {}).items():
                    expr = cc_def.get("expression", "")
                    self.log.append(f"UPDATE check constraint {cc_name} on {name}: {expr}")
        elif action == "remove":
            for idx, col in columns.items():
                name = col.get("name")
                if name:
                    sql = f"ALTER TABLE {self.fq} DROP COLUMN {name}"
                    self._run(sql, f"REMOVE column {name}")
                for tag in (col.get("tags") or {}):
                    sql = f"ALTER TABLE {self.fq} ALTER COLUMN {name} UNSET TAGS ('{tag}')"
                    self._run(sql, f"REMOVE tag {tag} from {name}")
                for cc_name in (col.get("column_check_constraints") or {}):
                    self.log.append(f"REMOVE check constraint {cc_name} from {name}")

    def _run(self, sql, desc):
        if self.dry_run:
            print(f"[DRY RUN] {sql}")
        else:
            self.spark.sql(sql)
        self.log.append(desc)