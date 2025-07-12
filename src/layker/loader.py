# src/utils/load_table/table_loader.py

from typing import Any, Dict, List, Tuple
from pyspark.sql import SparkSession

from utils.dry_run import DryRunLogger
from utils.load_table.table_introspector import TableIntrospector
from utils.load_table.table_sanitizer import sanitize_snapshot, normalize_dict

class DatabricksTableLoader:
    """
    Given a sanitized & validated YAML cfg, and a SparkSession,
    orchestrates either CREATE or ALTER TABLE operations.
    """

    def __init__(self, cfg: Dict[str, Any], spark: SparkSession, dry_run: bool = False):
        self.cfg = cfg
        self.spark = spark
        self.introspector = TableIntrospector(spark)
        self.dry_run = dry_run
        self.dry_logger = DryRunLogger() if dry_run else None
        self._modifications: List[str] = []

    @staticmethod
    def escape_sql(text: Any) -> str:
        return str(text).replace("'", "''")

    def _run_or_log(self, sql: str) -> None:
        if self.dry_run:
            self.dry_logger.add(sql)
        else:
            self.spark.sql(sql)

    def _get_columns_list(self) -> List[Dict[str, Any]]:
        raw = self.cfg["columns"]
        norm = {str(k): v for k, v in raw.items()}
        nums = sorted(map(int, norm.keys()))
        return [
            norm[str(i)]
            for i in nums
            if norm[str(i)].get("active", True)
        ]

    def create_or_update_table(self) -> None:
        cat, sch, tbl = self.cfg["catalog"], self.cfg["schema"], self.cfg["table"]
        fq = f"{cat}.{sch}.{tbl}"

        if not self.introspector.table_exists(fq):
            self._create_table(fq)
        else:
            raw_snap = self.introspector.snapshot(fq)
            self._update_table(fq, raw_snap)

        # ── Final summary ──
        if self._modifications:
            print("[FINAL SUMMARY] Changes applied:")
            for m in self._modifications:
                print(f" - {m}")
        print(f"[SUCCESS] {fq} is now in sync with YAML.")

    def _create_table(self, fq: str) -> None:
        print(f"[INFO] Table {fq} does not exist; creating it.")
        cols = self._get_columns_list()

        # 1) Build column DDL
        col_defs = []
        for c in cols:
            seg = f"`{c['name']}` {c['datatype'].lower()}"
            if not c.get("nullable", True):
                seg += " NOT NULL"
            col_defs.append(seg)
        col_ddl = ",\n  ".join(col_defs)

        # 2) PK/Partition
        pk = self.cfg.get("primary_key", [])
        pk_ddl = f", PRIMARY KEY ({', '.join(pk)})" if pk else ""
        part = self.cfg.get("partitioned_by", [])
        part_ddl = f"PARTITIONED BY ({', '.join(part)})" if part else ""
        tcomm = self.cfg["properties"].get("comment", "")
        comm_ddl = f"COMMENT '{self.escape_sql(tcomm)}'" if tcomm else ""

        # 3) Table properties
        props = self.cfg["properties"].get("table_properties", {})
        props_ddl = ""
        if props:
            kvs = ",\n  ".join(
                f"'{self.escape_sql(k)}' = '{self.escape_sql(v)}'"
                for k, v in props.items()
            )
            props_ddl = f"TBLPROPERTIES (\n  {kvs}\n)"

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {fq} (
          {col_ddl}{pk_ddl}
        )
        USING DELTA
        {part_ddl}
        {comm_ddl}
        {props_ddl}
        """.replace("\n        ", "\n").strip()

        print("[SUMMARY] Creating new table.")
        print(ddl)
        self._run_or_log(ddl)
        self._modifications.append("Table created")

        # 4) Owner
        owner = self.cfg.get("owner")
        if owner:
            self._run_or_log(f"ALTER TABLE {fq} OWNER TO `{self.escape_sql(owner)}`")
            self._modifications.append("Owner set")

        # 5) Refresh so Spark sees the new table immediately
        if not self.dry_run:
            self.spark.catalog.refreshTable(fq)

        # 6) Table-level extras: properties, tags, constraints, filters, keys
        self.apply_table_properties(fq, props)
        self.apply_table_tags(fq, self.cfg.get("tags", {}))
        self.apply_table_constraints(fq)
        self.apply_table_row_filters(fq)
        self.apply_table_unique_keys(fq)
        self.apply_table_foreign_keys(fq)
        self.apply_column_comments_and_tags(fq, cols)
        self.apply_column_check_constraints(fq, cols)

    def _update_table(self, fq: str, raw_snap: Dict[str, Any]) -> None:
        print("[INFO] Table exists; applying updates.")
        snap = sanitize_snapshot(raw_snap)
        cols = self._get_columns_list()

        # 1) Table properties diff
        desired_props = self.cfg["properties"].get("table_properties", {})
        if normalize_dict(desired_props) != snap["tbl_props"]:
            print("[STEP] Applying table properties")
            props_sql = (
                f"ALTER TABLE {fq} SET TBLPROPERTIES (\n  "
                + ",\n  ".join(
                    f"'{self.escape_sql(k)}' = '{self.escape_sql(v)}'"
                    for k, v in desired_props.items()
                )
                + "\n)"
            )
            self._run_or_log(props_sql)
            self._modifications.append("Table properties updated")

        # 2) Table tags
        existing_tbl_tags = snap["tbl_tags"]
        for k, v in self.cfg.get("tags", {}).items():
            if existing_tbl_tags.get(k) != str(v):
                print(f"[STEP] Applying table tag: {k}")
                self._run_or_log(
                    f"ALTER TABLE {fq} SET TAGS "
                    f"('{self.escape_sql(k)}' = '{self.escape_sql(v)}')"
                )
                self._modifications.append(f"Table tag updated: {k}={v}")

        # 3) Table-level constraints, filters, keys
        self.apply_table_constraints(fq)
        self.apply_table_row_filters(fq)
        self.apply_table_unique_keys(fq)
        self.apply_table_foreign_keys(fq)

        # 4) Columns: rename / add / drop
        yaml_names = [c["name"] for c in cols]
        yaml_types = [c["datatype"].lower() for c in cols]
        tbl_names, tbl_types = zip(*snap["columns"]) if snap["columns"] else ((), ())

        # a) rename / in-place type checks
        for idx, (y_name, y_type) in enumerate(zip(yaml_names, yaml_types)):
            if idx < len(tbl_names):
                t_name, t_type = tbl_names[idx], tbl_types[idx]
                if y_name != t_name:
                    print(f"[STEP] Renaming column: {t_name} → {y_name}")
                    self._run_or_log(f"ALTER TABLE {fq} RENAME COLUMN {t_name} TO {y_name}")
                    self._modifications.append(f"Renamed: {t_name}→{y_name}")
                # illegal type-changes caught upstream

        # b) add new
        for y_name, y_type in zip(yaml_names[len(tbl_names):], yaml_types[len(tbl_names):]):
            print(f"[STEP] Adding column: {y_name}")
            self._run_or_log(f"ALTER TABLE {fq} ADD COLUMNS (`{y_name}` {y_type})")
            self._modifications.append(f"Added column: {y_name}")

        # c) drop extras
        extras = [n for n in tbl_names if n not in yaml_names]
        for col in extras:
            print(f"[STEP] Dropping column: {col}")
            self._run_or_log(f"ALTER TABLE {fq} DROP COLUMN {col}")
            self._modifications.append(f"Dropped column: {col}")

        # 5) Column comments & tags
        existing_comments = snap["comments"]
        existing_ctags   = snap["col_tags"]
        for c in cols:
            name = c["name"]
            desired_comm = c.get("comment", "")
            if desired_comm != existing_comments.get(name, ""):
                print(f"[STEP] Applying comment for column {name}")
                self._run_or_log(
                    f"ALTER TABLE {fq} ALTER COLUMN {name} COMMENT '{self.escape_sql(desired_comm)}'"
                )
                self._modifications.append(f"Comment updated: {name}")

            for tag_k, tag_v in c.get("tags", {}).items():
                prev = existing_ctags.get(name, {}).get(tag_k)
                if prev != str(tag_v):
                    print(f"[STEP] Applying tag {tag_k} for column {name}")
                    self._run_or_log(
                        f"ALTER TABLE {fq} ALTER COLUMN {name} "
                        f"SET TAGS ('{self.escape_sql(tag_k)}' = '{self.escape_sql(tag_v)}')"
                    )
                    self._modifications.append(f"Tag updated for {name}: {tag_k}={tag_v}")

        # 6) Column-level check constraints
        self.apply_column_check_constraints(fq, cols)

    # ---- Helpers for all table-level metadata/features ----

    def apply_table_properties(self, fq: str, table_properties: Dict[str, Any]) -> None:
        if not table_properties:
            return
        props_kv = ",\n  ".join(
            f"'{self.escape_sql(k)}' = '{self.escape_sql(v)}'"
            for k, v in table_properties.items()
        )
        sql = f"ALTER TABLE {fq} SET TBLPROPERTIES (\n  {props_kv}\n)"
        self._run_or_log(sql)
        self._modifications.append("Table properties updated")

    def apply_table_tags(self, fq: str, yaml_tags: Dict[str, Any]) -> None:
        existing = self.introspector.get_table_tags(fq)
        for k, v in yaml_tags.items():
            if existing.get(k) != str(v):
                sql = (
                    f"ALTER TABLE {fq} SET TAGS "
                    f"('{self.escape_sql(k)}' = '{self.escape_sql(v)}')"
                )
                self._run_or_log(sql)
                self._modifications.append(f"Table tag updated: {k}={v}")

    def apply_table_constraints(self, fq: str) -> None:
        """
        Apply table-level check constraints from YAML (actually supported!).
        """
        tcc = self.cfg.get("table_check_constraints", {})
        for cname, cdict in tcc.items():
            expr = cdict.get("expression")
            if expr:
                sql = f"ALTER TABLE {fq} ADD CONSTRAINT {cname} CHECK ({expr})"
                print(f"[STEP] Applying table check constraint: {sql}")
                self._run_or_log(sql)
                self._modifications.append(f"Table check constraint applied: {cname}")

    def apply_table_row_filters(self, fq: str) -> None:
        """
        Apply row filters from YAML. (Not supported in Delta Lake, so log)
        """
        rf = self.cfg.get("row_filters", {})
        for fname, fdict in rf.items():
            expr = fdict.get("expression")
            sql = f"-- [FUTURE] ALTER TABLE {fq} ADD ROW FILTER {fname} WHERE {expr}"
            print(f"[STEP] (skipped) Would apply row filter: {sql}")
            self._modifications.append(f"Row filter (not applied): {fname}")

    def apply_table_unique_keys(self, fq: str) -> None:
        """
        Apply unique key constraints from YAML. (Not supported, future)
        """
        uk = self.cfg.get("unique_keys", [])
        for idx, group in enumerate(uk):
            if group:
                name = f"uq_{'_'.join(group)}"
                cols = ", ".join(group)
                sql = f"-- [FUTURE] ALTER TABLE {fq} ADD CONSTRAINT {name} UNIQUE ({cols})"
                print(f"[STEP] (skipped) Would apply unique key: {sql}")
                self._modifications.append(f"Unique key (not applied): {name}")

    def apply_table_foreign_keys(self, fq: str) -> None:
        """
        Apply foreign key constraints from YAML. (Not supported, future)
        """
        fks = self.cfg.get("foreign_keys", {})
        for fk_name, fk in fks.items():
            ref_tbl = fk.get("reference_table")
            ref_cols = fk.get("reference_columns", [])
            cols = fk.get("columns", [])
            if ref_tbl and ref_cols and cols:
                col_str = ", ".join(cols)
                ref_col_str = ", ".join(ref_cols)
                sql = (
                    f"-- [FUTURE] ALTER TABLE {fq} "
                    f"ADD CONSTRAINT {fk_name} FOREIGN KEY ({col_str}) "
                    f"REFERENCES {ref_tbl} ({ref_col_str})"
                )
                print(f"[STEP] (skipped) Would apply foreign key: {sql}")
                self._modifications.append(f"Foreign key (not applied): {fk_name}")

    def apply_column_comments_and_tags(
        self, fq: str, columns: List[Dict[str, Any]]
    ) -> None:
        for col in columns:
            name = col["name"]
            comment = col.get("comment", "")
            if comment:
                print(f"[STEP] Applying comment for column {name}")
                self._run_or_log(
                    f"ALTER TABLE {fq} ALTER COLUMN {name} "
                    f"COMMENT '{self.escape_sql(comment)}'"
                )
                self._modifications.append(f"Comment set: {name}")

            for tag_k, tag_v in col.get("tags", {}).items():
                print(f"[STEP] Applying tag {tag_k} for column {name}")
                self._run_or_log(
                    f"ALTER TABLE {fq} ALTER COLUMN {name} "
                    f"SET TAGS ('{self.escape_sql(tag_k)}' = '{self.escape_sql(tag_v)}')"
                )
                self._modifications.append(f"Tag set for {name}: {tag_k}={tag_v}")

    def apply_column_check_constraints(
        self, fq: str, columns: List[Dict[str, Any]]
    ) -> None:
        for col in columns:
            name = col["name"]
            ccc = col.get("column_check_constraints", {})
            for cname, cdict in ccc.items():
                expr = cdict.get("expression")
                if expr:
                    # Table-level only; log as not applied
                    sql = f"-- [FUTURE] ALTER TABLE {fq} ALTER COLUMN {name} ADD CONSTRAINT {cname} CHECK ({expr})"
                    print(f"[STEP] (skipped) Would apply column check constraint: {sql}")
                    self._modifications.append(f"Column check constraint (not applied): {name}.{cname}")
