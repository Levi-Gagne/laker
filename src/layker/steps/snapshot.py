# src/layker/steps/snapshot.py

"""
{
    "table": {
        "fully_qualified_name": "",
        "catalog": "",
        "schema": "",
        "table": "",
        "owner": "",
        "comment": "",
        "table_properties": {},
        "table_tags": {
            "example_tag": ""
        },
        "table_check_constraints": {
            "constraint_1": {
                "name": "",
                "expression": ""
            }
        },
        "row_filters": [
            {
                "filter_name": "",
                "target_columns": ""
            }
        ],
        "partitioned_by": [""],
        "primary_key": [""],
        "columns": {
            1: {
                "column_name": "",
                "datatype": "",
                "comment": "",
                "nullable": None,
                "masking_rule": None,
                "column_tags": {
                    "example_tag": ""
                },
                "column_check_constraints": {
                    "constraint_1": {
                        "name": "",
                        "expression": ""
                    }
                }
            },
            2: {
                "column_name": "",
                "datatype": "",
                "comment": "",
                "nullable": None,
                "masking_rule": None,
                "column_tags": {
                    "example_tag": ""
                },
                "column_check_constraints": {
                    "constraint_1": {
                        "name": "",
                        "expression": ""
                    }
                }
            },
            3: {
                "column_name": "",
                "datatype": "",
                "comment": "",
                "nullable": None,
                "masking_rule": None,
                "column_tags": {
                    "example_tag": ""
                },
                "column_check_constraints": {
                    "constraint_1": {
                        "name": "",
                        "expression": ""
                    }
                }
            }
        }
    }
}
"""

SNAPSHOT_QUERIES = {
    "table_tags": {
        "table": "system.information_schema.table_tags",
        "columns": ["tag_name", "tag_value"],
    },
    "column_tags": {
        "table": "system.information_schema.column_tags",
        "columns": ["column_name", "tag_name", "tag_value"],
    },
    "row_filters": {
        "table": "system.information_schema.row_filters",
        "columns": ["filter_name", "target_columns"],
    },
    "constraint_table_usage": {
        "table": "system.information_schema.constraint_table_usage",
        "columns": ["constraint_name"],
    },
    "constraint_column_usage": {
        "table": "system.information_schema.constraint_column_usage",
        "columns": ["column_name", "constraint_name"],
    },
}

def build_metadata_sql(kind: str, fq_table: str) -> str:
    """
    Builds the SQL string for querying metadata tables using METADATA_QUERIES config.
    """
    
    catalog, schema, table = parse_fully_qualified_table_name(fq_table)

    config = SNAPSHOT_QUERIES.get(kind)
    if not config:
        raise ValueError(f"[ERROR] Unsupported metadata query kind: {kind}")

    columns = ", ".join(config["columns"])
    return f"""
        SELECT {columns}
        FROM {config['table']}
        WHERE catalog_name = '{catalog}'
          AND schema_name = '{schema}'
          AND table_name = '{table}'
    """