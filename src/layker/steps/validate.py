import sys
import yaml
from typing import Tuple, Optional, Dict, Any

from layker.sanitizer import recursive_sanitize_comments, sanitize_metadata
from layker.validators.yaml import TableYamlValidator
from layker.yaml import TableSchemaConfig
from layker.utils.color import Color
from layker.utils.printer import print_error, print_success

def validate_and_sanitize_yaml(
    yaml_path: str,
    env: Optional[str] = None,
) -> Tuple[TableSchemaConfig, Dict[str, Any], str]:
    """
    Validate and sanitize the YAML config file.

    Args:
        yaml_path: Path to the YAML file.
        env: Optional environment override.

    Returns:
        Tuple of (TableSchemaConfig, sanitized_cfg_dict, fully_qualified_table_name)

    Exits:
        sys.exit() with error message on any failure.
    """
    try:
        ddl_cfg = TableSchemaConfig(yaml_path, env=env)
        raw_cfg = ddl_cfg._config
    except FileNotFoundError as e:
        print_error(f"YAML file not found: {e}")
        sys.exit(2)
    except yaml.YAMLError as e:
        print_error(f"YAML syntax error in {yaml_path}: {e}")
        sys.exit(2)
    except Exception as e:
        print_error(f"Error loading or parsing YAML: {e}")
        sys.exit(2)

    try:
        cfg = recursive_sanitize_comments(raw_cfg)
        cfg = sanitize_metadata(cfg)
    except Exception as e:
        print_error(f"Error sanitizing YAML: {e}")
        sys.exit(2)

    try:
        valid, errors = TableYamlValidator.validate_dict(cfg)
    except Exception as e:
        print_error(f"Validation crashed: {e}")
        sys.exit(2)

    if not valid:
        print_error("Validation failed:")
        for err in errors:
            print(f"    {Color.candy_red}- {err}{Color.r}")
        sys.exit(1)
    print_success("YAML validation passed.")

    fq = ddl_cfg.full_table_name
    return ddl_cfg, cfg, fq