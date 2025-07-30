# src/layker/utils/paths.py

import importlib.resources
from pathlib import Path
from typing import Optional, List


def file_on_disk(path_str: str) -> Optional[str]:
    """
    Returns the absolute path if it exists as a file on disk, else None.
    """
    p = Path(path_str)
    return str(p.resolve()) if p.is_file() else None


def is_volume_path(path_str: str) -> bool:
    """
    Checks if the path looks like a Databricks Volume path.
    Example: "/Volumes/my_catalog/my_schema/my_file.yaml"
    """
    return path_str.startswith("/Volumes/")


def list_resource_files(package: str) -> List[str]:
    """
    Returns a list of all resource file names (not full paths) in a package.
    E.g., list_resource_files("layker.resources") returns ["audit.yaml", "example.yaml"]
    """
    try:
        return [res.name for res in importlib.resources.files(package).iterdir() if res.is_file()]
    except Exception:
        return []


def resolve_resource_path(package: str, resource_name: str) -> Optional[str]:
    """
    Gets a path to a resource in the given package using importlib.resources.
    Returns the file path as a string if found, else None.
    """
    try:
        with importlib.resources.as_file(
            importlib.resources.files(package).joinpath(resource_name)
        ) as path:
            return str(path)
    except Exception:
        return None