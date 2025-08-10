from .__about__ import __version__, __author__, __license__

# Public API surface (handy for users)
from .main import run_table_load  # noqa: F401

__all__ = [
    "__version__",
    "__author__",
    "__license__",
    "run_table_load",
]