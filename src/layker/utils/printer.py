# src/layker/utils/printer.py

import time
import pprint
from functools import wraps
from typing import Optional

from layker.utils.color import Color
from layker.utils.time import format_elapsed


def section_header(title: str, color: str = Color.aqua_blue) -> str:
    bar = f"{color}{Color.b}" + "═" * 62 + Color.r
    title_line = f"{color}{Color.b}║ {title.center(58)} ║{Color.r}"
    return f"\n{bar}\n{title_line}\n{bar}"


def print_success(msg: str) -> None:
    print(f"{Color.b}{Color.green}✔ {msg}{Color.r}")


def print_warning(msg: str) -> None:
    print(f"{Color.b}{Color.yellow}! {msg}{Color.r}")


def print_error(msg: str) -> None:
    print(f"{Color.b}{Color.candy_red}✘ {msg}{Color.r}")


def print_dict(
    d: dict,
    name: Optional[str] = None,
    width: int = 120,
    sort_dicts: bool = False,
) -> None:
    """Pretty-print a dictionary with optional label, width, and sorting."""
    if name:
        print(f"{Color.b}{Color.sky_blue}{name}:{Color.r}{d}")
    pprint.pprint(d, width=width, sort_dicts=sort_dicts)


def laker_banner(title: Optional[str] = None, color: str = Color.aqua_blue):
    """
    Decorator: print a START banner on entry and an END banner with elapsed time on exit.

    Usage:
        @laker_banner("Run Table Load")
        def run_table_load(...):
            ...

        # Or omit the title to derive from function name:
        @laker_banner()
        def run_table_load(...):
            ...
    """
    def _decorate(fn):
        banner_title = title or fn.__name__.replace("_", " ").title()

        @wraps(fn)
        def _wrapped(*args, **kwargs):
            print(section_header(f"START {banner_title}", color=color))
            t0 = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - t0
                print(section_header(
                    f"END {banner_title}  —  finished in {format_elapsed(elapsed)}",
                    color=color,
                ))
        return _wrapped
    return _decorate