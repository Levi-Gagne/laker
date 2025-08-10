# src/layker/utils/printer.py

import time
import pprint
from functools import wraps
from typing import Optional

from layker.utils.color import Color
from layker.utils.timer import format_elapsed


# ===== Generic box builder (shared by all headers) =====
def _box(title: str, *, width: int, border_color: str, text_color: str) -> str:
    """
    Draw a box:
      ═══════════════════════════════════════════════
      ║                Your Title                   ║
      ═══════════════════════════════════════════════
    Width is the total width of the top/bottom bars.
    The inner text is centered to (width - 4).
    """
    bar = f"{border_color}{Color.b}" + "═" * width + Color.r
    inner_width = max(0, width - 4)
    title_line = (
        f"{border_color}{Color.b}║ "
        f"{text_color}{Color.b}{title.center(inner_width)}{Color.r}"
        f"{border_color}{Color.b} ║{Color.r}"
    )
    return f"\n{bar}\n{title_line}\n{bar}"


# ===== H1 / App banner (darkest, widest) =====
# Lakehouse Aligned YAML kit for Engineering Rules
def laker_app_banner(title: str) -> str:
    # 62 chars, dark border, light title
    return _box(
        title=title,
        width=62,
        border_color=Color.sky_blue,   # darkest in the theme
        text_color=Color.ivory,        # contrasting inner text
    )


# ===== H2 / Section header (medium) =====
def section_header(title: str) -> str:
    # 48 chars, slightly lighter border
    return _box(
        title=title,
        width=48,
        border_color=Color.aqua_blue,  # mid-tone
        text_color=Color.ivory,
    )


# ===== H3 / Sub-section header (lightest, smallest) =====
def subsection_header(title: str) -> str:
    # 36 chars, lightest border in the set (still readable)
    return _box(
        title=title,
        width=36,
        border_color=Color.green,      # lighter accent
        text_color=Color.ivory,
    )


# ===== Status helpers =====
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


# ===== Decorator: Start/End banner with elapsed timing (uses H1 styling) =====
def laker_banner(title: Optional[str] = None):
    """
    Decorator: prints a START app banner on entry and an END banner with elapsed time on exit.

    Example:
        @laker_banner("Run Table Load")
        def run_table_load(...): ...
    """
    def _decorate(fn):
        banner_title = title or fn.__name__.replace("_", " ").title()

        @wraps(fn)
        def _wrapped(*args, **kwargs):
            print(laker_app_banner(f"START {banner_title}"))
            t0 = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - t0
                print(laker_app_banner(
                    f"END {banner_title}  —  finished in {format_elapsed(elapsed)}"
                ))
        return _wrapped
    return _decorate