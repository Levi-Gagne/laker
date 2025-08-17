# src/layker/utils/printer.py
from __future__ import annotations

import os
import sys
import time
import re
from typing import Dict, List, Optional, Iterable, Union
from functools import wraps

from layker.utils.color import Color
from layker.utils.timer import format_elapsed


class Print:
    """
    Minimal, static printer utilities.

    Usage:
      print(f"{Print.INFO}Starting…")
      print(f"{Print.ERROR}Exception in table_exists({fq}): {e}")

    Also provides banner() + banner_timer() with H1..H6/app variants.
    """

    # --- env ---
    _ENABLED = (os.environ.get("NO_COLOR", "") == "") and bool(getattr(sys.stdout, "isatty", lambda: False)())
    _RESET   = getattr(Color, "r", "") if _ENABLED else ""

    # --- helpers ---
    @classmethod
    def _code(cls, name: str) -> str:
        return getattr(Color, name, "") if cls._ENABLED else ""

    @classmethod
    def _codes(cls, names: Iterable[str]) -> str:
        if not cls._ENABLED:
            return ""
        out: List[str] = []
        for n in names:
            if not isinstance(n, str):
                continue
            out.append(getattr(Color, n, ""))
        return "".join(out)

    # styling for the square brackets
    BRACKET_STYLE: List[str] = ["b", "sky_blue"]

    # Tag styles — include "r" per your preference so it’s explicit in the map.
    TAG_STYLES: Dict[str, List[str]] = {
        "ERROR":      ["b", "candy_red", "r"],
        "WARNING":    ["b", "yellow", "r"],
        "WARN":       ["b", "yellow", "r"],
        "INFO":       ["b", "sky_blue", "r"],
        "SUCCESS":    ["b", "green", "r"],
        "VALIDATION": ["b", "ivory", "r"],
        "DEBUG":      ["r"],  # plain bracket with immediate reset
    }

    # Build one token like: [ERROR] (bold + color) and RESET after.
    @classmethod
    def token(cls, label: str) -> str:
        lab = (label or "").upper()
        br  = cls._codes(cls.BRACKET_STYLE)
        lb  = cls._codes(cls.TAG_STYLES.get(lab, ["r"]))
        # bracket(open, styled) + reset + label(styled) + reset + bracket(close, styled) + reset
        return f"{br}[{cls._RESET}{lb}{lab}{cls._RESET}{br}]{cls._RESET}"

    # Optional brace replacement: "… {ERROR} …" -> colored token
    _TOKEN = re.compile(r"\{([A-Za-z0-9_]+)\}")
    @classmethod
    def tagify(cls, s: str) -> str:
        return cls._TOKEN.sub(lambda m: cls.token(m.group(1)), s)

    # ---------- banners ----------
    BANNERS: Dict[str, Dict[str, object]] = {
        "app": {"width": 72, "border": "sky_blue",  "text": "ivory"},
        "h1":  {"width": 72, "border": "aqua_blue", "text": "ivory"},
        "h2":  {"width": 64, "border": "green",     "text": "ivory"},
        "h3":  {"width": 56, "border": "sky_blue",  "text": "ivory"},
        "h4":  {"width": 48, "border": "aqua_blue", "text": "ivory"},
        "h5":  {"width": 36, "border": "green",     "text": "ivory"},
        "h6":  {"width": 24, "border": "ivory",     "text": "sky_blue"},
    }

    @classmethod
    def banner(cls, kind: str, title: str, *, shades: Optional[Dict[str, str]] = None, width: Optional[int] = None) -> str:
        k = kind.lower()
        cfg = cls.BANNERS.get(k)
        if not cfg:
            raise KeyError(f"Unknown banner kind '{kind}'. Known: {sorted(cls.BANNERS)}")
        w = int(width or cfg["width"])
        border_attr = str((shades or {}).get("border", cfg["border"]))
        text_attr   = str((shades or {}).get("text", cfg["text"]))

        border = cls._code(border_attr)
        text   = cls._code(text_attr)

        bar = f"{border}{Color.b}" + "═" * w + cls._RESET
        inner_width = max(0, w - 4)
        title_line = (
            f"{border}{Color.b}║ "
            f"{text}{Color.b}{title.center(inner_width)}{cls._RESET}"
            f"{border}{Color.b} ║{cls._RESET}"
        )
        return f"\n{bar}\n{title_line}\n{bar}"

    @classmethod
    def print_banner(cls, kind: str, title: str, *, shades: Optional[Dict[str, str]] = None, width: Optional[int] = None) -> None:
        print(cls.banner(kind, title, shades=shades, width=width))

    @classmethod
    def banner_timer(cls, title: Optional[str] = None, *, kind: str = "app", shades: Optional[Dict[str, str]] = None):
        def deco(fn):
            text = title or fn.__name__.replace("_", " ").title()
            @wraps(fn)
            def wrapped(*args, **kwargs):
                print(cls.banner(kind, f"START {text}", shades=shades))
                t0 = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - t0
                    print(cls.banner(kind, f"END {text} — finished in {format_elapsed(elapsed)}", shades=shades))
            return wrapped
        return deco


# Expose constants like Print.ERROR, Print.INFO, …
for _lab in list(Print.TAG_STYLES):
    setattr(Print, _lab, Print.token(_lab))