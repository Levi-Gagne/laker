# src/layker/output.py

from laker.color import Color

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
