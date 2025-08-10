# src/layker/utils/timer.py

def format_elapsed(seconds: float) -> str:
    """
    Format seconds as H:MM:SS.mmm (e.g., 0:00:01.237).
    Safe for short or long runtimes.
    """
    ms = int(round((seconds - int(seconds)) * 1000))
    s = int(seconds) % 60
    m = (int(seconds) // 60) % 60
    h = int(seconds) // 3600
    return f"{h}:{m:02d}:{s:02d}.{ms:03d}"
