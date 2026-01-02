#!/usr/bin/env python3
"""
progress_monitor_v1.0.0.py

Non-scrolling 3-line progress monitor that updates every second WITHOUT sleep().
Reads a JSON state file written by the main build process.
"""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path


def _fmt_hhmmss(seconds):
    if seconds is None:
        return "??:??:??"
    try:
        s = max(int(seconds), 0)
    except Exception:
        return "??:??:??"
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"


class ThreeLineTTY:
    def __init__(self):
        self._primed = False

    def render(self, l1: str, l2: str, l3: str):
        l1 = (l1 or "").replace("\n", " ")
        l2 = (l2 or "").replace("\n", " ")
        l3 = (l3 or "").replace("\n", " ")

        if not self._primed:
            sys.stdout.write("\n\n\n")
            sys.stdout.write("\x1b[3A")
            self._primed = True

        sys.stdout.write("\r\x1b[2K" + l1 + "\n")
        sys.stdout.write("\r\x1b[2K" + l2 + "\n")
        sys.stdout.write("\r\x1b[2K" + l3 + "\n")
        sys.stdout.write("\x1b[3A")
        sys.stdout.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True, help="Path to JSON progress state file")
    args = ap.parse_args()

    state_path = Path(args.state)
    tty = ThreeLineTTY()

    last_mtime = 0.0
    state = {}

    def _tick(_sig, _frame):
        nonlocal last_mtime, state
        try:
            if state_path.exists():
                st = state_path.stat()
                if st.st_mtime != last_mtime:
                    last_mtime = st.st_mtime
                    state = json.loads(state_path.read_text(encoding="utf-8", errors="ignore") or "{}")
        except Exception:
            pass

        now = time.time()
        started = state.get("started_at")
        elapsed = (now - started) if isinstance(started, (int, float)) else None

        overall_pct = state.get("overall_pct", 0.0)
        overall_eta = state.get("overall_eta_s")
        task_pct = state.get("task_pct", 0.0)
        task_eta = state.get("task_eta_s")
        sub_pct = state.get("sub_pct", 0.0)
        sub_eta = state.get("sub_eta_s")
        task_name = state.get("task_name", "")
        sub_name = state.get("sub_name", "")

        # If we haven't seen updates in a while, display staleness
        updated_at = state.get("updated_at")
        stale_s = (now - updated_at) if isinstance(updated_at, (int, float)) else None
        stale_note = f" | stale {_fmt_hhmmss(stale_s)}" if stale_s is not None and stale_s > 5 else ""

        l1 = f"OVERALL  {overall_pct:6.2f}% | ETA {_fmt_hhmmss(overall_eta)} | elapsed {_fmt_hhmmss(elapsed)}{stale_note}"
        l2 = f"TASK     {task_pct:6.2f}% | ETA {_fmt_hhmmss(task_eta)} | {task_name}"
        l3 = f"SUBTASK  {sub_pct:6.2f}% | ETA {_fmt_hhmmss(sub_eta)} | {sub_name}"
        tty.render(l1, l2, l3)

        if state.get("done") is True:
            # Drop below the 3 lines to not overwrite terminal prompt
            sys.stdout.write("\x1b[3B\n")
            sys.stdout.flush()
            raise SystemExit(0)

    signal.signal(signal.SIGALRM, _tick)
    signal.setitimer(signal.ITIMER_REAL, 0.01, 1.0)

    # Keep process alive without sleep; SIGALRM drives updates.
    while True:
        signal.pause()


if __name__ == "__main__":
    main()


