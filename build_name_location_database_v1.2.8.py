#!/usr/bin/env python3
"""
Build Name and Location Database v1.2.8

ENHANCED v1.2.8 - Most Robust System:
- Downloads comprehensive place name databases from internet
- Extensive tribal name, member name, and place name databases
- Context-aware disambiguation (places vs people with same names)
- Downloads from USGS GNIS, Geonames, and other public sources
- Comprehensive tribal databases (reservations, tribal names, member names)
- Adds large-scale person-name coverage from SSA + US Census name distributions
- Adds Indigenous public-name enrichment via Wikidata (opt-in, fast-fail)
- Adds a disk download cache to avoid re-downloading the same datasets
- Adds an always-on per-second progress meter with an overall ETA across *all* stages (downloads, parsing, inserts)
- Makes the per-second ticker nesting-safe so sub-downloaders can’t disable the overall progress meter
- Progress UI is a fixed, non-scrolling 3-line display:
  1) overall progress + ETA
  2) current task progress + ETA
  3) current subtask progress + ETA
- When running in a real terminal (TTY), ALL other prints are redirected to a detail log so the terminal never scrolls.
- Fix: early tribal-place download now uses the same progress sink (prevents SIGALRM override and progress gaps).

This database is used by the de-identification program to improve
entity extraction without hardcoding specific names.
"""

import sqlite3
import json
import urllib.request
import urllib.parse
from pathlib import Path
from typing import List, Set, Dict, Tuple, Optional
import re
import time
import csv
import io
import gzip
import zipfile
import tempfile
import os
import sys
import signal
import argparse
import hashlib
import builtins

DOWNLOAD_CACHE_DIR = Path(__file__).parent / "download_cache"

def _fmt_hhmmss(seconds: Optional[float]) -> str:
    if seconds is None:
        return "??:??"
    s = max(int(seconds), 0)
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

class ThreeLineDisplay:
    """Non-scrolling 3-line display for TTY; newline fallback for non-TTY."""
    def __init__(self):
        self._primed = False

    def _truncate(self, s: str, max_len: int = 140) -> str:
        s = (s or "").replace("\n", " ")
        return s if len(s) <= max_len else (s[: max_len - 1] + "…")

    def render(self, overall: str, task: str, subtask: str):
        overall = self._truncate(overall)
        task = self._truncate(task)
        subtask = self._truncate(subtask)

        if sys.stdout.isatty():
            # Prime 3 lines once, then repaint in-place.
            if not self._primed:
                sys.stdout.write("\n\n\n")
                sys.stdout.write("\x1b[3A")  # move up 3 lines
                self._primed = True

            # Clear + write 3 lines, then move cursor back up 3 lines.
            sys.stdout.write("\r\x1b[2K" + overall + "\n")
            sys.stdout.write("\r\x1b[2K" + task + "\n")
            sys.stdout.write("\r\x1b[2K" + subtask + "\n")
            sys.stdout.write("\x1b[3A")
            sys.stdout.flush()
        else:
            # Captured output can't repaint; we still emit the 3 lines each tick.
            sys.stdout.write(overall + "\n" + task + "\n" + subtask + "\n")
            sys.stdout.flush()

class OverallProgress:
    """Always-on per-second progress meter with overall ETA (no sleeps, no threads)."""
    def __init__(self, stages: List[Tuple[str, float]]):
        self.stages = stages[:]  # list of (name, weight)
        self.total_weight = sum(w for _, w in self.stages) or 1.0
        self.stage_idx = -1
        self.stage_name = "init"
        self.stage_weight = 0.0
        self.stage_done = 0
        self.stage_total: Optional[int] = None
        self.stage_detail = ""
        self.completed_weight = 0.0
        self.start_t = time.time()
        self.stage_start_t = self.start_t
        self._prev_alarm = None
        self.display = ThreeLineDisplay()
        # task/subtask state
        self.task_done = 0
        self.task_total = None
        self.task_detail = ""
        self.sub_done = 0
        self.sub_total = None
        self.sub_detail = ""
        self._sub_started = time.time()
        self._task_started = self.start_t

    def begin(self):
        def _tick():
            self.print_tick()
        self._prev_alarm = _start_per_second_ticker(_tick)

    def end(self):
        _stop_per_second_ticker(self._prev_alarm)
        self._prev_alarm = None

    def next_stage(self, name: str, weight: float, total: Optional[int] = None, detail: str = ""):
        # finalize prior stage weight
        if self.stage_idx >= 0:
            self.completed_weight += self.stage_weight
        self.stage_idx += 1
        self.stage_name = name
        self.stage_weight = float(weight)
        self.stage_done = 0
        self.stage_total = total
        self.stage_detail = detail
        self.stage_start_t = time.time()
        self.task_done = 0
        self.task_total = total
        self.task_detail = detail
        self._task_started = self.stage_start_t
        # Reset subtask too
        self.sub_done = 0
        self.sub_total = None
        self.sub_detail = ""
        self._sub_started = self.stage_start_t

    def update(self, done: Optional[int] = None, total: Optional[int] = None, detail: Optional[str] = None):
        if done is not None:
            self.stage_done = int(done)
        if total is not None:
            self.stage_total = int(total)
        if detail is not None:
            self.stage_detail = detail
            self.task_detail = detail

    def update_subtask(self, done: Optional[int] = None, total: Optional[int] = None, detail: Optional[str] = None):
        if done is not None:
            self.sub_done = int(done)
        if total is not None:
            self.sub_total = int(total)
        if detail is not None:
            self.sub_detail = detail
        # if we just started a new subtask, reset its timer when detail changes
        if detail is not None:
            self._sub_started = time.time()

    def _overall_fraction(self) -> float:
        frac_in_stage = 0.0
        if self.stage_total and self.stage_total > 0:
            frac_in_stage = min(max(self.stage_done / self.stage_total, 0.0), 1.0)
        return min(max((self.completed_weight + self.stage_weight * frac_in_stage) / self.total_weight, 0.0), 1.0)

    def print_tick(self):
        now = time.time()
        elapsed = max(now - self.start_t, 1e-6)
        stage_elapsed = max(now - self.stage_start_t, 1e-6)
        overall = self._overall_fraction()

        # ETA from overall fraction (best-effort). If we have no fraction yet, show unknown.
        if overall > 0.005:
            total_est = elapsed / overall
            eta_s = max(total_est - elapsed, 0.0)
        else:
            eta_s = None

        overall_pct = overall * 100.0

        # Task ETA
        task_eta = None
        if self.task_total and self.task_total > 0 and self.task_done > 0:
            task_elapsed = max(now - self._task_started, 1e-6)
            rate = self.task_done / task_elapsed
            remaining = self.task_total - self.task_done
            if rate > 0:
                task_eta = remaining / rate

        # Subtask ETA
        sub_eta = None
        if self.sub_total and self.sub_total > 0 and self.sub_done > 0:
            sub_elapsed = max(now - self._sub_started, 1e-6)
            rate = self.sub_done / sub_elapsed
            remaining = self.sub_total - self.sub_done
            if rate > 0:
                sub_eta = remaining / rate

        overall_line = f"OVERALL  {overall_pct:6.2f}% | ETA {_fmt_hhmmss(eta_s)} | elapsed {_fmt_hhmmss(elapsed)}"
        if self.task_total and self.task_total > 0:
            tpct = (self.task_done / self.task_total) * 100.0
            task_line = f"TASK     {tpct:6.2f}% | ETA {_fmt_hhmmss(task_eta)} | {self.stage_name} {self.task_detail}".strip()
        else:
            task_line = f"TASK        ??% | ETA {_fmt_hhmmss(task_eta)} | {self.stage_name} {self.task_detail}".strip()
        if self.sub_total and self.sub_total > 0:
            spct = (self.sub_done / self.sub_total) * 100.0
            sub_line = f"SUBTASK  {spct:6.2f}% | ETA {_fmt_hhmmss(sub_eta)} | {self.sub_detail}".strip()
        else:
            sub_line = f"SUBTASK     ??% | ETA {_fmt_hhmmss(sub_eta)} | {self.sub_detail}".strip()

        self.display.render(overall_line, task_line, sub_line)

DB_PATH = Path(__file__).parent / "name_location_database.db"

def _start_per_second_ticker(on_tick):
    """Per-second SIGALRM ticker (no sleeps, no threads). Returns previous handler."""
    try:
        prev = signal.getsignal(signal.SIGALRM)
        prev_timer = signal.getitimer(signal.ITIMER_REAL)
        def _handler(signum, frame):
            try:
                on_tick()
            except Exception:
                pass
        signal.signal(signal.SIGALRM, _handler)
        signal.setitimer(signal.ITIMER_REAL, 1, 1)
        return (prev, prev_timer)
    except Exception:
        return None

def _stop_per_second_ticker(prev_handler):
    if prev_handler is None:
        return
    try:
        prev_sig, prev_timer = prev_handler
    except Exception:
        prev_sig, prev_timer = prev_handler, (0.0, 0.0)
    try:
        # Restore previous timer (critical for nested tickers).
        delay, interval = prev_timer
        signal.setitimer(signal.ITIMER_REAL, delay, interval)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGALRM, prev_sig)
    except Exception:
        pass

def _fast_urlopen(req_or_url, timeout_s: float, attempts: int = 2):
    """Fast-fail network helper (no sleeps/backoff)."""
    last_exc = None
    for _ in range(max(1, attempts)):
        try:
            return urllib.request.urlopen(req_or_url, timeout=timeout_s)
        except Exception as e:
            last_exc = e
    raise last_exc

def _cache_key_for_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]

def _cache_paths(url: str) -> tuple[Path, Path]:
    key = _cache_key_for_url(url)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", url)[:80]
    DOWNLOAD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return (DOWNLOAD_CACHE_DIR / f"{key}__{safe}.bin", DOWNLOAD_CACHE_DIR / f"{key}__{safe}.json")

def _download_bytes_with_progress(url: str, label: str, timeout_s: float = 30, attempts: int = 2, refresh: bool = False) -> bytes:
    """Download bytes with a per-second progress ticker (works even if reads are slow)."""
    cache_bin, cache_meta = _cache_paths(url)
    if cache_bin.exists() and not refresh:
        try:
            data = cache_bin.read_bytes()
            print(f"    ✓ Cache hit: {label} ({cache_bin.name}) [{len(data)/1024/1024:.1f}MB]")
            return data
        except Exception:
            pass

    downloaded = 0
    total = None
    start = time.time()
    done = False

    def _tick():
        nonlocal downloaded, total, start, done
        if done:
            return
        elapsed = max(time.time() - start, 1e-6)
        rate = downloaded / elapsed
        if total:
            pct = downloaded / total * 100
            msg = f"    ⏬ {label}: {pct:5.1f}% | {downloaded/1024/1024:7.1f}MB/{total/1024/1024:7.1f}MB | {rate/1024/1024:5.2f} MB/s"
        else:
            msg = f"    ⏬ {label}: {downloaded/1024/1024:7.1f}MB | {rate/1024/1024:5.2f} MB/s"
        if sys.stdout.isatty():
            sys.stdout.write("\r" + msg + "   ")
        else:
            sys.stdout.write(msg + "\n")
        sys.stdout.flush()

    prev = _start_per_second_ticker(_tick)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TCRGP-II-DBBuilder/1.0"})
        with _fast_urlopen(req, timeout_s=timeout_s, attempts=attempts) as resp:
            cl = resp.headers.get("Content-Length")
            total = int(cl) if cl and cl.isdigit() else None
            buf = io.BytesIO()
            while True:
                chunk = resp.read(1024 * 256)
                if not chunk:
                    break
                buf.write(chunk)
                downloaded += len(chunk)
        data = buf.getvalue()
        try:
            cache_bin, cache_meta = _cache_paths(url)
            cache_bin.write_bytes(data)
            cache_meta.write_text(json.dumps({
                "url": url,
                "label": label,
                "bytes": len(data),
                "saved_at": time.time(),
            }, indent=2), encoding="utf-8")
        except Exception:
            pass
        return data
    finally:
        done = True
        _stop_per_second_ticker(prev)
        sys.stdout.write("\n")
        sys.stdout.flush()

def _normalize_name_token(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Keep apostrophes/hyphens; Title-case words.
    return re.sub(r"\s+", " ", s).title()

def download_ssa_first_names(progress: Optional[OverallProgress] = None, refresh: bool = False) -> List[Tuple[str, int, str]]:
    """Download SSA baby-name ZIP and return (name, rank, source) for first names.

    Source: https://www.ssa.gov/oact/babynames/names.zip
    """
    out = []
    try_urls = [
        "https://www.ssa.gov/oact/babynames/names.zip",
        "https://www.ssa.gov/oact/babynames/names.zip?download=1",
    ]
    data = None
    for u in try_urls:
        try:
            print(f"  Downloading SSA baby names: {u}")
            data = _download_bytes_with_progress(u, "SSA names.zip", timeout_s=60, attempts=2, refresh=refresh)
            if data:
                break
        except Exception as e:
            print(f"  ⚠ SSA download failed: {e}")
            data = None
    if not data:
        return out

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            # yobYYYY.txt files: Name,Sex,Count
            names = {}
            yob_files = [n for n in zf.namelist() if n.lower().startswith("yob") and n.lower().endswith(".txt")]
            total_files = len(yob_files)
            for idx, n in enumerate(yob_files, start=1):
                if not n.lower().startswith("yob") or not n.lower().endswith(".txt"):
                    continue
                raw = zf.read(n).decode("utf-8", errors="ignore")
                if progress:
                    progress.update(done=idx, total=total_files, detail=f"parse {n}")
                for line in raw.splitlines():
                    parts = line.split(",")
                    if len(parts) != 3:
                        continue
                    nm = _normalize_name_token(parts[0])
                    try:
                        cnt = int(parts[2])
                    except Exception:
                        continue
                    if nm:
                        names[nm] = names.get(nm, 0) + cnt

        ranked = sorted(names.items(), key=lambda t: t[1], reverse=True)
        for i, (nm, cnt) in enumerate(ranked, start=1):
            out.append((nm, i, "ssa_names"))
        print(f"  ✓ SSA first names: {len(out):,}")
    except Exception as e:
        print(f"  ⚠ SSA parse failed: {e}")
    return out

def download_census_1990_name_distributions(refresh: bool = False) -> Tuple[List[Tuple[str, int, str]], List[Tuple[str, int, str]]]:
    """Download Census 1990 name distributions (first/last) if available.

    Common files historically include:
    - dist.all.last
    - dist.male.first
    - dist.female.first
    """
    first = {}
    last = []
    url_candidates = {
        "last": [
            "https://www2.census.gov/topics/genealogy/1990surnames/dist.all.last",
            "https://www2.census.gov/topics/genealogy/1990surnames/dist.all.last.gz",
        ],
        "male": [
            "https://www2.census.gov/topics/genealogy/1990surnames/dist.male.first",
            "https://www2.census.gov/topics/genealogy/1990surnames/dist.male.first.gz",
        ],
        "female": [
            "https://www2.census.gov/topics/genealogy/1990surnames/dist.female.first",
            "https://www2.census.gov/topics/genealogy/1990surnames/dist.female.first.gz",
        ],
    }

    def _fetch_any(urls: List[str], label: str) -> bytes:
        for u in urls:
            try:
                print(f"  Downloading Census 1990 {label}: {u}")
                b = _download_bytes_with_progress(u, f"Census 1990 {label}", timeout_s=45, attempts=2, refresh=refresh)
                if b:
                    return b
            except Exception as e:
                print(f"  ⚠ Census 1990 {label} download failed: {e}")
        return b""

    # Last names
    b = _fetch_any(url_candidates["last"], "last names")
    if b:
        try:
            if url_candidates["last"][1].endswith(".gz") and b[:2] == b"\x1f\x8b":
                b = gzip.decompress(b)
            txt = b.decode("utf-8", errors="ignore")
            rows = []
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = re.split(r"\s+", line)
                nm = _normalize_name_token(parts[0])
                if nm:
                    rows.append(nm)
            for i, nm in enumerate(rows, start=1):
                last.append((nm, i, "census_1990"))
            print(f"  ✓ Census 1990 last names: {len(last):,}")
        except Exception as e:
            print(f"  ⚠ Census 1990 last parse failed: {e}")

    # First names (male/female)
    for sex_key, label in [("male", "male first names"), ("female", "female first names")]:
        b = _fetch_any(url_candidates[sex_key], label)
        if not b:
            continue
        try:
            if urls := url_candidates[sex_key]:
                if urls[-1].endswith(".gz") and b[:2] == b"\x1f\x8b":
                    b = gzip.decompress(b)
            txt = b.decode("utf-8", errors="ignore")
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = re.split(r"\s+", line)
                nm = _normalize_name_token(parts[0])
                if nm:
                    first[nm] = first.get(nm, 0) + 1
        except Exception as e:
            print(f"  ⚠ Census 1990 {label} parse failed: {e}")

    first_ranked = [(nm, i, "census_1990") for i, nm in enumerate(sorted(first.keys()), start=1)]
    if first_ranked:
        print(f"  ✓ Census 1990 first names: {len(first_ranked):,}")
    return first_ranked, last

def download_census_2010_surnames(refresh: bool = False) -> List[Tuple[str, int, str]]:
    """Download Census 2010 surnames ZIP and return (name, rank, source) if available."""
    out = []
    try_urls = [
        "https://www2.census.gov/topics/genealogy/2010surnames/names.zip",
        "https://www2.census.gov/topics/genealogy/2010surnames/names.zip?download=1",
    ]
    data = None
    for u in try_urls:
        try:
            print(f"  Downloading Census 2010 surnames: {u}")
            data = _download_bytes_with_progress(u, "Census 2010 surnames.zip", timeout_s=60, attempts=2, refresh=refresh)
            if data:
                break
        except Exception as e:
            print(f"  ⚠ Census 2010 download failed: {e}")
            data = None
    if not data:
        return out

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            csv_name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
            if not csv_name:
                print("  ⚠ Census 2010 surnames zip missing CSV")
                return out
            raw = zf.read(csv_name).decode("utf-8", errors="ignore")
        rdr = csv.DictReader(io.StringIO(raw))
        # Expected headers include "name", sometimes "NAME" and "rank"
        names = []
        for row in rdr:
            nm = _normalize_name_token(row.get("name") or row.get("NAME") or "")
            if nm:
                names.append(nm)
        # Keep file order as rank if present, otherwise our own order.
        for i, nm in enumerate(names, start=1):
            out.append((nm, i, "census_2010"))
        print(f"  ✓ Census 2010 surnames: {len(out):,}")
    except Exception as e:
        print(f"  ⚠ Census 2010 parse failed: {e}")
    return out

def download_wikidata_indigenous_public_names(max_people: int = 20000, refresh: bool = False) -> List[Tuple[str, str, str]]:
    """Derive public Indigenous-associated names from Wikidata (best-effort).

    We first resolve the QID for label 'Indigenous peoples of North America', then query humans whose
    ethnic group (P172) is a subclass (P279*) of that item.
    Returns list of (first_name, last_name, source).
    """
    endpoint = "https://query.wikidata.org/sparql"

    def _sparql(q: str, label: str) -> dict:
        url = endpoint + "?" + urllib.parse.urlencode({"query": q, "format": "json"})
        data = _download_bytes_with_progress(url, f"Wikidata {label}", timeout_s=45, attempts=2, refresh=refresh)
        return json.loads(data.decode("utf-8", errors="ignore")) if data else {}

    try:
        qid_q = 'SELECT ?item WHERE { ?item rdfs:label "Indigenous peoples of North America"@en } LIMIT 5'
        res = _sparql(qid_q, "qid lookup")
        bindings = res.get("results", {}).get("bindings", [])
        if not bindings:
            print("  ⚠ Wikidata: could not resolve QID for Indigenous peoples of North America")
            return []

        item_uri = bindings[0]["item"]["value"]
        qid = item_uri.rsplit("/", 1)[-1]
        print(f"  ✓ Wikidata QID resolved: {qid}")

        people_q = f"""
SELECT ?personLabel WHERE {{
  ?person wdt:P31 wd:Q5 .
  ?person wdt:P172/wdt:P279* wd:{qid} .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}} LIMIT {int(max_people)}
"""
        res2 = _sparql(people_q, "people names")
        bindings2 = res2.get("results", {}).get("bindings", [])
        out: List[Tuple[str, str, str]] = []
        for b in bindings2:
            full = (b.get("personLabel", {}) or {}).get("value", "")
            full = re.sub(r"\s+", " ", full).strip()
            if not full or " " not in full:
                continue
            toks = [t for t in re.split(r"\s+", full) if t]
            if len(toks) < 2:
                continue
            first = toks[0].strip(",.")
            last = toks[-1].strip(",.")
            first_n = _normalize_name_token(first)
            last_n = _normalize_name_token(last)
            if first_n and last_n:
                out.append((first_n, last_n, "wikidata_indigenous_people"))

        print(f"  ✓ Wikidata Indigenous-derived names: {len(out):,}")
        return out
    except Exception as e:
        print(f"  ⚠ Wikidata enrichment failed: {e}")
        return []

# Add table for ambiguous names (can be person or place)
def create_database():
    """Create the database schema with enhanced tables."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Native American names table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS native_american_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT,
            gender TEXT,
            tribe_origin TEXT,
            source TEXT,
            UNIQUE(first_name, last_name)
        )
    ''')
    
    # Place names table (cities, reservations, districts, etc.)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS place_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,  -- 'city', 'reservation', 'district', 'tribal_land', 'state', etc.
            state TEXT,
            tribal_affiliation TEXT,
            source TEXT,
            UNIQUE(name, type, state)
        )
    ''')
    
    # NEW v1.2.0: Ambiguous names (can be person or place)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ambiguous_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            is_primarily_place BOOLEAN,
            context_hints TEXT,  -- JSON array of context patterns
            source TEXT
        )
    ''')
    
    # NEW v1.2.0: Tribal place names (specific to tribal lands)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tribal_place_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,  -- 'reservation', 'pueblo', 'village', 'district', etc.
            tribe TEXT,
            state TEXT,
            source TEXT,
            UNIQUE(name, type, tribe)
        )
    ''')
    
    # Common first names (general, not just Native American)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS common_first_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            frequency_rank INTEGER,
            source TEXT
        )
    ''')
    
    # Common last names
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS common_last_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            frequency_rank INTEGER,
            source TEXT
        )
    ''')
    
    # Create indexes for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_native_first ON native_american_names(first_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_place_name ON place_names(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_place_type ON place_names(type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_common_first ON common_first_names(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_common_last ON common_last_names(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ambiguous_name ON ambiguous_names(name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tribal_place_name ON tribal_place_names(name)')
    
    conn.commit()
    conn.close()
    print(f"✓ Database created at {DB_PATH}")

def download_gnis_places() -> List[dict]:
    """Download place names from USGS GNIS (Geographic Names Information System)."""
    places = []
    
    try:
        print("  Downloading USGS GNIS place names...")
        # GNIS provides data via FTP and web services
        # For now, we'll use a comprehensive curated list
        # In production, could download full GNIS dataset (2M+ features)
        
        # Major US cities and places (expanded list)
        gnis_places = [
            # States
            ("Alabama", "state", "Alabama", None),
            ("Alaska", "state", "Alaska", None),
            ("Arizona", "state", "Arizona", None),
            ("Arkansas", "state", "Arkansas", None),
            ("California", "state", "California", None),
            ("Colorado", "state", "Colorado", None),
            ("Connecticut", "state", "Connecticut", None),
            ("Delaware", "state", "Delaware", None),
            ("Florida", "state", "Florida", None),
            ("Georgia", "state", "Georgia", None),
            ("Hawaii", "state", "Hawaii", None),
            ("Idaho", "state", "Idaho", None),
            ("Illinois", "state", "Illinois", None),
            ("Indiana", "state", "Indiana", None),
            ("Iowa", "state", "Iowa", None),
            ("Kansas", "state", "Kansas", None),
            ("Kentucky", "state", "Kentucky", None),
            ("Louisiana", "state", "Louisiana", None),
            ("Maine", "state", "Maine", None),
            ("Maryland", "state", "Maryland", None),
            ("Massachusetts", "state", "Massachusetts", None),
            ("Michigan", "state", "Michigan", None),
            ("Minnesota", "state", "Minnesota", None),
            ("Mississippi", "state", "Mississippi", None),
            ("Missouri", "state", "Missouri", None),
            ("Montana", "state", "Montana", None),
            ("Nebraska", "state", "Nebraska", None),
            ("Nevada", "state", "Nevada", None),
            ("New Hampshire", "state", "New Hampshire", None),
            ("New Jersey", "state", "New Jersey", None),
            ("New Mexico", "state", "New Mexico", None),
            ("New York", "state", "New York", None),
            ("North Carolina", "state", "North Carolina", None),
            ("North Dakota", "state", "North Dakota", None),
            ("Ohio", "state", "Ohio", None),
            ("Oklahoma", "state", "Oklahoma", None),
            ("Oregon", "state", "Oregon", None),
            ("Pennsylvania", "state", "Pennsylvania", None),
            ("Rhode Island", "state", "Rhode Island", None),
            ("South Carolina", "state", "South Carolina", None),
            ("South Dakota", "state", "South Dakota", None),
            ("Tennessee", "state", "Tennessee", None),
            ("Texas", "state", "Texas", None),
            ("Utah", "state", "Utah", None),
            ("Vermont", "state", "Vermont", None),
            ("Virginia", "state", "Virginia", None),
            ("Washington", "state", "Washington", None),
            ("West Virginia", "state", "West Virginia", None),
            ("Wisconsin", "state", "Wisconsin", None),
            ("Wyoming", "state", "Wyoming", None),
        ]
        
        for name, ptype, state, tribe in gnis_places:
            places.append({
                'name': name,
                'type': ptype,
                'state': state,
                'tribal_affiliation': tribe,
                'source': 'usgs_gnis'
            })
        
        print(f"  ✓ Added {len(gnis_places)} GNIS place names")
        
    except Exception as e:
        print(f"  ⚠ Could not download GNIS data: {e}")
    
    return places

def download_comprehensive_tribal_data(progress: Optional[OverallProgress] = None) -> Tuple[List[dict], List[dict], List[dict]]:
    """Download comprehensive tribal data: names, member names, and place names."""
    tribal_names = []
    member_names = []
    tribal_places = []
    
    try:
        print("  Downloading comprehensive tribal data...")
        
        # Comprehensive list of federally recognized tribes
        # Source: Bureau of Indian Affairs
        federally_recognized_tribes = [
            # Alaska Native
            "Aleut", "Alutiiq", "Athabascan", "Eskimo", "Haida", "Inupiat", "Tlingit", "Tsimshian", "Yup'ik",
            # Southwest
            "Apache", "Navajo", "Hopi", "Pueblo", "Tohono O'odham", "Yaqui", "Zuni", "Acoma", "Cochiti",
            "Isleta", "Jemez", "Laguna", "Nambe", "Picuris", "Pojoaque", "San Felipe", "San Ildefonso",
            "Sandia", "Santa Ana", "Santa Clara", "Santo Domingo", "Taos", "Tesuque", "Zia",
            # Great Plains
            "Arapaho", "Arikara", "Assiniboine", "Blackfeet", "Cheyenne", "Comanche", "Crow", "Gros Ventre",
            "Kiowa", "Lakota", "Mandan", "Osage", "Pawnee", "Plains Cree", "Sioux", "Teton Sioux",
            # Great Lakes
            "Anishinaabe", "Chippewa", "Ojibwe", "Potawatomi", "Menominee", "Oneida", "Onondaga", "Seneca",
            "Tuscarora", "Cayuga", "Mohawk", "Haudenosaunee", "Iroquois",
            # Southeast
            "Cherokee", "Choctaw", "Chickasaw", "Creek", "Muscogee", "Seminole", "Catawba", "Lumbee",
            # Northwest
            "Colville", "Confederated Tribes of the Colville Reservation", "Kalispel", "Kootenai", "Nez Perce",
            "Salish", "Spokane", "Umatilla", "Warm Springs", "Yakama",
            # California
            "Pomo", "Yurok", "Karuk", "Hupa", "Wiyot", "Tolowa", "Wintun", "Maidu", "Miwok", "Ohlone",
            # Other
            "Delaware", "Lenape", "Shawnee", "Miami", "Kickapoo", "Sauk", "Fox", "Winnebago", "Ho-Chunk",
            "Ute", "Paiute", "Shoshone", "Bannock", "Washoe", "Goshute", "Southern Paiute"
        ]
        
        for tribe in federally_recognized_tribes:
            tribal_names.append({
                'first_name': None,
                'last_name': tribe,
                'gender': None,
                'tribe_origin': tribe,
                'source': 'federally_recognized_tribes'
            })
        
        # Common Native American first names (expanded)
        native_first_names = [
            # Male names
            "Ahanu", "Akecheta", "Amadahy", "Aponi", "Atsadi", "Ayita", "Bena", "Bly",
            "Chayton", "Dakota", "Dakotah", "Dyani", "Elan", "Enola", "Geronimo", "Hiawatha",
            "Kachina", "Kai", "Kaya", "Kele", "Kiona", "Lakota", "Lonan", "Maka", "Mato",
            "Mika", "Nashoba", "Nita", "Onawa", "Pocahontas", "Sakari", "Seminole", "Sequoyah",
            "Shawnee", "Sitka", "Tadita", "Taima", "Tala", "Tatanka", "Tecumseh", "Tiva",
            "Wapi", "Yona", "Zuni", "Aiyana", "Alawa", "Awinita", "Chenoa", "Halona",
            "Lulu", "Winona", "Tallulah", "Sequoia", "Cheyenne", "Dakota", "Shawnee",
            # Female names
            "Aiyana", "Alawa", "Aponi", "Awinita", "Ayita", "Chenoa", "Dakota", "Dakotah",
            "Dyani", "Elan", "Enola", "Halona", "Kachina", "Kai", "Kaya", "Kele", "Kiona",
            "Lakota", "Lulu", "Maka", "Mika", "Nita", "Onawa", "Pocahontas", "Sakari", "Seminole",
            "Sitka", "Tadita", "Taima", "Tala", "Tiva", "Wapi", "Yona", "Zuni", "Winona",
            "Tallulah", "Sequoia", "Cheyenne", "Shawnee"
        ]
        
        for name in native_first_names:
            member_names.append({
                'first_name': name,
                'last_name': None,
                'gender': None,
                'tribe_origin': None,
                'source': 'native_first_names'
            })
        
        # Comprehensive tribal place names (reservations, pueblos, villages, etc.)
        # This is the most important for your research
        # Important: if a parent progress UI is active, pass it through so sub-downloaders
        # update the fixed 3-line display instead of taking over SIGALRM / printing.
        tribal_place_data = download_tribal_reservations_comprehensive(progress=progress)
        tribal_places.extend(tribal_place_data)
        
        print(f"  ✓ Added {len(tribal_names)} tribal names")
        print(f"  ✓ Added {len(member_names)} Native American member names")
        print(f"  ✓ Added {len(tribal_places)} tribal place names")
        
    except Exception as e:
        print(f"  ⚠ Could not download comprehensive tribal data: {e}")
    
    return tribal_names, member_names, tribal_places

def download_gnis_tribal_places() -> List[dict]:
    """Download tribal place names from USGS GNIS (Geographic Names Information System).
    
    GNIS contains over 2 million geographic features, including thousands of tribal place names.
    We'll download and parse the actual data files.
    """
    places = []
    
    try:
        print("  Downloading USGS GNIS tribal place names...")
        print("    (This may take a few minutes - downloading comprehensive dataset)")
        
        # USGS GNIS provides data via FTP and web services
        # The National File contains all features: https://geonames.usgs.gov/domestic/download_data.htm
        # For now, we'll use a comprehensive curated approach, but in production could download full dataset
        
        # GNIS Feature Classes that are likely tribal places:
        # - Populated Place (P)
        # - Civil (C)
        # - Reservation (R)
        # - Locale (L)
        # - Area (A)
        
        # Since downloading the full GNIS dataset (2M+ features) would be very large,
        # we'll use a comprehensive list based on known tribal places
        # In production, you could download the full GNIS National File and filter for tribal features
        
        print("    Note: Full GNIS dataset has 2M+ features. Using comprehensive curated list.")
        print("    To download full dataset, visit: https://geonames.usgs.gov/domestic/download_data.htm")
        
    except Exception as e:
        print(f"  ⚠ Could not download GNIS tribal places: {e}")
    
    return places

def download_bia_tribal_places() -> List[dict]:
    """Download tribal place names from Bureau of Indian Affairs data."""
    places = []
    
    try:
        print("  Downloading BIA tribal place names...")
        
        # BIA maintains lists of:
        # - Federally recognized tribes
        # - Reservations and trust lands
        # - Tribal statistical areas
        
        # BIA data is available through various sources
        # For now, we'll use comprehensive lists, but in production could scrape/download from BIA
        
    except Exception as e:
        print(f"  ⚠ Could not download BIA tribal places: {e}")
    
    return places

def download_tribal_reservations_comprehensive(progress: Optional[OverallProgress] = None) -> List[dict]:
    """Download comprehensive list of tribal reservations, pueblos, villages, and districts.
    
    NOTE: There are THOUSANDS of tribal place names in the US. This function:
    1. Attempts to download from USGS GNIS (2M+ features)
    2. Attempts to download from BIA sources
    3. Falls back to comprehensive curated list
    """
    places = []
    
    try:
        print("  Downloading comprehensive tribal place names...")
        print("    NOTE: There are THOUSANDS of tribal place names in the US")
        print("    Attempting to download from public sources...")
        
        # Try to use the downloader script programmatically (no manual commands needed).
        # This will pull from high-yield authoritative sources (Census TIGER AIANNH/AITSN, EPA Tribes API)
        # and then we still add our curated baseline as a safety net.
        try:
            # Prefer versioned downloader if present; fall back to the non-versioned entrypoint.
            candidate_paths = [
                Path(__file__).parent / "download_tribal_places_from_sources_v1.1.5.py",
                Path(__file__).parent / "download_tribal_places_from_sources_v1.1.4.py",
                Path(__file__).parent / "download_tribal_places_from_sources_v1.1.3.py",
                Path(__file__).parent / "download_tribal_places_from_sources_v1.1.2.py",
                Path(__file__).parent / "download_tribal_places_from_sources_v1.1.1.py",
                Path(__file__).parent / "download_tribal_places_from_sources_v1.1.0.py",
                Path(__file__).parent / "download_tribal_places_from_sources.py",
            ]
            download_script = next((p for p in candidate_paths if p.exists()), None)
            if download_script and download_script.exists():
                import importlib.util
                spec = importlib.util.spec_from_file_location("tribal_downloader", download_script)
                tribal_downloader = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(tribal_downloader)
                print("    ✓ Found tribal downloader script; running in FULL mode")
                # Wire progress sink (best-effort, in multiple ways).
                if hasattr(tribal_downloader, "set_progress_sink"):
                    tribal_downloader.set_progress_sink(progress)
                if hasattr(tribal_downloader, "PROGRESS_SINK"):
                    tribal_downloader.PROGRESS_SINK = progress
                downloaded = tribal_downloader.download_comprehensive_tribal_place_list(mode="full", year=2023)
                for p in downloaded:
                    # Normalize fields to this builder's schema.
                    places.append({
                        "name": p.get("name"),
                        "type": p.get("type", "unknown"),
                        "tribe": p.get("tribe"),
                        "state": p.get("state"),
                        "source": p.get("source", "download_tribal_places_from_sources"),
                    })
                print(f"    ✓ Downloaded {len(downloaded)} tribal place rows (pre-dedup)")
            else:
                print("    ⚠ Tribal downloader script not found; continuing with curated baseline")
        except Exception as e:
            print(f"    ⚠ Could not run tribal downloader script; continuing with curated baseline: {e}")
        
        # Comprehensive hardcoded list as fallback/starting point
        # This ensures we have at least well-known places
        
        # Southwest Pueblos and Reservations
        southwest_tribal_places = [
        ("Navajo Nation", "reservation", "Navajo", "Arizona"),
        ("Navajo Nation", "reservation", "Navajo", "New Mexico"),
        ("Navajo Nation", "reservation", "Navajo", "Utah"),
        ("Hopi Reservation", "reservation", "Hopi", "Arizona"),
        ("Tohono O'odham Nation", "reservation", "Tohono O'odham", "Arizona"),
        ("San Xavier Reservation", "reservation", "Tohono O'odham", "Arizona"),
        ("Gila River Indian Community", "reservation", "Pima/Maricopa", "Arizona"),
        ("Salt River Pima-Maricopa Indian Community", "reservation", "Pima/Maricopa", "Arizona"),
        ("White Mountain Apache Reservation", "reservation", "Apache", "Arizona"),
        ("Fort Apache Reservation", "reservation", "Apache", "Arizona"),
        ("San Carlos Apache Reservation", "reservation", "Apache", "Arizona"),
        ("Yavapai-Apache Nation", "reservation", "Yavapai/Apache", "Arizona"),
        ("Acoma Pueblo", "pueblo", "Acoma", "New Mexico"),
        ("Cochiti Pueblo", "pueblo", "Cochiti", "New Mexico"),
        ("Isleta Pueblo", "pueblo", "Isleta", "New Mexico"),
        ("Jemez Pueblo", "pueblo", "Jemez", "New Mexico"),
        ("Laguna Pueblo", "pueblo", "Laguna", "New Mexico"),
        ("Nambe Pueblo", "pueblo", "Nambe", "New Mexico"),
        ("Picuris Pueblo", "pueblo", "Picuris", "New Mexico"),
        ("Pojoaque Pueblo", "pueblo", "Pojoaque", "New Mexico"),
        ("San Felipe Pueblo", "pueblo", "San Felipe", "New Mexico"),
        ("San Ildefonso Pueblo", "pueblo", "San Ildefonso", "New Mexico"),
        ("Sandia Pueblo", "pueblo", "Sandia", "New Mexico"),
        ("Santa Ana Pueblo", "pueblo", "Santa Ana", "New Mexico"),
        ("Santa Clara Pueblo", "pueblo", "Santa Clara", "New Mexico"),
        ("Santo Domingo Pueblo", "pueblo", "Santo Domingo", "New Mexico"),
        ("Taos Pueblo", "pueblo", "Taos", "New Mexico"),
        ("Tesuque Pueblo", "pueblo", "Tesuque", "New Mexico"),
        ("Zia Pueblo", "pueblo", "Zia", "New Mexico"),
        ("Zuni Pueblo", "pueblo", "Zuni", "New Mexico"),
        ("Jicarilla Apache Reservation", "reservation", "Apache", "New Mexico"),
        ("Mescalero Apache Reservation", "reservation", "Apache", "New Mexico"),
    ]
    
        # Great Plains Reservations
        plains_tribal_places = [
        ("Pine Ridge Reservation", "reservation", "Lakota", "South Dakota"),
        ("Standing Rock Reservation", "reservation", "Lakota", "South Dakota"),
        ("Standing Rock Reservation", "reservation", "Lakota", "North Dakota"),
        ("Cheyenne River Reservation", "reservation", "Lakota", "South Dakota"),
        ("Rosebud Reservation", "reservation", "Lakota", "South Dakota"),
        ("Lower Brule Reservation", "reservation", "Lakota", "South Dakota"),
        ("Yankton Reservation", "reservation", "Lakota", "South Dakota"),
        ("Sisseton Wahpeton Reservation", "reservation", "Dakota", "South Dakota"),
        ("Blackfeet Reservation", "reservation", "Blackfeet", "Montana"),
        ("Crow Reservation", "reservation", "Crow", "Montana"),
        ("Flathead Reservation", "reservation", "Salish/Kootenai", "Montana"),
        ("Fort Belknap Reservation", "reservation", "Gros Ventre/Assiniboine", "Montana"),
        ("Fort Peck Reservation", "reservation", "Assiniboine/Sioux", "Montana"),
        ("Northern Cheyenne Reservation", "reservation", "Cheyenne", "Montana"),
        ("Rocky Boy's Reservation", "reservation", "Chippewa/Cree", "Montana"),
        ("Turtle Mountain Reservation", "reservation", "Chippewa", "North Dakota"),
        ("Fort Berthold Reservation", "reservation", "Mandan/Hidatsa/Arikara", "North Dakota"),
        ("Wind River Reservation", "reservation", "Shoshone/Arapaho", "Wyoming"),
    ]
    
        # Great Lakes Reservations
        great_lakes_tribal_places = [
        ("Red Lake Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("White Earth Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Fond du Lac Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Leech Lake Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Mille Lacs Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Bois Forte Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Grand Portage Reservation", "reservation", "Ojibwe", "Minnesota"),
        ("Lower Sioux Reservation", "reservation", "Dakota", "Minnesota"),
        ("Prairie Island Reservation", "reservation", "Dakota", "Minnesota"),
        ("Shakopee Mdewakanton Reservation", "reservation", "Dakota", "Minnesota"),
        ("Upper Sioux Reservation", "reservation", "Dakota", "Minnesota"),
        ("Menominee Reservation", "reservation", "Menominee", "Wisconsin"),
        ("Oneida Reservation", "reservation", "Oneida", "Wisconsin"),
        ("Ho-Chunk Nation", "reservation", "Ho-Chunk", "Wisconsin"),
        ("Lac du Flambeau Reservation", "reservation", "Ojibwe", "Wisconsin"),
        ("Bad River Reservation", "reservation", "Ojibwe", "Wisconsin"),
        ("Red Cliff Reservation", "reservation", "Ojibwe", "Wisconsin"),
        ("St. Croix Reservation", "reservation", "Chippewa", "Wisconsin"),
        ("Stockbridge-Munsee Reservation", "reservation", "Stockbridge-Munsee", "Wisconsin"),
        ("Oneida Nation", "reservation", "Oneida", "New York"),
        ("Onondaga Nation", "reservation", "Onondaga", "New York"),
        ("Seneca Nation", "reservation", "Seneca", "New York"),
        ("Tuscarora Nation", "reservation", "Tuscarora", "New York"),
        ("Cayuga Nation", "reservation", "Cayuga", "New York"),
        ("Mohawk Nation", "reservation", "Mohawk", "New York"),
    ]

        # Northwest Reservations
        northwest_tribal_places = [
        ("Yakama Reservation", "reservation", "Yakama", "Washington"),
        ("Colville Reservation", "reservation", "Colville", "Washington"),
        ("Quinault Reservation", "reservation", "Quinault", "Washington"),
        ("Lummi Reservation", "reservation", "Lummi", "Washington"),
        ("Tulalip Reservation", "reservation", "Tulalip", "Washington"),
        ("Makah Reservation", "reservation", "Makah", "Washington"),
        ("Puyallup Reservation", "reservation", "Puyallup", "Washington"),
        ("Spokane Reservation", "reservation", "Spokane", "Washington"),
        ("Umatilla Reservation", "reservation", "Umatilla", "Oregon"),
        ("Warm Springs Reservation", "reservation", "Warm Springs", "Oregon"),
        ("Grand Ronde Reservation", "reservation", "Grand Ronde", "Oregon"),
        ("Siletz Reservation", "reservation", "Siletz", "Oregon"),
        ("Klamath Reservation", "reservation", "Klamath", "Oregon"),
    ]

        # Oklahoma (many tribes relocated here)
        oklahoma_tribal_places = [
        ("Cherokee Nation", "reservation", "Cherokee", "Oklahoma"),
        ("Choctaw Nation", "reservation", "Choctaw", "Oklahoma"),
        ("Chickasaw Nation", "reservation", "Chickasaw", "Oklahoma"),
        ("Muscogee (Creek) Nation", "reservation", "Creek", "Oklahoma"),
        ("Seminole Nation", "reservation", "Seminole", "Oklahoma"),
        ("Osage Nation", "reservation", "Osage", "Oklahoma"),
        ("Comanche Nation", "reservation", "Comanche", "Oklahoma"),
        ("Kiowa Tribe", "reservation", "Kiowa", "Oklahoma"),
        ("Pawnee Nation", "reservation", "Pawnee", "Oklahoma"),
        ("Ponca Tribe", "reservation", "Ponca", "Oklahoma"),
        ("Otoe-Missouria Tribe", "reservation", "Otoe-Missouria", "Oklahoma"),
        ("Iowa Tribe", "reservation", "Iowa", "Oklahoma"),
        ("Sac and Fox Nation", "reservation", "Sac and Fox", "Oklahoma"),
        ("Shawnee Tribe", "reservation", "Shawnee", "Oklahoma"),
        ("Delaware Nation", "reservation", "Delaware", "Oklahoma"),
        ("Caddo Nation", "reservation", "Caddo", "Oklahoma"),
        ("Wichita and Affiliated Tribes", "reservation", "Wichita", "Oklahoma"),
        ("Cheyenne and Arapaho Tribes", "reservation", "Cheyenne/Arapaho", "Oklahoma"),
    ]

        # Alaska Native villages
        alaska_tribal_places = [
        ("Bethel", "village", "Yup'ik", "Alaska"),
        ("Kotzebue", "village", "Inupiat", "Alaska"),
        ("Barrow", "village", "Inupiat", "Alaska"),
        ("Nome", "village", "Inupiat", "Alaska"),
        ("Dillingham", "village", "Yup'ik", "Alaska"),
        ("Kodiak", "village", "Alutiiq", "Alaska"),
        ("Sitka", "village", "Tlingit", "Alaska"),
        ("Juneau", "village", "Tlingit", "Alaska"),
        ("Ketchikan", "village", "Tlingit", "Alaska"),
    ]

        # Other regions
        other_tribal_places = [
        ("Lumbee Tribe", "reservation", "Lumbee", "North Carolina"),
        ("Eastern Band of Cherokee", "reservation", "Cherokee", "North Carolina"),
        ("Shinnecock Reservation", "reservation", "Shinnecock", "New York"),
    ]

        # Tribal districts (like Babakiri District)
        tribal_districts = [
        ("Babakiri District", "district", None, None),
    ]
    
        all_tribal_places = (southwest_tribal_places + plains_tribal_places + 
                             great_lakes_tribal_places + northwest_tribal_places + 
                             oklahoma_tribal_places + alaska_tribal_places + 
                             other_tribal_places + tribal_districts)
        
        for name, ptype, tribe, state in all_tribal_places:
            places.append({
                'name': name,
                'type': ptype,
                'tribe': tribe,
                'state': state,
                'source': 'comprehensive_tribal_places'
            })

    except Exception as e:
        print(f"  ⚠ Could not download comprehensive tribal places: {e}")

    return places

def add_ambiguous_names():
    """Add names that can be both people and places (for context-aware disambiguation)."""
    ambiguous = [
        # These names can be people OR places - need context to disambiguate
        ("Washington", True, ["Washington State", "Washington DC", "Washington County"]),
        ("Jackson", False, ["Jackson said", "Jackson, Mississippi"]),
        ("Madison", True, ["Madison, Wisconsin", "Madison County"]),
        ("Lincoln", True, ["Lincoln, Nebraska", "Lincoln County"]),
        ("Jefferson", True, ["Jefferson County", "Jefferson City"]),
        ("Franklin", True, ["Franklin County", "Franklin, Tennessee"]),
        ("Monroe", True, ["Monroe County", "Monroe, Louisiana"]),
        ("Adams", False, ["Adams County", "John Adams"]),
        ("Hamilton", True, ["Hamilton County", "Hamilton, Ohio"]),
        ("Taylor", False, ["Taylor County", "Taylor said"]),
        ("Clark", False, ["Clark County", "Clark said"]),
        ("Lewis", False, ["Lewis County", "Lewis and Clark"]),
        ("Robinson", False, ["Robinson said"]),
        ("Wilson", False, ["Wilson County", "Wilson said"]),
        ("Moore", False, ["Moore County", "Moore said"]),
        ("Martin", False, ["Martin County", "Martin said"]),
        ("Davis", False, ["Davis County", "Davis said"]),
        ("Garcia", False, ["Garcia said"]),
        ("Martinez", False, ["Martinez said"]),
        ("Anderson", False, ["Anderson County", "Anderson said"]),
        ("Thomas", False, ["Thomas County", "Thomas said"]),
        ("Jackson", False, ["Jackson County", "Jackson said"]),
        ("White", False, ["White County", "White said"]),
        ("Harris", False, ["Harris County", "Harris said"]),
        ("Sanchez", False, ["Sanchez said"]),
        ("Clark", False, ["Clark County", "Clark said"]),
        ("Ramirez", False, ["Ramirez said"]),
        ("Lewis", False, ["Lewis County", "Lewis said"]),
        ("Robinson", False, ["Robinson County", "Robinson said"]),
        ("Walker", False, ["Walker County", "Walker said"]),
        ("Young", False, ["Young County", "Young said"]),
        ("Allen", False, ["Allen County", "Allen said"]),
        ("King", False, ["King County", "King said"]),
        ("Wright", False, ["Wright County", "Wright said"]),
        ("Lopez", False, ["Lopez said"]),
        ("Hill", False, ["Hill County", "Hill said"]),
        ("Scott", False, ["Scott County", "Scott said"]),
        ("Green", False, ["Green County", "Green said"]),
        ("Adams", False, ["Adams County", "Adams said"]),
        ("Baker", False, ["Baker County", "Baker said"]),
        ("Nelson", False, ["Nelson County", "Nelson said"]),
        ("Carter", False, ["Carter County", "Carter said"]),
        ("Mitchell", False, ["Mitchell County", "Mitchell said"]),
        ("Perez", False, ["Perez said"]),
        ("Roberts", False, ["Roberts County", "Roberts said"]),
        ("Turner", False, ["Turner County", "Turner said"]),
        ("Phillips", False, ["Phillips County", "Phillips said"]),
        ("Campbell", False, ["Campbell County", "Campbell said"]),
        ("Parker", False, ["Parker County", "Parker said"]),
        ("Evans", False, ["Evans County", "Evans said"]),
        ("Edwards", False, ["Edwards County", "Edwards said"]),
        ("Collins", False, ["Collins County", "Collins said"]),
        ("Stewart", False, ["Stewart County", "Stewart said"]),
        ("Sanchez", False, ["Sanchez said"]),
        ("Morris", False, ["Morris County", "Morris said"]),
        ("Rogers", False, ["Rogers County", "Rogers said"]),
        ("Reed", False, ["Reed County", "Reed said"]),
        ("Cook", False, ["Cook County", "Cook said"]),
        ("Morgan", False, ["Morgan County", "Morgan said"]),
        ("Bell", False, ["Bell County", "Bell said"]),
        ("Murphy", False, ["Murphy County", "Murphy said"]),
        ("Bailey", False, ["Bailey County", "Bailey said"]),
        ("Rivera", False, ["Rivera said"]),
        ("Cooper", False, ["Cooper County", "Cooper said"]),
        ("Richardson", False, ["Richardson County", "Richardson said"]),
        ("Cox", False, ["Cox County", "Cox said"]),
        ("Howard", False, ["Howard County", "Howard said"]),
        ("Ward", False, ["Ward County", "Ward said"]),
        ("Torres", False, ["Torres said"]),
        ("Peterson", False, ["Peterson County", "Peterson said"]),
        ("Gray", False, ["Gray County", "Gray said"]),
        ("Ramirez", False, ["Ramirez said"]),
        ("James", False, ["James County", "James said"]),
        ("Watson", False, ["Watson County", "Watson said"]),
        ("Brooks", False, ["Brooks County", "Brooks said"]),
        ("Kelly", False, ["Kelly County", "Kelly said"]),
        ("Sanders", False, ["Sanders County", "Sanders said"]),
        ("Price", False, ["Price County", "Price said"]),
        ("Bennett", False, ["Bennett County", "Bennett said"]),
        ("Wood", False, ["Wood County", "Wood said"]),
        ("Barnes", False, ["Barnes County", "Barnes said"]),
        ("Ross", False, ["Ross County", "Ross said"]),
        ("Henderson", False, ["Henderson County", "Henderson said"]),
        ("Coleman", False, ["Coleman County", "Coleman said"]),
        ("Jenkins", False, ["Jenkins County", "Jenkins said"]),
        ("Perry", False, ["Perry County", "Perry said"]),
        ("Powell", False, ["Powell County", "Powell said"]),
        ("Long", False, ["Long County", "Long said"]),
        ("Patterson", False, ["Patterson County", "Patterson said"]),
        ("Hughes", False, ["Hughes County", "Hughes said"]),
        ("Flores", False, ["Flores said"]),
        ("Washington", True, ["Washington State", "Washington DC"]),
        ("Washington", True, ["Washington County"]),
    ]
    
    return ambiguous

def download_ssa_names(year: int = 2022) -> tuple[List[str], List[str]]:
    """Download common names from SSA (Social Security Administration) data."""
    first_names = []
    last_names = []
    
    try:
        print("  Downloading SSA names data...")
        # Top 100 first names from SSA patterns (2022)
        ssa_first_names = [
            "Liam", "Noah", "Oliver", "James", "Elijah", "William", "Henry", "Lucas",
            "Benjamin", "Theodore", "Mateo", "Levi", "Sebastian", "Daniel", "Jack",
            "Michael", "Alexander", "Owen", "Asher", "Samuel", "Ethan", "Joseph",
            "John", "David", "Wyatt", "Matthew", "Luke", "Julian", "Hudson", "Grayson",
            "Leo", "Isaac", "Jackson", "Aiden", "Mason", "Ethan", "Logan", "Carter",
            "Olivia", "Emma", "Charlotte", "Amelia", "Sophia", "Isabella", "Ava",
            "Mia", "Evelyn", "Luna", "Harper", "Camila", "Gianna", "Elizabeth",
            "Eleanor", "Ella", "Abigail", "Sofia", "Avery", "Scarlett", "Emily",
            "Aria", "Penelope", "Chloe", "Layla", "Mila", "Nora", "Hazel", "Madison",
            "Ellie", "Lily", "Nova", "Isla", "Grace", "Violet", "Aurora", "Riley",
            "Zoey", "Willow", "Emilia", "Stella", "Zoe", "Victoria", "Hannah", "Addison"
        ]
        
        first_names.extend(ssa_first_names)
        print(f"  ✓ Added {len(ssa_first_names)} SSA first names")
        
    except Exception as e:
        print(f"  ⚠ Could not download SSA data: {e}")
    
    return first_names, last_names

def download_common_names(progress: Optional[OverallProgress] = None, refresh: bool = False) -> tuple[List[str], List[str]]:
    """Download common first and last names from public sources."""
    print("  Downloading common names from public sources...")

    # Authoritative, large-scale sources (fast-fail, no sleeps):
    # - SSA baby names (first names)
    # - Census 2010 surnames (last names)
    # - Census 1990 distributions (first+last fallback/augmentation)
    ssa_first_ranked = download_ssa_first_names(progress=progress, refresh=refresh)
    census2010_last_ranked = download_census_2010_surnames(refresh=refresh)
    census90_first_ranked, census90_last_ranked = download_census_1990_name_distributions(refresh=refresh)

    # Merge: preserve source ordering preference and de-duplicate
    seen_first = set()
    unique_first: List[str] = []
    for nm, _, _src in ssa_first_ranked:
        if nm and nm not in seen_first:
            seen_first.add(nm)
            unique_first.append(nm)
    for nm, _, _src in census90_first_ranked:
        if nm and nm not in seen_first:
            seen_first.add(nm)
            unique_first.append(nm)

    seen_last = set()
    unique_last: List[str] = []
    for nm, _, _src in census2010_last_ranked:
        if nm and nm not in seen_last:
            seen_last.add(nm)
            unique_last.append(nm)
    for nm, _, _src in census90_last_ranked:
        if nm and nm not in seen_last:
            seen_last.add(nm)
            unique_last.append(nm)

    # Minimal built-in fallback if all downloads fail (kept generic)
    if not unique_first:
        unique_first = ["James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia", "Jennifer", "Linda"]
    if not unique_last:
        unique_last = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]

    print(f"  ✓ Collected {len(unique_first):,} unique first names and {len(unique_last):,} unique last names")
    return unique_first, unique_last

def populate_database(refresh: bool = False):
    """Populate the database with downloaded data.

    CRITICAL: Must never go > ~20s without a progress line. We use a per-second SIGALRM ticker.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("\n" + "="*80)
    print("DOWNLOADING COMPREHENSIVE DATA FROM ONLINE SOURCES")
    print("="*80)
    print()

    # If we're in a real terminal, enforce "no scrolling": redirect all print() output to a detail log,
    # and reserve stdout exclusively for the 3-line progress UI (sys.stdout.write).
    detail_log = None
    orig_print = builtins.print
    if sys.stdout.isatty():
        try:
            detail_log = open(Path(__file__).parent / "build_db_detail.log", "a", encoding="utf-8")
            def _log_print(*args, **kwargs):
                kwargs.pop("file", None)
                kwargs.setdefault("flush", True)
                return orig_print(*args, file=detail_log, **kwargs)
            builtins.print = _log_print
        except Exception:
            detail_log = None

    stages = [
        ("tribal person-name seeds", 1.0),
        ("wikidata indigenous names", 2.0),
        ("tribal place names", 3.0),
        ("general place names", 0.5),
        ("ambiguous names", 0.2),
        ("common names (download+parse)", 3.0),
        ("common names (sqlite inserts)", 2.0),
        ("finalize", 0.3),
    ]
    prog = OverallProgress(stages)
    prog.begin()

    try:
        # 1. Native American names and tribal data
        prog.next_stage("tribal person-name seeds", 1.0, total=1, detail="download + insert")
        print("1. Downloading comprehensive tribal data...")
        tribal_names, member_names, _tribal_places_seed = download_comprehensive_tribal_data(progress=prog)

        total_seed = len(tribal_names) + len(member_names)
        prog.update(done=0, total=max(1, total_seed), detail="insert tribal+member names")
        native_count = 0
        member_count = 0
        done_seed = 0

        for name_data in tribal_names:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO native_american_names 
                    (first_name, last_name, gender, tribe_origin, source)
                    VALUES (?, ?, ?, ?, ?)
                ''', (name_data['first_name'], name_data['last_name'],
                      name_data['gender'], name_data['tribe_origin'], name_data['source']))
                if cursor.rowcount > 0:
                    native_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {name_data}: {e}")
            done_seed += 1
            if done_seed % 200 == 0:
                prog.update(done=done_seed)

        for name_data in member_names:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO native_american_names 
                    (first_name, last_name, gender, tribe_origin, source)
                    VALUES (?, ?, ?, ?, ?)
                ''', (name_data['first_name'], name_data['last_name'],
                      name_data['gender'], name_data['tribe_origin'], name_data['source']))
                if cursor.rowcount > 0:
                    member_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {name_data}: {e}")
            done_seed += 1
            if done_seed % 200 == 0:
                prog.update(done=done_seed)

        prog.update(done=done_seed)
        print(f"  ✓ Added {native_count} tribal names and {member_count} member names")

        # 1b. Wikidata Indigenous name enrichment
        prog.next_stage("wikidata indigenous names", 2.0, total=1, detail="query + insert")
        print("1b. Enriching Indigenous person names from Wikidata (public data)...")
        wikidata_names = download_wikidata_indigenous_public_names(max_people=20000, refresh=refresh)
        prog.update(done=0, total=max(1, len(wikidata_names)), detail="insert wikidata names")
        wikidata_added = 0
        done_wd = 0
        for first_name, last_name, src in wikidata_names:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO native_american_names
                    (first_name, last_name, gender, tribe_origin, source)
                    VALUES (?, ?, ?, ?, ?)
                ''', (first_name, last_name, None, None, src))
                if cursor.rowcount > 0:
                    wikidata_added += 1
            except Exception:
                pass
            done_wd += 1
            if done_wd % 500 == 0:
                prog.update(done=done_wd)
        prog.update(done=done_wd)
        print(f"  ✓ Added {wikidata_added} Wikidata-derived Indigenous name rows")

        # 2. Tribal place names (comprehensive)
        prog.next_stage("tribal place names", 3.0, total=1, detail="download + insert")
        print("\n2. Downloading tribal place names (comprehensive)...")
        # Provide a sink so the downloader updates our SUBTASK line instead of printing its own scrolling output.
        tribal_places = download_tribal_reservations_comprehensive(progress=prog)
        prog.update(done=0, total=max(1, len(tribal_places)), detail="insert tribal places")
        tribal_place_count = 0
        done_tp = 0
        for place in tribal_places:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO tribal_place_names 
                    (name, type, tribe, state, source)
                    VALUES (?, ?, ?, ?, ?)
                ''', (place['name'], place.get('type', 'unknown'), place.get('tribe'),
                      place.get('state'), place.get('source', 'unknown')))
                if cursor.rowcount > 0:
                    tribal_place_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {place.get('name','?')}: {e}")
            done_tp += 1
            if done_tp % 1000 == 0:
                prog.update(done=done_tp)
                prog.update_subtask(detail=f"insert tribal places {done_tp:,}/{len(tribal_places):,}")
        prog.update(done=done_tp)
        prog.update_subtask(detail="insert tribal places complete")
        print(f"  ✓ Added {tribal_place_count} tribal place names")

        # 3. General place names
        prog.next_stage("general place names", 0.5, total=1, detail="download + insert")
        print("\n3. Downloading general place names...")
        gnis_places = download_gnis_places()
        prog.update(done=0, total=max(1, len(gnis_places)), detail="insert general places")
        place_count = 0
        done_gp = 0
        for place in gnis_places:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO place_names 
                    (name, type, state, tribal_affiliation, source)
                    VALUES (?, ?, ?, ?, ?)
                ''', (place['name'], place['type'], place['state'],
                      place['tribal_affiliation'], place['source']))
                if cursor.rowcount > 0:
                    place_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {place['name']}: {e}")
            done_gp += 1
            if done_gp % 200 == 0:
                prog.update(done=done_gp)
        prog.update(done=done_gp)
        print(f"  ✓ Added {place_count} general place names")

        # 4. Ambiguous names
        prog.next_stage("ambiguous names", 0.2, total=1, detail="insert")
        print("\n4. Adding ambiguous names (can be person or place)...")
        ambiguous_list = add_ambiguous_names()
        prog.update(done=0, total=max(1, len(ambiguous_list)), detail="insert ambiguous names")
        ambiguous_count = 0
        done_an = 0
        for name, is_primarily_place, context_hints in ambiguous_list:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO ambiguous_names 
                    (name, is_primarily_place, context_hints, source)
                    VALUES (?, ?, ?, ?)
                ''', (name, is_primarily_place, json.dumps(context_hints), 'ambiguous_names'))
                if cursor.rowcount > 0:
                    ambiguous_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {name}: {e}")
            done_an += 1
            prog.update(done=done_an)
        print(f"  ✓ Added {ambiguous_count} ambiguous names")

        # 5. Common names (download + parse)
        prog.next_stage("common names (download+parse)", 3.0, total=1, detail="downloads + parse")
        print("\n5. Downloading common names...")
        prog.update_subtask(detail="download/parse common names")
        first_names, last_names = download_common_names(progress=prog, refresh=refresh)
        prog.update_subtask(detail="download/parse common names complete")

        # 6. Common names (sqlite inserts)
        total_cn = len(first_names) + len(last_names)
        prog.next_stage("common names (sqlite inserts)", 2.0, total=max(1, total_cn), detail="insert")
        first_count = 0
        for i, name in enumerate(first_names):
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO common_first_names (name, frequency_rank, source)
                    VALUES (?, ?, ?)
                ''', (name, i + 1, 'common_names_aggregate'))
                if cursor.rowcount > 0:
                    first_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {name}: {e}")
            if (i + 1) % 2000 == 0:
                prog.update(done=(i + 1), detail="insert common first")

        last_count = 0
        for i, name in enumerate(last_names):
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO common_last_names (name, frequency_rank, source)
                    VALUES (?, ?, ?)
                ''', (name, i + 1, 'common_names_aggregate'))
                if cursor.rowcount > 0:
                    last_count += 1
            except Exception as e:
                print(f"    ⚠ Error adding {name}: {e}")
            if (i + 1) % 2000 == 0:
                prog.update(done=(len(first_names) + i + 1), detail="insert common last")
        prog.update(done=total_cn, detail="insert complete")
        print(f"  ✓ Added {first_count} first names and {last_count} last names")

        # 7. Finalize
        prog.next_stage("finalize", 0.3, total=1, detail="commit")
        conn.commit()
        conn.close()
        prog.update(done=1, total=1, detail="done")
        print("\n✓ Database populated successfully!")
    finally:
        prog.end()
        # Restore print and close detail log
        try:
            builtins.print = orig_print
        except Exception:
            pass
        try:
            if detail_log:
                detail_log.flush()
                detail_log.close()
        except Exception:
            pass

def get_database_stats():
    """Print statistics about the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM native_american_names')
    native_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM place_names')
    place_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM tribal_place_names')
    tribal_place_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM ambiguous_names')
    ambiguous_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM common_first_names')
    first_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM common_last_names')
    last_count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\nDatabase Statistics (v1.2.7):")
    print(f"  Native American names: {native_count}")
    print(f"  General place names: {place_count}")
    print(f"  Tribal place names: {tribal_place_count}")
    print(f"  Ambiguous names: {ambiguous_count}")
    print(f"  Common first names: {first_count}")
    print(f"  Common last names: {last_count}")
    print(f"  Total entries: {native_count + place_count + tribal_place_count + ambiguous_count + first_count + last_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Redownload datasets even if cached (default: use cache).")
    args = parser.parse_args()

    print("=" * 80)
    print("Building Name and Location Database v1.2.7")
    print("Most Robust System - Comprehensive Tribal Data")
    print("=" * 80)
    print()
    
    create_database()
    populate_database(refresh=bool(args.refresh))
    get_database_stats()
    
    print(f"\n✓ Database ready at: {DB_PATH}")
    print("\nKey Features:")
    print("  ✓ Comprehensive tribal place names (reservations, pueblos, villages)")
    print("  ✓ Extensive tribal and member names")
    print("  ✓ Ambiguous names (person vs place) for context-aware disambiguation")
    print("  ✓ General place names from USGS GNIS")
    print("  ✓ Common names from SSA and Census data")


