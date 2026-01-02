#!/usr/bin/env python3
"""
Iterative Improvement Loop v1.2.7

Automated iterative improvement system for transcript cleaning.
Runs: clean → grade → repeat until all A grades or no progress.

v1.2.7 changes (stability + observability):
- KEEP CLEANING SINGLE-PROCESS to avoid loading NER multiple times (prevents OOM/reboots)
- Stream cleaner stdout to a log file (no capture_output buffering)
- Write a per-second JSON status file compatible with progress_monitor_v1.0.0.py

Version: 1.2.7
"""

import argparse
import json
import multiprocessing
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional


def _detect_cleaner_version_from_version_file(version_file: Path) -> str:
    """Extract deidentify_and_tag_transcripts version from VERSION (best-effort)."""
    try:
        txt = version_file.read_text(encoding="utf-8", errors="ignore")
        for line in txt.splitlines():
            line = line.strip()
            if line.startswith("deidentify_and_tag_transcripts_v"):
                return line.split("_v", 1)[1]
    except Exception:
        pass
    # Fallback to the latest known good default.
    return "1.17.0"


def _start_per_second_ticker(on_tick):
    """Per-second SIGALRM ticker (no sleeps, no threads). Returns previous handler."""
    try:
        prev = signal.getsignal(signal.SIGALRM)

        def _handler(signum, frame):
            try:
                on_tick()
            except Exception:
                pass

        signal.signal(signal.SIGALRM, _handler)
        signal.setitimer(signal.ITIMER_REAL, 1, 1)
        return prev
    except Exception:
        return None


def _stop_per_second_ticker(prev_handler):
    try:
        signal.setitimer(signal.ITIMER_REAL, 0)
    except Exception:
        pass
    if prev_handler is not None:
        try:
            signal.signal(signal.SIGALRM, prev_handler)
        except Exception:
            pass


def _safe_write_json(path: Path, payload: Dict) -> None:
    """Best-effort JSON write for monitoring; never raises."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


def _tail_file(path: Path, max_bytes: int = 2048) -> str:
    """Read last N bytes of file for status display."""
    try:
        if not path.exists():
            return ""
        size = path.stat().st_size
        if size <= 0:
            return ""
        with open(path, "rb") as f:
            f.seek(max(0, size - max_bytes))
            chunk = f.read(max_bytes)
        return chunk.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _last_nonempty_line(s: str) -> str:
    lines = [ln.strip() for ln in (s or "").splitlines() if ln.strip()]
    return lines[-1] if lines else ""


def _status_payload(
    started_at: float,
    overall_pct: float,
    task_pct: float,
    task_name: str,
    sub_pct: float,
    sub_name: str,
    updated_at: Optional[float] = None,
    overall_eta_s: Optional[float] = None,
    task_eta_s: Optional[float] = None,
    sub_eta_s: Optional[float] = None,
    done: bool = False,
    extra: Optional[Dict] = None,
) -> Dict:
    payload = {
        "started_at": started_at,
        "updated_at": float(updated_at if updated_at is not None else time.time()),
        "overall_pct": float(max(0.0, min(100.0, overall_pct))),
        "overall_eta_s": overall_eta_s,
        "task_pct": float(max(0.0, min(100.0, task_pct))),
        "task_eta_s": task_eta_s,
        "task_name": task_name or "",
        "sub_pct": float(max(0.0, min(100.0, sub_pct))),
        "sub_eta_s": sub_eta_s,
        "sub_name": sub_name or "",
        "done": bool(done),
    }
    if extra:
        payload.update(extra)
    return payload


def run_cleaner_streaming(
    transcript_files: List[Path],
    output_dir: Path,
    version: str,
    status_file: Path,
    started_at: float,
    iteration: int,
) -> bool:
    """
    Run cleaner once (single process) and stream stdout/stderr to a log file.
    Tracks progress by counting 'Processing:' markers printed by the cleaner.
    """
    print(f"\n{'='*80}")
    print(f"RUNNING CLEANER (v{version}) — SINGLE PROCESS (RAM SAFE)")
    print(f"{'='*80}")

    cleaner_log = output_dir / "cleaner.log"

    cmd = [
        sys.executable,
        f"deidentify_and_tag_transcripts_v{version}.py",
        "--files",
        *[str(p) for p in transcript_files],
        "-o",
        str(output_dir),
    ]

    total = len(transcript_files)
    processed = 0
    current_name = ""
    last_line = ""
    t0 = time.time()

    # Initialize status before launching
    _safe_write_json(
        status_file,
        _status_payload(
            started_at=started_at,
            overall_pct=0.0,
            task_pct=0.0,
            task_name=f"iteration {iteration}: clean",
            sub_pct=0.0,
            sub_name="starting cleaner…",
            extra={"log_file": str(cleaner_log), "phase": "clean"},
        ),
    )

    with open(cleaner_log, "a", encoding="utf-8") as log_fp:
        log_fp.write(f"\n\n=== CLEAN START {datetime.now().isoformat()} | v{version} ===\n")
        log_fp.flush()

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,  # line-buffered
            )
        except Exception as e:
            log_fp.write(f"ERROR: failed to start cleaner: {e}\n")
            log_fp.flush()
            return False

        def _compute_eta(done_count: int) -> Optional[int]:
            if done_count <= 0:
                return None
            elapsed = max(time.time() - t0, 1e-6)
            rate = done_count / elapsed
            remaining = max(total - done_count, 0)
            return int(remaining / max(rate, 1e-6))

        def _tick():
            nonlocal last_line
            task_pct = (processed / total * 100.0) if total else 100.0
            overall_pct = task_pct * 0.5  # clean is first half
            tail = _tail_file(cleaner_log, max_bytes=2048)
            last_line = _last_nonempty_line(tail) or last_line
            _safe_write_json(
                status_file,
                _status_payload(
                    started_at=started_at,
                    overall_pct=overall_pct,
                    task_pct=task_pct,
                    task_name=f"iteration {iteration}: clean",
                    sub_pct=0.0,
                    sub_name=(current_name or last_line or "cleaning…"),
                    task_eta_s=_compute_eta(processed),
                    extra={
                        "log_file": str(cleaner_log),
                        "phase": "clean",
                        "processed_files": processed,
                        "total_files": total,
                        "current_file": current_name,
                        "last_line": last_line,
                    },
                ),
            )

        prev = _start_per_second_ticker(_tick)
        try:
            # Stream output; do not buffer in memory.
            assert proc.stdout is not None
            for line in proc.stdout:
                log_fp.write(line)
                log_fp.flush()
                stripped = (line or "").strip()
                if stripped:
                    last_line = stripped

                # Progress marker emitted by cleaner
                if stripped.startswith("Processing:"):
                    current_name = stripped.split("Processing:", 1)[1].strip()
                    processed = min(processed + 1, total)

                # Also write status on meaningful changes
                if stripped.startswith("Processing:") or stripped.startswith("  ✓ Created:"):
                    _tick()

            rc = proc.wait()
            _tick()
            if rc != 0:
                print(f"Error: Cleaner exited non-zero ({rc}). See log: {cleaner_log}")
                return False
            return True
        finally:
            _stop_per_second_ticker(prev)


def run_grader(raw_file: Path, cleaned_file: Path, mapping_file: Path, tags_file: Path, output_file: Path) -> Dict:
    """Run the grader on a cleaned transcript."""
    cmd = [
        sys.executable,
        "grade_transcript_cleaning_v1.1.3.py",
        str(raw_file),
        str(cleaned_file),
        "-m",
        str(mapping_file),
        "-t",
        str(tags_file),
        "-o",
        str(output_file),
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
        with open(output_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error running grader for {raw_file.name}: {e}")
        return {}


def run_grader_wrapper(args: Tuple) -> Tuple[str, Dict]:
    """Wrapper for parallel grading."""
    raw_file, cleaned_file, mapping_file, tags_file, grade_file = args
    report = run_grader(raw_file, cleaned_file, mapping_file, tags_file, grade_file)
    return (raw_file.name, report)


def analyze_grades(grading_reports: List[Dict]) -> Dict:
    """Analyze grades and identify improvement priorities."""
    analysis = {
        "overall_scores": [],
        "category_issues": {
            "deidentification_completeness": [],
            "deidentification_accuracy": [],
            "formatting_quality": [],
            "citation_system": [],
            "tagging_completeness": [],
            "tagging_precision": [],
            "spelling_matching": [],
        },
        "common_issues": [],
        "improvement_priorities": [],
    }

    for report in grading_reports:
        analysis["overall_scores"].append(report["overall_score"])

        for cat_key, cat_data in report["categories"].items():
            if cat_data["grade"] != "A":
                analysis["category_issues"][cat_key].append(
                    {
                        "transcript": report["transcript_name"],
                        "grade": cat_data["grade"],
                        "score": cat_data["score"],
                        "suggestions": cat_data["suggestions"],
                    }
                )

    if analysis["overall_scores"]:
        analysis["average_score"] = sum(analysis["overall_scores"]) / len(analysis["overall_scores"])
    else:
        analysis["average_score"] = 0

    all_suggestions = []
    for report in grading_reports:
        for cat_data in report["categories"].values():
            all_suggestions.extend(cat_data["suggestions"])

    from collections import Counter

    suggestion_counts = Counter(all_suggestions)
    analysis["common_issues"] = [item for item, count in suggestion_counts.most_common(10)]

    category_scores = {}
    for cat_key in analysis["category_issues"]:
        issues = analysis["category_issues"][cat_key]
        if issues:
            avg_score = sum(issue["score"] for issue in issues) / len(issues)
            category_scores[cat_key] = avg_score

    analysis["improvement_priorities"] = sorted(category_scores.items(), key=lambda x: x[1])
    return analysis


def generate_improvement_suggestions(analysis: Dict) -> List[str]:
    """Generate actionable improvement suggestions."""
    suggestions = []

    for cat_key, issues in analysis["category_issues"].items():
        if issues:
            if cat_key == "deidentification_completeness":
                suggestions.append("Improve entity extraction - some PII not being found")
                suggestions.append("Add more entity patterns or improve spaCy usage")
            elif cat_key == "deidentification_accuracy":
                suggestions.append("Improve name variant matching")
                suggestions.append("Fix false positive filtering")
            elif cat_key == "formatting_quality":
                suggestions.append("Fix timestamp removal")
                suggestions.append("Improve dialogue formatting")
            elif cat_key == "citation_system":
                suggestions.append("Fix citation system implementation")
            elif cat_key == "tagging_completeness":
                suggestions.append("Add more keyword patterns")
                suggestions.append("Improve tag coverage")
            elif cat_key == "tagging_precision":
                suggestions.append("Improve context-aware tagging")
                suggestions.append("Add more negative patterns")
            elif cat_key == "spelling_matching":
                suggestions.append("Improve fuzzy matching thresholds")
                suggestions.append("Add more misspelling corrections")

    return suggestions


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cleaner-version",
        default=None,
        help="Override deidentify_and_tag_transcripts version (e.g., 1.18.3). Default: read from VERSION.",
    )
    parser.add_argument("--num-cpus", type=int, default=multiprocessing.cpu_count(), help="CPUs for parallel grading (default: all).")
    parser.add_argument("--max-iterations", type=int, default=5, help="Safety limit.")
    parser.add_argument("--progress", action="store_true", help="Enable per-second progress ticker during grading.")
    parser.add_argument(
        "--status-file",
        default="iteration_outputs/iter_loop_status.json",
        help="Path to JSON status file (for progress_monitor_v1.0.0.py).",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("ITERATIVE IMPROVEMENT LOOP v1.2.7 (RAM-SAFE CLEANING + LIVE STATUS)")
    print(f"Using {max(1, args.num_cpus)} CPU cores for grading")
    print("=" * 80)

    script_dir = Path(__file__).parent
    newer_dir = script_dir / "newer transcripts"
    output_base = script_dir / "iteration_outputs"
    output_base.mkdir(exist_ok=True)

    status_file = (script_dir / args.status_file).resolve()
    started_at = time.time()

    transcript_files = sorted([f for f in newer_dir.glob("*.docx") if "Alaska" not in f.name])
    if len(transcript_files) < 5:
        print(f"Warning: Only found {len(transcript_files)} new transcripts")
    print(f"\nFound {len(transcript_files)} new transcript(s) to process")

    cleaner_version = (args.cleaner_version or _detect_cleaner_version_from_version_file(script_dir / "VERSION")).strip()
    grader_version = "1.1.3"
    iteration = 0
    previous_scores = []
    no_progress_count = 0
    max_iterations = max(1, args.max_iterations)

    # Prime status file
    _safe_write_json(
        status_file,
        _status_payload(
            started_at=started_at,
            overall_pct=0.0,
            task_pct=0.0,
            task_name="starting…",
            sub_pct=0.0,
            sub_name="initializing",
            extra={"phase": "init"},
        ),
    )

    while True:
        iteration += 1
        print(f"\n{'='*80}")
        print(f"ITERATION {iteration}")
        print(f"{'='*80}")

        iter_dir = output_base / f"iteration_{iteration}"
        iter_dir.mkdir(exist_ok=True)
        cleaned_dir = iter_dir / "cleaned"
        cleaned_dir.mkdir(exist_ok=True)
        grades_dir = iter_dir / "grades"
        grades_dir.mkdir(exist_ok=True)

        # Step 1: Clean (single process, stream output)
        print(f"\nStep 1: Running cleaner v{cleaner_version}...")
        if not run_cleaner_streaming(
            transcript_files=transcript_files,
            output_dir=cleaned_dir,
            version=cleaner_version,
            status_file=status_file,
            started_at=started_at,
            iteration=iteration,
        ):
            _safe_write_json(
                status_file,
                _status_payload(
                    started_at=started_at,
                    overall_pct=0.0,
                    task_pct=0.0,
                    task_name=f"iteration {iteration}: clean",
                    sub_pct=0.0,
                    sub_name="cleaner failed",
                    done=True,
                    extra={"phase": "clean", "error": True},
                ),
            )
            break

        # Step 2: Grade (parallel)
        print(f"\nStep 2: Running grader v{grader_version} (parallelized)...")
        grading_reports: List[Dict] = []

        grading_tasks = []
        for raw_file in transcript_files:
            base_name = raw_file.stem
            cleaned_file = cleaned_dir / f"{base_name}_deidentified.txt"
            mapping_file = cleaned_dir / f"{base_name}_mapping.json"
            tags_file = cleaned_dir / f"{base_name}_tags.csv"
            grade_file = grades_dir / f"{base_name}_grade.json"
            if cleaned_file.exists():
                grading_tasks.append((raw_file, cleaned_file, mapping_file, tags_file, grade_file))

        num_workers = max(1, min(args.num_cpus, len(grading_tasks)))
        print(f"  Using {num_workers} CPU cores for parallel grading...")

        completed = 0
        total = len(grading_tasks)
        t0 = time.time()

        def _grade_eta() -> Optional[int]:
            if completed <= 0:
                return None
            elapsed = max(time.time() - t0, 1e-6)
            rate = completed / elapsed
            remaining = max(total - completed, 0)
            return int(remaining / max(rate, 1e-6))

        def _grade_tick():
            task_pct = (completed / total * 100.0) if total else 100.0
            overall_pct = 50.0 + task_pct * 0.5
            _safe_write_json(
                status_file,
                _status_payload(
                    started_at=started_at,
                    overall_pct=overall_pct,
                    task_pct=task_pct,
                    task_name=f"iteration {iteration}: grade",
                    sub_pct=0.0,
                    sub_name=f"{completed}/{total} graded",
                    task_eta_s=_grade_eta(),
                    extra={"phase": "grade", "graded": completed, "total": total},
                ),
            )

        prev = _start_per_second_ticker(_grade_tick) if args.progress else None
        try:
            if num_workers > 1 and len(grading_tasks) > 1:
                with multiprocessing.Pool(processes=num_workers) as pool:
                    for filename, report in pool.imap_unordered(run_grader_wrapper, grading_tasks):
                        completed += 1
                        if report:
                            grading_reports.append(report)
                        print(f"  ✓ Graded: {filename}")
                        _grade_tick()
            else:
                for raw_file, cleaned_file, mapping_file, tags_file, grade_file in grading_tasks:
                    print(f"  Grading: {raw_file.name}")
                    report = run_grader(raw_file, cleaned_file, mapping_file, tags_file, grade_file)
                    completed += 1
                    if report:
                        grading_reports.append(report)
                    _grade_tick()
        finally:
            if args.progress:
                _stop_per_second_ticker(prev)
                sys.stdout.write("\n")
                sys.stdout.flush()

        # Step 3: Analyze grades
        print(f"\nStep 3: Analyzing grades...")
        analysis = analyze_grades(grading_reports)

        print(f"\n{'='*80}")
        print(f"ITERATION {iteration} RESULTS")
        print(f"{'='*80}")
        print(f"\nAverage Score: {analysis['average_score']:.1f}/100")
        print(f"\nCategory Grades:")
        for report in grading_reports:
            print(f"\n  {report['transcript_name']}: {report['overall_grade']} ({report['overall_score']:.1f}/100)")
            for cat_key, cat_data in report["categories"].items():
                if cat_data["grade"] != "A":
                    print(f"    - {cat_key}: {cat_data['grade']} ({cat_data['score']:.1f})")

        all_a_grades = all(
            report["overall_grade"] == "A" or report["overall_grade"] == "A-"
            for report in grading_reports
        )
        if all_a_grades:
            print(f"\n{'='*80}")
            print("SUCCESS: All transcripts achieved A grades!")
            print(f"{'='*80}")
            _safe_write_json(
                status_file,
                _status_payload(
                    started_at=started_at,
                    overall_pct=100.0,
                    task_pct=100.0,
                    task_name=f"iteration {iteration}: done",
                    sub_pct=100.0,
                    sub_name="all A grades",
                    done=True,
                    extra={"phase": "done"},
                ),
            )
            break

        current_avg = analysis["average_score"]
        if previous_scores:
            if abs(current_avg - previous_scores[-1]) < 0.5:
                no_progress_count += 1
                print(f"\nNo significant progress (change: {current_avg - previous_scores[-1]:.2f})")
                print(f"No progress count: {no_progress_count}/2")
            else:
                no_progress_count = 0
                print(f"\nProgress: {previous_scores[-1]:.1f} → {current_avg:.1f} (+{current_avg - previous_scores[-1]:.2f})")

        previous_scores.append(current_avg)

        if no_progress_count >= 2:
            print(f"\n{'='*80}")
            print("STOPPING: No progress for 2 iterations")
            print(f"{'='*80}")
            _safe_write_json(
                status_file,
                _status_payload(
                    started_at=started_at,
                    overall_pct=100.0,
                    task_pct=100.0,
                    task_name=f"iteration {iteration}: stopped",
                    sub_pct=100.0,
                    sub_name="no progress",
                    done=True,
                    extra={"phase": "stopped", "reason": "no_progress"},
                ),
            )
            break

        # Step 4: Generate improvement suggestions (informational only)
        print(f"\nStep 4: Generating improvement suggestions...")
        suggestions = generate_improvement_suggestions(analysis)
        print(f"\nImprovement Priorities:")
        for priority, score in analysis["improvement_priorities"][:5]:
            print(f"  - {priority}: {score:.1f}")
        print(f"\nSuggestions:")
        for sugg in suggestions[:10]:
            print(f"  • {sugg}")

        summary_file = iter_dir / "iteration_summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "iteration": iteration,
                    "average_score": current_avg,
                    "grading_reports": grading_reports,
                    "analysis": analysis,
                    "suggestions": suggestions,
                },
                f,
                indent=2,
            )
        print(f"\n✓ Iteration {iteration} complete. Summary saved to {summary_file}")

        if iteration >= max_iterations:
            print(f"\n{'='*80}")
            print(f"STOPPING: Reached maximum iterations ({max_iterations})")
            print(f"{'='*80}")
            _safe_write_json(
                status_file,
                _status_payload(
                    started_at=started_at,
                    overall_pct=100.0,
                    task_pct=100.0,
                    task_name=f"iteration {iteration}: stopped",
                    sub_pct=100.0,
                    sub_name="max iterations",
                    done=True,
                    extra={"phase": "stopped", "reason": "max_iterations"},
                ),
            )
            break

        print(f"\nContinuing to iteration {iteration + 1}...")


if __name__ == "__main__":
    main()


