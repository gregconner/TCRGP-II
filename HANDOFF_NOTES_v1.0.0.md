## Handoff Notes (Dropbox-based workspace)

This file is meant to let you pick up work on another computer using the **same Dropbox-synced filesystem** without losing progress.

### Workspace location (Dropbox)
- Primary working folder: `/Users/gregoryconner/Dropbox/Cursor/TCRGP II Analysis`
- GitHub is **backup**, not the primary sync mechanism. Dropbox is the day-to-day “single source of truth” for files.

### Current “active” versions (source of truth = `VERSION`)
Open `VERSION` to confirm these exact entries are present:
- **Cleaner**: `deidentify_and_tag_transcripts_v1.18.7`
- **Grader**: `grade_transcript_cleaning_v1.1.3`
- **Iter loop**: `iterative_improvement_loop_v1.2.8`
- **Progress monitor**: `progress_monitor_v1.0.0`

### Current git snapshot (backup / reproducibility)
- Commit: `5ba768c9d8b8528dfe570d9bf8e881d0a113c9d7`
- Message: `Add .cursorignore to prevent Cursor RAM blowups (exclude transcripts + outputs)`

---

## What the system does (the pipeline)

You have a 3-stage pipeline:
1) **Cleaner** (`deidentify_and_tag_transcripts_v1.18.7.py`)
   - Reads raw transcripts (often DOCX-derived text).
   - Removes timestamp junk, normalizes formatting.
   - De-identifies PII (especially person names) and produces a **human-readable** transcript with citation tags.
   - Writes cleaned outputs + mapping files.

2) **Grader** (`grade_transcript_cleaning_v1.1.3.py`)
   - Scores cleaned transcripts against the rubric.
   - Rubric was updated to match the cleaner’s output format (speaker labels removed from the main transcript lines).

3) **Iterative loop** (`iterative_improvement_loop_v1.2.8.py`)
   - Runs clean → grade repeatedly.
   - Stops when it detects “no progress” (not necessarily meaning everything is 100%).
   - Writes a status JSON so you can monitor progress without relying on stdout.

---

## Output format requirements (implemented)

### 1) Citation lines: NO role labels in main text
The cleaned transcript’s main body uses:
- **`[A.1] …text…`** (citation only)

It should NOT embed speaker roles like:
- `[A.1] Interviewer: …text…`

### 2) Timestamp / citation reference table is sparse/conditional
The end-of-transcript reference table:
- Only includes entries where timestamps exist.
- If no timecodes exist in the raw transcript, the cleaner prints a single-line note indicating **no timecodes were available**.
  - The grader accepts this as satisfying the timestamp-table expectation.

### 3) Person codes are readable (#A/#B/… and beyond 26)
Person tags are:
- `#A`, `#B`, … `#Z`, `#AA`, `#AB`, … (Excel-style)

If a referenced person is also one of the speakers, their person tag is aligned with the speaker letter:
- speaker `B` ↔ person `#B`

### 4) Tribe/ethnonym terms come from the SQLite database (not hardcoded)
Tribe words must be loaded from the project DB and treated as **tribe terms**, not people.

DB file (SQLite):
- `name_location_database.db`

### 5) Ambiguous capitalized common words are marked for review (not auto-deidentified)
Common words that can also be names (e.g., Will/May) are bracketed:
- `[Will]`, `[May]`

This is controlled by `--ambiguous-policy`:
- `mark_all` or `pos_based` (POS-based requires spaCy POS/NER signals).

### 6) WEBVTT timestamp fragments are removed
DOCX extraction sometimes leaves fragments like:
- `.090 --> .280`

Cleaner has logic to remove these.

---

## Performance + stability (what changed and why)

### Cleaner replacement-step performance fix
The cleaner previously “hung” during deidentification because it was doing expensive replacement work.
The current approach only replaces strings that were actually extracted as person entities, using a combined regex pass.

### Loop memory stability: single-process cleaning
The loop uses **single-process cleaning** to avoid OOM from loading big NER models multiple times simultaneously.
Grading can still be parallel (lighter memory).

---

## Cursor (the editor) RAM blowup fix (critical)

Your system got killed because **Cursor was using ~88GB RAM**. This was caused by Cursor indexing large/binary/generated files in the workspace.

Fix applied:
- `.cursorignore` was added and committed.
- It excludes:
  - `older transcripts/`, `newer transcripts/`, and `*.docx`
  - `iteration_outputs/`
  - `*_mapping.json` and generated logs/status
  - common caches / venvs

On the new computer:
- Open the workspace in Cursor
- **Reload Window / restart Cursor** so it reindexes with the new `.cursorignore`

---

## Where outputs and progress live

The iterative loop writes to:
- `iteration_outputs/`

Key monitoring files:
- `iteration_outputs/iter_loop_status.json` (per-second-ish status for progress/ETA)
- `iteration_outputs/cleaner.log` (streamed cleaner output)
- `iteration_outputs/progress_monitor.log` (if you run the monitor script)

If a run stops:
- Check `iter_loop_status.json` for `phase`, `reason`, and last task/subtask.

---

## What happened in the most recent run

The loop completed but stopped with:
- `reason: "no_progress"`

This means it ran iterations and decided it wasn’t improving grades further. It does **not** guarantee everything is 100%.

Next step when you continue:
- Inspect the latest grade outputs under `iteration_outputs/` and see what deductions remain.

---

## Practical restart checklist (new computer, same Dropbox)

1) Let Dropbox fully sync the folder.
2) Open `/Users/gregoryconner/Dropbox/Cursor/TCRGP II Analysis` in Cursor.
3) Restart/Reload Cursor so `.cursorignore` takes effect (important for RAM).
4) Open `VERSION` and confirm the active versions (cleaner/grader/loop).
5) Run the loop (whatever command you already use to execute `iterative_improvement_loop_v1.2.8.py`).
6) If you want live progress without trusting stdout:
   - Watch `iteration_outputs/iter_loop_status.json`
   - Tail `iteration_outputs/cleaner.log`

---

## Notes on “rules” you’ve been enforcing

- All code files are versioned in the filename (`*_vX.Y.Z.py`).
- After changes:
  - bump versioned filename (or add new version)
  - update `VERSION`
  - git add/commit
  - push to GitHub (backup)


