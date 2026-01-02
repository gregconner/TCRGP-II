## Transcript citation standards (practical, widely used)

This project uses a **stable, human-readable reference system** designed for qualitative research workflows where readers need to locate quotations quickly in a transcript and, when possible, jump to the corresponding audio/video time.

### What researchers commonly do in practice

- **Speaker-turn + line numbering**
  - Many qualitative researchers cite **speaker turns** and/or **line numbers** in transcripts, often formatted like:
    - “(Interview X, lines 123–131)”
    - “(Participant A, lines 45–52)”
  - Line numbering is popular because it is:
    - **format-independent** (works in Word/PDF/plain text)
    - **human-checkable**
    - **stable** if the transcript content is not reflowed

- **Timestamp-based citation**
  - For audio/video-aligned transcripts, researchers often cite:
    - “(Interview X, 00:12:34–00:13:10)”
  - This is strongest when the transcript is derived from a timecoded source (WEBVTT, SRT, etc.).

- **Page numbering (PDF/Word)**
  - Page numbers are often used when the transcript is distributed as a PDF.
  - Page numbers are **layout-dependent**, so they are best used as a secondary aid (with a more stable system underneath).

### Notes on “standard” systems

There is no single universal ISO-style canonical numbering for interview transcripts equivalent to e.g. **Stephanus pagination** (Plato) or **Bekker numbers** (Aristotle). In qualitative methods, the de facto “standard” is **clear, reproducible locators** (line numbers, timestamps, and consistent speaker IDs) that reviewers can verify.

### Project standard (this repo)

This repo standardizes citation as:

- **Speaker letter + verse number**: `[A.12]`
- **Optional page** (for navigation in long outputs): `Page N`
- **Timestamp lookup table** at the end mapping `Speaker.Verse → timestamp`

This behaves like chapter:verse numbering:

- **Speaker letter**: A, B, C… (unique speakers)
- **Verse**: counts each speaker turn (or each non-empty line for prose transcripts)

Example:

> `[B.7] Interviewee: ...`

### How to cite from a cleaned transcript (recommended)

- **Short**: `(Interview Name, B.7)`
- **With page**: `(Interview Name, Page 4, B.7)`
- **With timestamp**: `(Interview Name, B.7; see timestamp table: 00:12:34)`

### Stability guarantees

The implementation is designed to be stable across runs by:

- assigning speaker letters **deterministically** from `Person_#` order (not “first seen” order)
- numbering verses sequentially per speaker
- paginating by a fixed **lines-per-page** setting


