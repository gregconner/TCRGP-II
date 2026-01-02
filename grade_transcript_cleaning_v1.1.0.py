#!/usr/bin/env python3
"""
Transcript Cleaning Grading System v1.1.0

Revision goals (v1.1.0):
- Fix brittle citation checks (support multi-letter speakers like AA.1, not just A.1)
- Fix tagging completeness calculation (only count taggable content lines, not blank lines / tables)
- Avoid common false positives in "remaining PII" (timestamps like 00:21:16, page markers)
- Normalize scoring so an "A" in a category yields the category's full max points (overall max = 100)

INPUT:
- Raw transcript (original, with PII)
- Cleaned transcript (de-identified output)
- Mapping file (optional)
- Tags file (optional)

OUTPUT:
- JSON grading report

Version: 1.1.0
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from datetime import datetime

# Try to import spaCy for entity extraction (best-effort).
try:
    import spacy
    try:
        nlp = spacy.load("en_core_web_md")
        SPACY_AVAILABLE = True
    except OSError:
        try:
            nlp = spacy.load("en_core_web_sm")
            SPACY_AVAILABLE = True
        except OSError:
            nlp = None
            SPACY_AVAILABLE = False
except ImportError:
    nlp = None
    SPACY_AVAILABLE = False


# ============================================================================
# CONFIG
# ============================================================================

EXCLUDED_WORDS = {
    "persons": {"some", "project", "the future", "that you", "like you"},
    "organizations": {"the cooperative", "a cooperative", "this co-op"},
    "locations": {"some", "project", "future", "information"},
}

PERSON_PATTERNS = [
    r"^([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:",  # Speaker labels
]

TS_HHMMSS_RE = re.compile(r"^\d{2}:\d{2}:\d{2}(?:\.\d+)?$")

# Multi-letter speaker support: A.1, Z.10, AA.3, etc.
SPEAKER_VERSE_RE = re.compile(r"\[([A-Z]{1,3})\.(\d+)\]")


# ============================================================================
# IO
# ============================================================================

def extract_text_from_docx(docx_path: Path) -> str:
    try:
        from docx import Document
        doc = Document(docx_path)
        full_text = []
        for para in doc.paragraphs:
            if para.text.strip():
                full_text.append(para.text)
        return "\n".join(full_text)
    except Exception:
        return ""


# ============================================================================
# ENTITY EXTRACTION (RAW "GROUND TRUTH" - BEST EFFORT)
# ============================================================================

def _looks_like_timestamp(token: str) -> bool:
    return bool(re.match(r"^\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?$", token.strip()))

def extract_entities_from_text(text: str, use_spacy: bool = True) -> Dict[str, Set[str]]:
    entities: Dict[str, Set[str]] = {
        "persons": set(),
        "organizations": set(),
        "locations": set(),
        "tribes": set(),
        "financial_amounts": set(),
        "years": set(),
    }

    # Speaker labels
    for pattern in PERSON_PATTERNS:
        for match in re.finditer(pattern, text, re.MULTILINE):
            name = match.group(1).strip()
            if len(name) > 2 and name.lower() not in EXCLUDED_WORDS.get("persons", set()):
                entities["persons"].add(name)

    # spaCy entities
    if use_spacy and SPACY_AVAILABLE and nlp:
        doc = nlp(text)
        for ent in doc.ents:
            entity_text = ent.text.strip()
            entity_lower = entity_text.lower()

            # Drop obvious non-PII tokens and timestamp-like strings
            if _looks_like_timestamp(entity_text):
                continue
            if entity_lower in {"page", "n/a"}:
                continue

            if ent.label_ == "PERSON":
                if (
                    len(entity_text) > 2
                    and entity_lower not in EXCLUDED_WORDS.get("persons", set())
                    and not any(word in entity_lower for word in ["some", "project", "future"])
                ):
                    entities["persons"].add(entity_text)
            elif ent.label_ in ["GPE", "LOC"]:
                if len(entity_text) > 2 and entity_lower not in EXCLUDED_WORDS.get("locations", set()):
                    entities["locations"].add(entity_text)
            elif ent.label_ == "ORG":
                if len(entity_text) > 3 and entity_lower not in EXCLUDED_WORDS.get("organizations", set()):
                    entities["organizations"].add(entity_text)

    # Financial amounts
    for m in re.finditer(r"\$[\d,]+(?:\s*(?:million|thousand|billion))?", text, re.IGNORECASE):
        entities["financial_amounts"].add(m.group())

    # Years (timeline contexts)
    for m in re.finditer(r"\b(19|20)\d{2}\b", text):
        ctx = text[max(0, m.start() - 30) : min(len(text), m.end() + 30)].lower()
        if any(w in ctx for w in ["founded", "established", "since", "year", "started", "began"]):
            entities["years"].add(m.group())

    # Tribe / Nation / Pueblo
    for m in re.finditer(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Nation|Tribe|Pueblo)\b", text):
        entities["tribes"].add(m.group(1))

    return entities


# ============================================================================
# CHECKS
# ============================================================================

def check_pii_remaining(raw_entities: Dict[str, Set[str]], cleaned_text: str) -> Dict[str, List[str]]:
    remaining: Dict[str, List[str]] = {k: [] for k in raw_entities.keys()}
    cleaned_lower = cleaned_text.lower()

    for entity_type, entity_set in raw_entities.items():
        for entity in entity_set:
            entity_lower = entity.lower().strip()

            # Don’t treat timestamps as residual PII
            if _looks_like_timestamp(entity_lower):
                continue

            pattern = r"\b" + re.escape(entity_lower) + r"\b"
            if re.search(pattern, cleaned_lower):
                pos = cleaned_lower.find(entity_lower)
                ctx = cleaned_text[max(0, pos - 30) : min(len(cleaned_text), pos + len(entity) + 30)]
                # Skip if it’s part of a de-id code
                if not re.search(r"(Person|Location|Organization|Tribe)_\d+", ctx, re.IGNORECASE):
                    remaining[entity_type].append(entity)

    return remaining


def _split_main_content_lines(cleaned_text: str) -> List[Tuple[int, str]]:
    """
    Identify "taggable content lines" for coverage:
    - Excludes blank lines
    - Excludes page markers (Page N)
    - Excludes the citation reference table block (header + rows)
    """
    lines = cleaned_text.splitlines()
    out: List[Tuple[int, str]] = []

    in_table = False
    for i, line in enumerate(lines, 1):
        s = line.strip()
        if not s:
            continue

        if "CITATION REFERENCE TABLE" in s:
            in_table = True
            continue
        if in_table:
            # stop including table content entirely
            continue

        if re.match(r"^Page\s+\d+\s*$", s, re.IGNORECASE):
            continue
        if re.match(r"^[=\-]{10,}\s*$", s):
            continue

        out.append((i, line))

    return out


def check_citation_system(cleaned_text: str) -> Dict[str, object]:
    results: Dict[str, object] = {
        "has_speaker_letters": False,
        "has_verse_numbers": False,
        "has_page_numbers": False,
        "has_timestamp_table": False,
        "speaker_letter_count": 0,
        "verse_count": 0,
        "page_count": 0,
        "issues": [],
    }

    matches = SPEAKER_VERSE_RE.findall(cleaned_text)
    if matches:
        results["has_speaker_letters"] = True
        results["has_verse_numbers"] = True
        results["speaker_letter_count"] = len(set(m[0] for m in matches))
        results["verse_count"] = len(matches)
    else:
        results["issues"].append("No speaker letters or verse numbers found")

    page_matches = re.findall(r"Page\s+(\d+)", cleaned_text, re.IGNORECASE)
    if page_matches:
        results["has_page_numbers"] = True
        results["page_count"] = len(set(page_matches))
    else:
        results["issues"].append("No page numbers found")

    # Timestamp table: require header + at least one row like "A.1 |"
    if "CITATION REFERENCE TABLE" in cleaned_text:
        row_re = re.compile(r"^\s*([A-Z]{1,3}\.\d+)\s*\|\s*([0-9Nn]/?A|N/A|\d{2}:\d{2}:\d{2})", re.MULTILINE)
        if row_re.search(cleaned_text):
            results["has_timestamp_table"] = True
        else:
            results["issues"].append("Timestamp table header found but no rows detected")
    else:
        results["issues"].append("No timestamp table found")

    return results


def check_formatting_quality(cleaned_text: str) -> Dict[str, object]:
    results: Dict[str, object] = {
        "timestamps_removed": True,
        "webvtt_removed": True,
        "has_dialogue_format": False,
        "formatting_artifacts": [],
        "issues": [],
    }

    if re.search(r"\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}", cleaned_text):
        results["timestamps_removed"] = False
        results["issues"].append("WEBVTT arrow timestamps still present")

    # Also treat standalone HH:MM:SS lines as timestamp leakage
    for line in cleaned_text.splitlines():
        if TS_HHMMSS_RE.match(line.strip()):
            results["timestamps_removed"] = False
            results["issues"].append("Standalone HH:MM:SS timestamp lines present")
            break

    if "WEBVTT" in cleaned_text:
        results["webvtt_removed"] = False
        results["issues"].append("WEBVTT header still present")

    # Dialogue/citation format: any [A.1] style (multi-letter supported)
    if re.search(r"^\[[A-Z]{1,3}\.\d+\]\s+.+?:\s+.+", cleaned_text, re.MULTILINE):
        results["has_dialogue_format"] = True
    else:
        results["issues"].append("No clear dialogue/citation format found")

    if re.search(r"Person_\d+\.?\s*$", cleaned_text, re.MULTILINE):
        results["formatting_artifacts"].append("Standalone Person_X codes found")
    if re.search(r"\n{4,}", cleaned_text):
        results["formatting_artifacts"].append("Excessive blank lines")

    return results


def check_tagging_quality(cleaned_text: str, tags_file: Optional[Path]) -> Dict[str, object]:
    results: Dict[str, object] = {
        "has_tags": False,
        "tag_count": 0,
        "category_count": 0,
        "false_positives": [],
        "coverage_estimate": 0.0,
        "taggable_lines": 0,
        "tagged_taggable_lines": 0,
    }

    if not tags_file or not tags_file.exists():
        return results

    try:
        import csv

        with open(tags_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        results["has_tags"] = True
        results["tag_count"] = len(rows)
        results["category_count"] = len(set(r.get("Tag_Category", "") for r in rows if r.get("Tag_Category")))

        taggable = _split_main_content_lines(cleaned_text)
        taggable_line_nums = {ln for ln, _ in taggable}
        results["taggable_lines"] = len(taggable_line_nums)

        tagged_lines = set()
        for r in rows:
            ln = r.get("Line_Number", "")
            if ln and ln.isdigit():
                tagged_lines.add(int(ln))

        tagged_taggable = tagged_lines.intersection(taggable_line_nums)
        results["tagged_taggable_lines"] = len(tagged_taggable)

        if results["taggable_lines"] > 0:
            results["coverage_estimate"] = results["tagged_taggable_lines"] / results["taggable_lines"]
        else:
            results["coverage_estimate"] = 1.0
    except Exception as e:
        results["false_positives"].append(f"Error reading tags file: {e}")

    return results


def check_name_matching_accuracy(mapping_file: Optional[Path]) -> Dict[str, object]:
    results: Dict[str, object] = {
        "has_mapping": False,
        "person_count": 0,
        "name_variants": {},
        "issues": [],
    }
    if mapping_file and mapping_file.exists():
        try:
            with open(mapping_file, "r", encoding="utf-8") as f:
                m = json.load(f)
            results["has_mapping"] = True
            results["person_count"] = len(set(m.get("persons", {}).values()))
            results["name_variants"] = m.get("name_variants", {})
        except Exception as e:
            results["issues"].append(f"Error reading mapping file: {e}")
    return results


# ============================================================================
# GRADING
# ============================================================================

def _letter_grade_from_pct(pct: float) -> str:
    if pct >= 0.93:
        return "A"
    if pct >= 0.90:
        return "A-"
    if pct >= 0.87:
        return "B+"
    if pct >= 0.83:
        return "B"
    if pct >= 0.80:
        return "B-"
    if pct >= 0.77:
        return "C+"
    if pct >= 0.73:
        return "C"
    if pct >= 0.70:
        return "C-"
    if pct >= 0.67:
        return "D+"
    if pct >= 0.60:
        return "D"
    return "F"


def grade_deidentification_completeness(remaining_pii: Dict[str, List[str]]) -> Tuple[str, float, List[str]]:
    max_score = 25.0
    total_pii = sum(len(v) for v in remaining_pii.values())
    if total_pii == 0:
        score = max_score
        grade = "A"
        suggestions = ["Perfect de-identification - no PII remaining"]
    else:
        # Penalize 1 point per residual item, up to max.
        score = max(0.0, max_score - float(total_pii))
        grade = _letter_grade_from_pct(score / max_score)
        suggestions = [f"{total_pii} PII items remaining"]
        for t, items in remaining_pii.items():
            if items:
                suggestions.append(f"  - {len(items)} {t}: {', '.join(items[:5])}")
    return grade, score, suggestions


def grade_deidentification_accuracy(remaining_pii: Dict[str, List[str]], total_entities: int) -> Tuple[str, float, List[str]]:
    max_score = 20.0
    if total_entities <= 0:
        return "N/A", max_score, ["No entities found to evaluate"]
    error_rate = sum(len(v) for v in remaining_pii.values()) / float(total_entities)
    score = max(0.0, max_score * (1.0 - error_rate))
    grade = _letter_grade_from_pct(score / max_score)
    return grade, score, [f"Error rate: {error_rate:.1%}"]


def grade_formatting_quality(fmt: Dict[str, object]) -> Tuple[str, float, List[str]]:
    max_score = 15.0
    score = max_score
    issues: List[str] = []

    if not fmt.get("timestamps_removed", True):
        score -= 4
        issues.append("Timestamps not removed")
    if not fmt.get("webvtt_removed", True):
        score -= 2
        issues.append("WEBVTT not removed")
    if not fmt.get("has_dialogue_format", False):
        score -= 4
        issues.append("No dialogue/citation format")

    artifacts = list(fmt.get("formatting_artifacts", []) or [])
    if artifacts:
        score -= float(min(3, len(artifacts)))  # cap penalty
        issues.extend(artifacts)

    score = max(0.0, score)
    grade = _letter_grade_from_pct(score / max_score)
    suggestions = list(fmt.get("issues", []) or []) + issues
    if not suggestions:
        suggestions = ["Excellent formatting quality"]
    return grade, score, suggestions


def grade_citation_system(cit: Dict[str, object]) -> Tuple[str, float, List[str]]:
    max_score = 15.0
    score = max_score
    issues: List[str] = []

    if not cit.get("has_speaker_letters", False):
        score -= 5
        issues.append("Speaker letters missing")
    if not cit.get("has_verse_numbers", False):
        score -= 5
        issues.append("Verse numbers missing")
    if not cit.get("has_page_numbers", False):
        score -= 3
        issues.append("Page numbers missing")
    if not cit.get("has_timestamp_table", False):
        score -= 2
        issues.append("Timestamp table missing")

    extra = list(cit.get("issues", []) or [])
    for e in extra:
        if e not in issues:
            issues.append(e)

    score = max(0.0, score)
    grade = _letter_grade_from_pct(score / max_score)

    suggestions = issues if issues else ["Complete citation system"]
    sc = cit.get("speaker_letter_count", 0) or 0
    vc = cit.get("verse_count", 0) or 0
    if sc or vc:
        suggestions.append(f"Found {sc} speakers, {vc} verses")
    return grade, score, suggestions


def grade_tagging_completeness(tag: Dict[str, object]) -> Tuple[str, float, List[str]]:
    max_score = 10.0
    if not tag.get("has_tags", False):
        return "F", 0.0, ["No tags file found"]

    coverage = float(tag.get("coverage_estimate", 0.0) or 0.0)
    score = max(0.0, min(max_score, coverage * max_score))
    grade = _letter_grade_from_pct(score / max_score)
    suggestions = [
        f"Tag coverage (taggable lines): {coverage:.1%}",
        f"Tagged/taggable lines: {tag.get('tagged_taggable_lines', 0)}/{tag.get('taggable_lines', 0)}",
        f"Total tags: {tag.get('tag_count', 0)}",
        f"Categories: {tag.get('category_count', 0)}",
    ]
    return grade, score, suggestions


def grade_tagging_precision(tag: Dict[str, object]) -> Tuple[str, float, List[str]]:
    max_score = 10.0
    false_positive_count = len(tag.get("false_positives", []) or [])
    # Placeholder: if the reader parsing succeeded, assume OK unless explicit issues were recorded.
    score = max_score if false_positive_count == 0 else max(0.0, max_score - float(false_positive_count))
    grade = _letter_grade_from_pct(score / max_score)
    suggestions = tag.get("false_positives", []) or ["No obvious false positives detected"]
    return grade, score, suggestions


def grade_spelling_matching(m: Dict[str, object]) -> Tuple[str, float, List[str]]:
    max_score = 5.0
    if not m.get("has_mapping", False):
        return "F", 0.0, ["No mapping file found"]

    variant_count = 0
    try:
        for variants in (m.get("name_variants", {}) or {}).values():
            if isinstance(variants, list):
                variant_count += len(variants)
    except Exception:
        pass

    # Heuristic: reward any evidence of variant clustering, but keep full score attainable.
    score = max_score if variant_count >= 5 else max(1.0, min(max_score, float(variant_count)))
    grade = _letter_grade_from_pct(score / max_score)
    suggestions = [
        f"Name variants detected: {variant_count}",
        f"Unique persons: {m.get('person_count', 0)}",
    ]
    if m.get("issues"):
        suggestions.extend(m["issues"])
    return grade, score, suggestions


def grade_transcript_cleaning(
    raw_transcript_path: Path,
    cleaned_transcript_path: Path,
    mapping_file_path: Optional[Path] = None,
    tags_file_path: Optional[Path] = None,
) -> Dict[str, object]:
    # Read raw
    if raw_transcript_path.suffix == ".docx":
        raw_text = extract_text_from_docx(raw_transcript_path)
    else:
        raw_text = raw_transcript_path.read_text(encoding="utf-8", errors="ignore")

    cleaned_text = cleaned_transcript_path.read_text(encoding="utf-8", errors="ignore")

    raw_entities = extract_entities_from_text(raw_text, use_spacy=SPACY_AVAILABLE)
    total_entities = sum(len(v) for v in raw_entities.values())

    remaining_pii = check_pii_remaining(raw_entities, cleaned_text)
    formatting_results = check_formatting_quality(cleaned_text)
    citation_results = check_citation_system(cleaned_text)
    tagging_results = check_tagging_quality(cleaned_text, tags_file_path)
    matching_results = check_name_matching_accuracy(mapping_file_path)

    g1, s1, sugg1 = grade_deidentification_completeness(remaining_pii)
    g2, s2, sugg2 = grade_deidentification_accuracy(remaining_pii, total_entities)
    g3, s3, sugg3 = grade_formatting_quality(formatting_results)
    g4, s4, sugg4 = grade_citation_system(citation_results)
    g5, s5, sugg5 = grade_tagging_completeness(tagging_results)
    g6, s6, sugg6 = grade_tagging_precision(tagging_results)
    g7, s7, sugg7 = grade_spelling_matching(matching_results)

    total_score = float(s1 + s2 + s3 + s4 + s5 + s6 + s7)
    overall_grade = _letter_grade_from_pct(total_score / 100.0)

    return {
        "transcript_name": cleaned_transcript_path.name,
        "date": datetime.now().isoformat(),
        "overall_grade": overall_grade,
        "overall_score": total_score,
        "categories": {
            "deidentification_completeness": {"grade": g1, "score": s1, "max_score": 25, "suggestions": sugg1},
            "deidentification_accuracy": {"grade": g2, "score": s2, "max_score": 20, "suggestions": sugg2},
            "formatting_quality": {"grade": g3, "score": s3, "max_score": 15, "suggestions": sugg3},
            "citation_system": {"grade": g4, "score": s4, "max_score": 15, "suggestions": sugg4},
            "tagging_completeness": {"grade": g5, "score": s5, "max_score": 10, "suggestions": sugg5},
            "tagging_precision": {"grade": g6, "score": s6, "max_score": 10, "suggestions": sugg6},
            "spelling_matching": {"grade": g7, "score": s7, "max_score": 5, "suggestions": sugg7},
        },
        "detailed_findings": {
            "remaining_pii": remaining_pii,
            "formatting_issues": list(formatting_results.get("issues", []) or []),
            "citation_issues": list(citation_results.get("issues", []) or []),
            "tagging_stats": {
                "tag_count": tagging_results.get("tag_count", 0),
                "category_count": tagging_results.get("category_count", 0),
                "coverage": tagging_results.get("coverage_estimate", 0.0),
                "taggable_lines": tagging_results.get("taggable_lines", 0),
                "tagged_taggable_lines": tagging_results.get("tagged_taggable_lines", 0),
            },
        },
        "meta": {
            "grader_version": "1.1.0",
            "spacy_available": SPACY_AVAILABLE,
        },
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Grade transcript cleaning quality (v1.1.0)")
    parser.add_argument("raw_transcript", type=str)
    parser.add_argument("cleaned_transcript", type=str)
    parser.add_argument("-m", "--mapping", type=str)
    parser.add_argument("-t", "--tags", type=str)
    parser.add_argument("-o", "--output", type=str)
    args = parser.parse_args()

    raw_path = Path(args.raw_transcript)
    cleaned_path = Path(args.cleaned_transcript)
    mapping_path = Path(args.mapping) if args.mapping else None
    tags_path = Path(args.tags) if args.tags else None

    report = grade_transcript_cleaning(raw_path, cleaned_path, mapping_path, tags_path)

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()


