#!/usr/bin/env python3
"""
Transcript Cleaning Grading System v1.0.0

Comprehensive evaluation system for de-identification and tagging quality.
Compares raw transcripts to cleaned versions and provides detailed grades
and improvement suggestions.

INPUT:
- Raw transcript (original, with PII)
- Cleaned transcript (de-identified output)
- Mapping file (optional, for verification)

OUTPUT:
- Grades (A-F) for each category
- Overall grade
- Detailed suggestions for improvement
- Quantitative metrics

Version: 1.0.0
"""

import re
import json
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Set, Optional
from datetime import datetime
from difflib import SequenceMatcher

# Try to import spaCy for entity extraction
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
# CONFIGURATION
# ============================================================================

# Common words to exclude from entity extraction
EXCLUDED_WORDS = {
    "persons": {"some", "project", "the future", "that you", "like you"},
    "organizations": {"the cooperative", "a cooperative", "this co-op"},
    "locations": {"some", "project", "future", "information"}
}

# Person name patterns
PERSON_PATTERNS = [
    r'^([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:',  # Speaker labels
]

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_text_from_docx(docx_path: Path) -> str:
    """Extract text from DOCX file."""
    try:
        from docx import Document
        doc = Document(docx_path)
        full_text = []
        for para in doc.paragraphs:
            if para.text.strip():
                full_text.append(para.text)
        return '\n'.join(full_text)
    except ImportError:
        print("Error: python-docx not installed. Install with: pip install python-docx")
        return ""
    except Exception as e:
        print(f"Error reading {docx_path}: {e}")
        return ""

def extract_entities_from_text(text: str, use_spacy: bool = True) -> Dict[str, Set[str]]:
    """Extract all entities from raw text to create ground truth."""
    entities = {
        "persons": set(),
        "organizations": set(),
        "locations": set(),
        "tribes": set(),
        "financial_amounts": set(),
        "years": set()
    }
    
    # Extract person names from speaker labels
    for pattern in PERSON_PATTERNS:
        matches = re.finditer(pattern, text, re.MULTILINE)
        for match in matches:
            name = match.group(1).strip()
            if len(name) > 2 and name.lower() not in EXCLUDED_WORDS.get("persons", set()):
                entities["persons"].add(name)
    
    # Use spaCy for additional entity extraction
    if use_spacy and SPACY_AVAILABLE:
        doc = nlp(text)
        for ent in doc.ents:
            entity_text = ent.text.strip()
            entity_lower = entity_text.lower()
            
            if ent.label_ == "PERSON":
                if (len(entity_text) > 2 and 
                    entity_lower not in EXCLUDED_WORDS.get("persons", set()) and
                    not any(word in entity_lower for word in ["some", "project", "future"])):
                    entities["persons"].add(entity_text)
            elif ent.label_ in ["GPE", "LOC"]:
                if (len(entity_text) > 2 and
                    entity_lower not in EXCLUDED_WORDS.get("locations", set())):
                    entities["locations"].add(entity_text)
            elif ent.label_ == "ORG":
                if (len(entity_text) > 10 and
                    entity_lower not in EXCLUDED_WORDS.get("organizations", set())):
                    entities["organizations"].add(entity_text)
    
    # Extract financial amounts
    financial_matches = re.finditer(r'\$[\d,]+(?:\s*(?:million|thousand|billion))?', text, re.IGNORECASE)
    for match in financial_matches:
        entities["financial_amounts"].add(match.group())
    
    # Extract years (in timeline contexts)
    year_matches = re.finditer(r'\b(19|20)\d{2}\b', text)
    for match in year_matches:
        # Only add if in timeline context
        context_start = max(0, match.start() - 30)
        context_end = min(len(text), match.end() + 30)
        context = text[context_start:context_end].lower()
        if any(word in context for word in ["founded", "established", "since", "year", "started", "began"]):
            entities["years"].add(match.group())
    
    # Extract tribe/nation names
    tribe_matches = re.finditer(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Nation|Tribe|Pueblo)\b', text)
    for match in tribe_matches:
        entities["tribes"].add(match.group(1))
    
    return entities

def check_pii_remaining(raw_entities: Dict[str, Set[str]], cleaned_text: str) -> Dict[str, List[str]]:
    """Check which PII items remain in cleaned text."""
    remaining = {
        "persons": [],
        "organizations": [],
        "locations": [],
        "tribes": [],
        "financial_amounts": [],
        "years": []
    }
    
    cleaned_lower = cleaned_text.lower()
    
    for entity_type, entity_set in raw_entities.items():
        for entity in entity_set:
            entity_lower = entity.lower()
            # Check if entity appears in cleaned text (case-insensitive)
            # But exclude if it's part of a code (Person_X, Location_X, etc.)
            pattern = r'\b' + re.escape(entity_lower) + r'\b'
            if re.search(pattern, cleaned_lower):
                # Verify it's not part of a code
                entity_pos = cleaned_lower.find(entity_lower)
                if entity_pos >= 0:
                    # Check context around the match
                    context_start = max(0, entity_pos - 20)
                    context_end = min(len(cleaned_text), entity_pos + len(entity) + 20)
                    context = cleaned_text[context_start:context_end]
                    
                    # Skip if it's part of a code pattern
                    if not re.search(r'(Person|Location|Organization|Tribe)_\d+', context, re.IGNORECASE):
                        remaining[entity_type].append(entity)
    
    return remaining

def check_citation_system(cleaned_text: str) -> Dict[str, any]:
    """Check if citation system is properly implemented."""
    results = {
        "has_speaker_letters": False,
        "has_verse_numbers": False,
        "has_page_numbers": False,
        "has_timestamp_table": False,
        "speaker_letter_count": 0,
        "verse_count": 0,
        "page_count": 0,
        "issues": []
    }
    
    # Check for speaker letters (A, B, C...)
    speaker_verse_pattern = r'\[([A-Z])\.(\d+)\]'
    matches = re.findall(speaker_verse_pattern, cleaned_text)
    if matches:
        results["has_speaker_letters"] = True
        results["has_verse_numbers"] = True
        results["speaker_letter_count"] = len(set(m[0] for m in matches))
        results["verse_count"] = len(matches)
    else:
        results["issues"].append("No speaker letters or verse numbers found")
    
    # Check for page numbers
    page_pattern = r'Page\s+(\d+)'
    page_matches = re.findall(page_pattern, cleaned_text, re.IGNORECASE)
    if page_matches:
        results["has_page_numbers"] = True
        results["page_count"] = len(set(page_matches))
    else:
        results["issues"].append("No page numbers found")
    
    # Check for timestamp table
    if "CITATION REFERENCE TABLE" in cleaned_text or "timestamp" in cleaned_text.lower():
        results["has_timestamp_table"] = True
    else:
        results["issues"].append("No timestamp table found")
    
    return results

def check_formatting_quality(raw_text: str, cleaned_text: str) -> Dict[str, any]:
    """Check formatting quality of cleaned text."""
    results = {
        "timestamps_removed": True,
        "webvtt_removed": True,
        "has_dialogue_format": False,
        "has_speaker_labels": False,
        "formatting_artifacts": [],
        "issues": []
    }
    
    # Check if timestamps removed
    timestamp_pattern = r'\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}'
    if re.search(timestamp_pattern, cleaned_text):
        results["timestamps_removed"] = False
        results["issues"].append("Timestamps still present in cleaned text")
    
    # Check if WEBVTT removed
    if "WEBVTT" in cleaned_text:
        results["webvtt_removed"] = False
        results["issues"].append("WEBVTT header still present")
    
    # Check for dialogue format
    if re.search(r'\[[A-Z]\.\d+\]', cleaned_text) or re.search(r'(Interviewer|Interviewee):', cleaned_text):
        results["has_dialogue_format"] = True
        results["has_speaker_labels"] = True
    else:
        results["issues"].append("No clear dialogue format or speaker labels")
    
    # Check for formatting artifacts
    if re.search(r'Person_\d+\.?\s*$', cleaned_text, re.MULTILINE):
        results["formatting_artifacts"].append("Standalone Person_X codes found")
    
    if re.search(r'\n{4,}', cleaned_text):
        results["formatting_artifacts"].append("Excessive blank lines")
    
    return results

def check_tagging_quality(cleaned_text: str, tags_file: Optional[Path] = None) -> Dict[str, any]:
    """Check keyword tagging quality."""
    results = {
        "has_tags": False,
        "tag_count": 0,
        "category_count": 0,
        "false_positives": [],
        "coverage_estimate": 0.0
    }
    
    if tags_file and tags_file.exists():
        try:
            import csv
            with open(tags_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                tags = list(reader)
                results["has_tags"] = True
                results["tag_count"] = len(tags)
                
                # Count categories
                categories = set(row['Tag_Category'] for row in tags)
                results["category_count"] = len(categories)
                
                # Estimate coverage (rough heuristic)
                # Count unique line numbers
                unique_lines = set(int(row['Line_Number']) for row in tags if row['Line_Number'].isdigit())
                total_lines = len(cleaned_text.split('\n'))
                if total_lines > 0:
                    results["coverage_estimate"] = len(unique_lines) / total_lines
        except Exception as e:
            results["false_positives"].append(f"Error reading tags file: {e}")
    
    return results

def check_name_matching_accuracy(mapping_file: Optional[Path] = None) -> Dict[str, any]:
    """Check if name matching is accurate."""
    results = {
        "has_mapping": False,
        "person_count": 0,
        "name_variants": {},
        "issues": []
    }
    
    if mapping_file and mapping_file.exists():
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
                results["has_mapping"] = True
                results["person_count"] = len(set(mapping_data.get("persons", {}).values()))
                results["name_variants"] = mapping_data.get("name_variants", {})
        except Exception as e:
            results["issues"].append(f"Error reading mapping file: {e}")
    
    return results

# ============================================================================
# GRADING FUNCTIONS
# ============================================================================

def grade_deidentification_completeness(remaining_pii: Dict[str, List[str]]) -> Tuple[str, float, List[str]]:
    """Grade de-identification completeness (25 points)."""
    total_pii = sum(len(items) for items in remaining_pii.values())
    
    if total_pii == 0:
        grade = "A"
        score = 25.0
        suggestions = ["Perfect de-identification - no PII remaining"]
    elif total_pii <= 2:
        grade = "A"
        score = 23.0
        suggestions = [f"Excellent: Only {total_pii} PII items remaining"]
    elif total_pii <= 5:
        grade = "B"
        score = 20.0
        suggestions = [f"Good: {total_pii} PII items remaining"]
    elif total_pii <= 10:
        grade = "C"
        score = 17.0
        suggestions = [f"Acceptable: {total_pii} PII items remaining"]
    elif total_pii <= 15:
        grade = "D"
        score = 14.0
        suggestions = [f"Needs improvement: {total_pii} PII items remaining"]
    else:
        grade = "F"
        score = max(0, 25 - total_pii)
        suggestions = [f"Critical: {total_pii} PII items remaining"]
    
    # Add specific suggestions
    for entity_type, items in remaining_pii.items():
        if items:
            suggestions.append(f"  - {len(items)} {entity_type} not de-identified: {', '.join(items[:5])}")
    
    return grade, score, suggestions

def grade_deidentification_accuracy(remaining_pii: Dict[str, List[str]], total_entities: int) -> Tuple[str, float, List[str]]:
    """Grade de-identification accuracy (20 points)."""
    if total_entities == 0:
        return "N/A", 20.0, ["No entities found to evaluate"]
    
    error_rate = sum(len(items) for items in remaining_pii.values()) / total_entities
    
    if error_rate < 0.05:
        grade = "A"
        score = 18.0
    elif error_rate < 0.10:
        grade = "B"
        score = 16.0
    elif error_rate < 0.15:
        grade = "C"
        score = 14.0
    elif error_rate < 0.20:
        grade = "D"
        score = 12.0
    else:
        grade = "F"
        score = max(0, 20 - (error_rate * 100))
    
    suggestions = [f"Error rate: {error_rate:.1%}"]
    return grade, score, suggestions

def grade_formatting_quality(formatting_results: Dict[str, any]) -> Tuple[str, float, List[str]]:
    """Grade formatting quality (15 points)."""
    score = 15.0
    issues = []
    
    if not formatting_results["timestamps_removed"]:
        score -= 3
        issues.append("Timestamps not removed")
    
    if not formatting_results["webvtt_removed"]:
        score -= 2
        issues.append("WEBVTT not removed")
    
    if not formatting_results["has_dialogue_format"]:
        score -= 3
        issues.append("No dialogue format")
    
    if formatting_results["formatting_artifacts"]:
        score -= len(formatting_results["formatting_artifacts"]) * 1
        issues.extend(formatting_results["formatting_artifacts"])
    
    if score >= 14:
        grade = "A"
    elif score >= 12:
        grade = "B"
    elif score >= 10:
        grade = "C"
    elif score >= 8:
        grade = "D"
    else:
        grade = "F"
    
    suggestions = formatting_results.get("issues", []) + issues
    if not suggestions:
        suggestions = ["Excellent formatting quality"]
    
    return grade, max(0, score), suggestions

def grade_citation_system(citation_results: Dict[str, any]) -> Tuple[str, float, List[str]]:
    """Grade citation system (15 points)."""
    score = 15.0
    issues = []
    
    if not citation_results["has_speaker_letters"]:
        score -= 5
        issues.append("Speaker letters missing")
    
    if not citation_results["has_verse_numbers"]:
        score -= 5
        issues.append("Verse numbers missing")
    
    if not citation_results["has_page_numbers"]:
        score -= 3
        issues.append("Page numbers missing")
    
    if not citation_results["has_timestamp_table"]:
        score -= 2
        issues.append("Timestamp table missing")
    
    if citation_results["issues"]:
        issues.extend(citation_results["issues"])
        score -= len(citation_results["issues"]) * 1
    
    if score >= 14:
        grade = "A"
    elif score >= 12:
        grade = "B"
    elif score >= 10:
        grade = "C"
    elif score >= 8:
        grade = "D"
    else:
        grade = "F"
    
    suggestions = issues if issues else ["Complete citation system"]
    if citation_results["speaker_letter_count"] > 0:
        suggestions.append(f"Found {citation_results['speaker_letter_count']} speakers, {citation_results['verse_count']} verses")
    
    return grade, max(0, score), suggestions

def grade_tagging_completeness(tagging_results: Dict[str, any]) -> Tuple[str, float, List[str]]:
    """Grade keyword tagging completeness (10 points)."""
    if not tagging_results["has_tags"]:
        return "F", 0.0, ["No tags file found"]
    
    coverage = tagging_results["coverage_estimate"]
    
    if coverage >= 0.90:
        grade = "A"
        score = 9.0
    elif coverage >= 0.80:
        grade = "B"
        score = 8.0
    elif coverage >= 0.70:
        grade = "C"
        score = 7.0
    elif coverage >= 0.60:
        grade = "D"
        score = 6.0
    else:
        grade = "F"
        score = max(0, coverage * 10)
    
    suggestions = [
        f"Tag coverage: {coverage:.1%}",
        f"Total tags: {tagging_results['tag_count']}",
        f"Categories: {tagging_results['category_count']}"
    ]
    
    return grade, score, suggestions

def grade_tagging_precision(tagging_results: Dict[str, any]) -> Tuple[str, float, List[str]]:
    """Grade keyword tagging precision (10 points)."""
    # This is a simplified version - would need more analysis for true precision
    # For now, check for obvious false positives
    
    false_positive_count = len(tagging_results.get("false_positives", []))
    
    if false_positive_count == 0:
        grade = "A"
        score = 9.0
    elif false_positive_count <= 2:
        grade = "B"
        score = 8.0
    elif false_positive_count <= 5:
        grade = "C"
        score = 7.0
    elif false_positive_count <= 10:
        grade = "D"
        score = 6.0
    else:
        grade = "F"
        score = max(0, 10 - false_positive_count)
    
    suggestions = tagging_results.get("false_positives", [])
    if not suggestions:
        suggestions = ["No obvious false positives detected"]
    
    return grade, score, suggestions

def grade_spelling_matching(matching_results: Dict[str, any]) -> Tuple[str, float, List[str]]:
    """Grade spelling and name matching (5 points)."""
    if not matching_results["has_mapping"]:
        return "F", 0.0, ["No mapping file found"]
    
    # Check for name variants (good sign of matching working)
    variant_count = sum(len(variants) for variants in matching_results.get("name_variants", {}).values())
    
    if variant_count > 5:
        grade = "A"
        score = 5.0
    elif variant_count > 3:
        grade = "B"
        score = 4.0
    elif variant_count > 1:
        grade = "C"
        score = 3.0
    elif variant_count > 0:
        grade = "D"
        score = 2.0
    else:
        grade = "F"
        score = 1.0
    
    suggestions = [
        f"Name variants detected: {variant_count}",
        f"Unique persons: {matching_results['person_count']}"
    ]
    
    if matching_results.get("issues"):
        suggestions.extend(matching_results["issues"])
    
    return grade, score, suggestions

# ============================================================================
# MAIN GRADING FUNCTION
# ============================================================================

def grade_transcript_cleaning(
    raw_transcript_path: Path,
    cleaned_transcript_path: Path,
    mapping_file_path: Optional[Path] = None,
    tags_file_path: Optional[Path] = None
) -> Dict[str, any]:
    """
    Grade a cleaned transcript against its raw version.
    
    Returns comprehensive grading report.
    """
    print(f"\nGrading: {cleaned_transcript_path.name}")
    
    # Read raw transcript
    if raw_transcript_path.suffix == '.docx':
        raw_text = extract_text_from_docx(raw_transcript_path)
    else:
        with open(raw_transcript_path, 'r', encoding='utf-8') as f:
            raw_text = f.read()
    
    # Read cleaned transcript
    with open(cleaned_transcript_path, 'r', encoding='utf-8') as f:
        cleaned_text = f.read()
    
    # Extract entities from raw text (ground truth)
    print("  → Extracting entities from raw transcript...")
    raw_entities = extract_entities_from_text(raw_text, use_spacy=SPACY_AVAILABLE)
    total_entities = sum(len(entities) for entities in raw_entities.values())
    print(f"    Found {len(raw_entities['persons'])} persons, {len(raw_entities['organizations'])} orgs, "
          f"{len(raw_entities['locations'])} locations, {len(raw_entities['tribes'])} tribes")
    
    # Check for remaining PII
    print("  → Checking for remaining PII...")
    remaining_pii = check_pii_remaining(raw_entities, cleaned_text)
    remaining_count = sum(len(items) for items in remaining_pii.values())
    print(f"    Found {remaining_count} PII items still present")
    
    # Check formatting
    print("  → Checking formatting quality...")
    formatting_results = check_formatting_quality(raw_text, cleaned_text)
    
    # Check citation system
    print("  → Checking citation system...")
    citation_results = check_citation_system(cleaned_text)
    
    # Check tagging
    print("  → Checking tagging quality...")
    tagging_results = check_tagging_quality(cleaned_text, tags_file_path)
    
    # Check name matching
    print("  → Checking name matching...")
    matching_results = check_name_matching_accuracy(mapping_file_path)
    
    # Grade each category
    grade1, score1, sugg1 = grade_deidentification_completeness(remaining_pii)
    grade2, score2, sugg2 = grade_deidentification_accuracy(remaining_pii, total_entities)
    grade3, score3, sugg3 = grade_formatting_quality(formatting_results)
    grade4, score4, sugg4 = grade_citation_system(citation_results)
    grade5, score5, sugg5 = grade_tagging_completeness(tagging_results)
    grade6, score6, sugg6 = grade_tagging_precision(tagging_results)
    grade7, score7, sugg7 = grade_spelling_matching(matching_results)
    
    total_score = score1 + score2 + score3 + score4 + score5 + score6 + score7
    
    # Determine overall grade
    if total_score >= 93:
        overall_grade = "A"
    elif total_score >= 90:
        overall_grade = "A-"
    elif total_score >= 87:
        overall_grade = "B+"
    elif total_score >= 83:
        overall_grade = "B"
    elif total_score >= 80:
        overall_grade = "B-"
    elif total_score >= 77:
        overall_grade = "C+"
    elif total_score >= 73:
        overall_grade = "C"
    elif total_score >= 70:
        overall_grade = "C-"
    elif total_score >= 67:
        overall_grade = "D+"
    elif total_score >= 60:
        overall_grade = "D"
    else:
        overall_grade = "F"
    
    # Compile report
    report = {
        "transcript_name": cleaned_transcript_path.name,
        "date": datetime.now().isoformat(),
        "overall_grade": overall_grade,
        "overall_score": total_score,
        "categories": {
            "deidentification_completeness": {
                "grade": grade1,
                "score": score1,
                "max_score": 25,
                "suggestions": sugg1
            },
            "deidentification_accuracy": {
                "grade": grade2,
                "score": score2,
                "max_score": 20,
                "suggestions": sugg2
            },
            "formatting_quality": {
                "grade": grade3,
                "score": score3,
                "max_score": 15,
                "suggestions": sugg3
            },
            "citation_system": {
                "grade": grade4,
                "score": score4,
                "max_score": 15,
                "suggestions": sugg4
            },
            "tagging_completeness": {
                "grade": grade5,
                "score": score5,
                "max_score": 10,
                "suggestions": sugg5
            },
            "tagging_precision": {
                "grade": grade6,
                "score": score6,
                "max_score": 10,
                "suggestions": sugg6
            },
            "spelling_matching": {
                "grade": grade7,
                "score": score7,
                "max_score": 5,
                "suggestions": sugg7
            }
        },
        "detailed_findings": {
            "remaining_pii": remaining_pii,
            "formatting_issues": formatting_results.get("issues", []),
            "citation_issues": citation_results.get("issues", []),
            "tagging_stats": {
                "tag_count": tagging_results.get("tag_count", 0),
                "category_count": tagging_results.get("category_count", 0),
                "coverage": tagging_results.get("coverage_estimate", 0.0)
            }
        }
    }
    
    return report

def print_grading_report(report: Dict[str, any]):
    """Print a formatted grading report."""
    print("\n" + "=" * 80)
    print("GRADING REPORT")
    print("=" * 80)
    print(f"\nTranscript: {report['transcript_name']}")
    print(f"Date: {report['date']}")
    print(f"\nOverall Grade: {report['overall_grade']} ({report['overall_score']:.1f}/100)")
    print("\n" + "-" * 80)
    print("CATEGORY GRADES:")
    print("-" * 80)
    
    categories = [
        ("1. De-identification Completeness", "deidentification_completeness", 25),
        ("2. De-identification Accuracy", "deidentification_accuracy", 20),
        ("3. Formatting Quality", "formatting_quality", 15),
        ("4. Citation System", "citation_system", 15),
        ("5. Keyword Tagging Completeness", "tagging_completeness", 10),
        ("6. Keyword Tagging Precision", "tagging_precision", 10),
        ("7. Spelling and Name Matching", "spelling_matching", 5),
    ]
    
    for label, key, max_score in categories:
        cat = report["categories"][key]
        print(f"{label:40s} {cat['grade']:3s} ({cat['score']:.1f}/{max_score})")
    
    print("\n" + "-" * 80)
    print("SUGGESTIONS FOR IMPROVEMENT:")
    print("-" * 80)
    
    for label, key, _ in categories:
        cat = report["categories"][key]
        if cat["grade"] != "A":
            print(f"\n{label}:")
            for sugg in cat["suggestions"][:5]:  # Limit to 5 suggestions per category
                print(f"  • {sugg}")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Grade transcript cleaning quality',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'raw_transcript',
        type=str,
        help='Path to raw transcript file'
    )
    parser.add_argument(
        'cleaned_transcript',
        type=str,
        help='Path to cleaned transcript file'
    )
    parser.add_argument(
        '-m', '--mapping',
        type=str,
        help='Path to mapping file (optional)'
    )
    parser.add_argument(
        '-t', '--tags',
        type=str,
        help='Path to tags file (optional)'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output file for JSON report (optional)'
    )
    
    args = parser.parse_args()
    
    raw_path = Path(args.raw_transcript)
    cleaned_path = Path(args.cleaned_transcript)
    mapping_path = Path(args.mapping) if args.mapping else None
    tags_path = Path(args.tags) if args.tags else None
    
    if not raw_path.exists():
        print(f"Error: Raw transcript not found: {raw_path}")
        return
    
    if not cleaned_path.exists():
        print(f"Error: Cleaned transcript not found: {cleaned_path}")
        return
    
    print("=" * 80)
    print("TRANSCRIPT CLEANING GRADING SYSTEM v1.0.0")
    print("=" * 80)
    
    # Grade the transcript
    report = grade_transcript_cleaning(raw_path, cleaned_path, mapping_path, tags_path)
    
    # Print report
    print_grading_report(report)
    
    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Report saved to: {output_path}")

if __name__ == "__main__":
    main()

