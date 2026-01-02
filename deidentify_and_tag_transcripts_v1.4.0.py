#!/usr/bin/env python3
"""
De-identify and Tag Transcripts for Research v1.4.0

IMPROVEMENTS OVER v1.3.0:
- Citation system with speaker letters (A, B, C...), verse numbers (A.1, A.2...)
- Page numbering for easy reference
- Timestamp conversion table at end (speaker.verse → timestamp)
- Preserves timestamps for citation while maintaining de-identification

IMPROVEMENTS OVER v1.2.0:
- Integrated spaCy NER for better entity extraction
- Hybrid approach: regex patterns (speaker labels) + spaCy (dialogue)
- Better handling of severe misspellings using context
- Finds names in dialogue (not just speaker labels)
- Format-independent name recognition
- Improved variant grouping with spaCy context

IMPROVEMENTS OVER v1.1.0:
- Much more restrictive entity extraction (only actual proper nouns)
- Validates names/locations before extraction
- Filters out common phrases and words aggressively
- Better matches reference cleaned version format

IMPROVEMENTS OVER v1.0.0:
- Fixed over-extraction of entities (reduced false positives)
- Removes timestamps from WEBVTT format
- Corrects common misspellings (UPIC → Yupik, etc.)
- Improved entity extraction patterns
- Better formatting output
- Filters out common words that aren't entities

This program automatically:
1. De-identifies transcripts by replacing names, locations, and sensitive data with codes
2. Handles misspellings and name variants using fuzzy matching + spaCy context
3. Tags research-relevant keywords and concepts
4. Extracts quantitative metrics
5. Generates mapping files for traceability
6. Formats with citation system: speaker letters, verse numbers, page numbers
7. Creates timestamp conversion table for precise citation

REQUIREMENTS:
- python-docx (for DOCX files)
- spacy (optional but recommended): pip install spacy && python -m spacy download en_core_web_md

Version: 1.4.0
"""

import re
import json
import csv
from pathlib import Path
from collections import defaultdict, Counter
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Set, Optional
import sys

# Try to import spaCy (optional but recommended)
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False
    spacy = None

# ============================================================================
# CONFIGURATION
# ============================================================================

# Categories from existing analysis framework
RESEARCH_CATEGORIES = {
    "Membership": ["member", "membership", "constituent", "participant"],
    "Governance": ["board", "director", "leadership", "governance", "bylaw", "steering committee"],
    "Finance": ["revenue", "budget", "financial", "grant", "funding", "loan", "credit", "dollar", "$"],
    "Employment": ["employee", "staff", "worker", "job", "contractor", "consultant", "advisor"],
    "Partnerships": ["partner", "collaboration", "alliance", "network", "support organization"],
    "Innovation": ["innovation", "new practice", "new model", "new product", "development", "pilot"],
    "Operations": ["operation", "supply chain", "processing", "warehouse", "equipment", "production"],
    "Markets": ["market", "sales", "customer", "revenue stream", "distribution", "retail"],
    "Technology": ["digital", "website", "social media", "online", "technology", "software", "app"],
    "Culture": ["traditional", "tribal value", "cultural", "indigenous", "heritage", "elder", "ceremony"],
    "Geography": ["location", "reservation", "tribal land", "community", "region", "nation", "pueblo"],
    "Risk": ["challenge", "obstacle", "risk", "barrier", "issue", "problem", "adaptation"],
    "Timeline": ["founded", "established", "year", "since", "started", "began", "created"],
    "Success": ["success", "growth", "profit", "sustainable", "impact", "benefit", "achievement"],
    "COVID": ["covid", "pandemic", "coronavirus", "lockdown", "quarantine", "remote work", "virtual"]
}

# Survey questions alignment tags
SURVEY_QUESTION_TAGS = {
    "Q1_TribalValues": ["tribal value", "traditional system", "cultural", "indigenous value", "heritage"],
    "Q2_MarketingPlan": ["marketing plan", "business plan", "marketing strategy", "sales plan"],
    "Q3_WebsiteSocial": ["website", "social media", "facebook", "instagram", "online", "digital marketing"],
    "Q4_OutsideAssistance": ["consultant", "developer", "assistance", "help", "support organization", "partner"],
    "Q5_StandardApproaches": ["cooperative model", "coop development", "standard", "best practice", "bylaw"],
    "Q6_CommunityDifferences": ["challenge", "difficult", "conflict", "issue", "problem", "disagree", "barrier"],
    "Q7_LeadershipEngagement": ["tribal leader", "council", "chief", "board", "engage", "communicate", "meeting"],
    "Q8_Success": ["success", "grow", "profit", "achieve", "accomplish", "positive", "benefit", "impact"],
    "Q9_COVID": ["covid", "pandemic", "coronavirus", "lockdown", "quarantine", "remote work", "virtual"]
}

# Indigenous-specific terminology
INDIGENOUS_TERMS = [
    "sovereignty", "tribal sovereignty", "self-determination", "matriarch", "elder", "ceremony",
    "traditional knowledge", "land-based", "water rights", "treaty", "reservation", "pueblo",
    "nation", "tribal council", "indigenous", "native", "first nation", "aboriginal"
]

# Common transcription errors and patterns
COMMON_MISSPELLINGS = {
    "burshia": ["brache", "berchet", "berchet-gowazi", "brochure", "burche"],
    "jodi": ["jody"],
    "pamela": ["pam"],
    "standing": ["stand"]
}

# Common misspellings to correct
MISSPELLING_CORRECTIONS = {
    "upic": "Yupik",
    "yupic": "Yupik",
    "UPIC": "Yupik",
    "YUPIC": "Yupik"
}

# Words to exclude from entity extraction (common words that cause false positives)
EXCLUDED_WORDS = {
    "persons": {"some", "project", "the future", "not able to answer them immediately", 
              "that you", "like you", "sure that", "looking at my", "asking if"},
    "locations": {"some", "project", "the future", "information", "things", "stuff", 
                  "question", "type", "them", "need", "makes", "on the", "where the",
                  "are here", "is because", "is included", "were happening"},
    "organizations": {"the cooperative", "a cooperative", "this co-op", "the co-op",
                     "your cooperative", "our cooperative", "their co-op"}
}

# Fuzzy matching threshold (lowered for severe misspellings when using spaCy)
SIMILARITY_THRESHOLD = 0.75
SIMILARITY_THRESHOLD_LOW = 0.60  # For severe misspellings with context clues

# Citation system configuration
LINES_PER_PAGE = 50  # Number of lines per page for pagination

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def similarity(a: str, b: str) -> float:
    """Calculate similarity between two strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_similar_names(name: str, name_list: List[str], threshold: float = SIMILARITY_THRESHOLD) -> List[str]:
    """Find names similar to the given name."""
    similar = []
    for other_name in name_list:
        if similarity(name, other_name) >= threshold:
            similar.append(other_name)
    return similar

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
        sys.exit(1)
    except Exception as e:
        print(f"Error reading {docx_path}: {e}")
        return ""

def remove_webvtt_timestamps(text: str) -> str:
    """Remove WEBVTT format timestamps and clean up format."""
    # Remove WEBVTT header
    text = re.sub(r'^WEBVTT\s*\n', '', text, flags=re.MULTILINE)
    
    # Remove timestamp lines (00:00:01.900 --> 00:00:12.490)
    text = re.sub(r'^\d+\s*\n', '', text, flags=re.MULTILINE)  # Remove segment numbers
    text = re.sub(r'\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}\s*\n', '', text, flags=re.MULTILINE)
    
    # Clean up multiple newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    return text.strip()

def correct_misspellings(text: str) -> str:
    """Correct common misspellings in text."""
    corrected = text
    for wrong, correct in MISSPELLING_CORRECTIONS.items():
        # Use word boundaries for whole word replacement
        pattern = r'\b' + re.escape(wrong) + r'\b'
        corrected = re.sub(pattern, correct, corrected, flags=re.IGNORECASE)
    return corrected

def parse_webvtt_with_timestamps(text: str) -> List[Dict[str, str]]:
    """
    Parse WEBVTT format and extract segments with timestamps.
    Returns list of dicts with 'timestamp' and 'text' keys.
    """
    segments = []
    lines = text.split('\n')
    i = 0
    
    # Skip WEBVTT header
    while i < len(lines) and (lines[i].strip() == 'WEBVTT' or not lines[i].strip()):
        i += 1
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Check for segment number
        if line.isdigit():
            i += 1
            if i >= len(lines):
                break
                
            # Next line should be timestamp
            timestamp_line = lines[i].strip()
            timestamp_match = re.match(r'(\d{2}:\d{2}:\d{2}\.\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}\.\d{3})', timestamp_line)
            
            if timestamp_match:
                start_time = timestamp_match.group(1)
                # Convert to simpler format (HH:MM:SS)
                time_parts = start_time.split('.')
                timestamp = time_parts[0]  # Just HH:MM:SS, drop milliseconds
                
                i += 1
                # Collect text lines until next segment or empty line
                text_lines = []
                while i < len(lines):
                    current_line = lines[i].strip()
                    # Stop if we hit another segment number or empty line followed by number
                    if current_line.isdigit() or (not current_line and i + 1 < len(lines) and lines[i + 1].strip().isdigit()):
                        break
                    # Skip timestamp lines
                    if not re.match(r'\d{2}:\d{2}:\d{2}', current_line) and current_line:
                        text_lines.append(current_line)
                    i += 1
                
                if text_lines:
                    segments.append({
                        'timestamp': timestamp,
                        'text': ' '.join(text_lines)
                    })
                continue
        
        i += 1
    
    return segments

def format_with_citation_system(
    text: str, 
    speaker_mapping: Dict[str, str],
    segments_with_timestamps: Optional[List[Dict[str, str]]] = None
) -> Tuple[str, Dict[str, Dict[str, str]]]:
    """
    Format text with citation system: speaker letters, verse numbers, page numbers.
    Returns: (formatted_text, timestamp_table)
    timestamp_table format: {speaker_verse: {'timestamp': '...', 'speaker_role': '...'}}
    """
    lines = text.split('\n')
    formatted_lines = []
    timestamp_table = {}
    
    # Assign speaker letters
    speaker_letters = {}  # Maps Person_X code to letter (A, B, C...)
    speaker_verse_counts = {}  # Tracks verse number per speaker
    current_letter = ord('A')
    
    # Track current page and line count
    current_page = 1
    line_count = 0
    
    # Process segments if timestamps available
    segment_idx = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Skip lines that are just "Person_X." (standalone person codes without dialogue)
        if re.match(r'^Person_\d+\.?\s*$', line):
            continue
        
        # Check if line starts with a person code (speaker)
        speaker_match = re.match(r'^(Person_\d+):\s*(.*)$', line)
        if speaker_match:
            speaker_code = speaker_match.group(1)
            dialogue = speaker_match.group(2)
            
            # Assign speaker letter if not already assigned
            if speaker_code not in speaker_letters:
                speaker_letters[speaker_code] = chr(current_letter)
                speaker_verse_counts[speaker_code] = 0
                current_letter += 1
            
            speaker_letter = speaker_letters[speaker_code]
            speaker_verse_counts[speaker_code] += 1
            verse_num = speaker_verse_counts[speaker_code]
            speaker_verse = f"{speaker_letter}.{verse_num}"
            
            # Get speaker role
            if speaker_code in speaker_mapping:
                speaker_label = speaker_mapping[speaker_code]
            else:
                speaker_label = "Interviewer" if "Person_1" in speaker_code or "Person_3" in speaker_code else "Interviewee"
            
            # Get timestamp if available (match by order/position since text is de-identified)
            timestamp = None
            if segments_with_timestamps and segment_idx < len(segments_with_timestamps):
                # Match by position/order (since original text is de-identified)
                timestamp = segments_with_timestamps[segment_idx]['timestamp']
                segment_idx += 1
            
            # Add to timestamp table
            timestamp_table[speaker_verse] = {
                'timestamp': timestamp or 'N/A',
                'speaker_role': speaker_label
            }
            
            # Check if we need a new page
            if line_count > 0 and line_count % LINES_PER_PAGE == 0:
                current_page += 1
                formatted_lines.append(f"\nPage {current_page}\n")
            
            # Format with citation: [A.2] Speaker: text
            formatted_lines.append(f"[{speaker_verse}] {speaker_label}: {dialogue}")
            line_count += 1
        else:
            # Keep non-dialogue lines as-is (but clean)
            if line and not re.match(r'^\d+$', line):  # Skip segment numbers
                # Check if we need a new page
                if line_count > 0 and line_count % LINES_PER_PAGE == 0:
                    current_page += 1
                    formatted_lines.append(f"\nPage {current_page}\n")
                
                formatted_lines.append(line)
                line_count += 1
    
    # Add page number to first line if not already there
    if formatted_lines and not formatted_lines[0].startswith("Page"):
        formatted_lines.insert(0, "Page 1\n")
    
    # Create timestamp conversion table as text
    formatted_text = '\n'.join(formatted_lines)
    
    # Add timestamp table at end
    formatted_text += "\n\n" + "=" * 80 + "\n"
    formatted_text += "CITATION REFERENCE TABLE\n"
    formatted_text += "=" * 80 + "\n\n"
    formatted_text += f"{'Speaker.Verse':<15} {'Timestamp':<12} {'Speaker Role':<20}\n"
    formatted_text += "-" * 80 + "\n"
    
    for speaker_verse in sorted(timestamp_table.keys(), key=lambda x: (x[0], int(x.split('.')[1]))):
        entry = timestamp_table[speaker_verse]
        formatted_text += f"{speaker_verse:<15} {entry['timestamp']:<12} {entry['speaker_role']:<20}\n"
    
    return formatted_text, timestamp_table

def format_as_dialogue(text: str, speaker_mapping: Dict[str, str]) -> str:
    """Format text as clean dialogue, replacing speaker codes with role labels."""
    lines = text.split('\n')
    formatted_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check if line starts with a person code (speaker)
        speaker_match = re.match(r'^(Person_\d+):\s*(.*)$', line)
        if speaker_match:
            speaker_code = speaker_match.group(1)
            dialogue = speaker_match.group(2)
            
            # Try to determine if this is interviewer or interviewee based on context
            # For now, use generic labels
            if speaker_code in speaker_mapping:
                speaker_label = speaker_mapping[speaker_code]
            else:
                # Default: first person is usually interviewer
                speaker_label = "Interviewer" if "Person_1" in speaker_code or "Person_3" in speaker_code else "Interviewee"
            
            formatted_lines.append(f"{speaker_label}: {dialogue}")
        else:
            # Keep non-dialogue lines as-is (but clean)
            if line and not re.match(r'^\d+$', line):  # Skip segment numbers
                formatted_lines.append(line)
    
    return '\n'.join(formatted_lines)

# ============================================================================
# NAME VARIANT DETECTION
# ============================================================================

class NameVariantDetector:
    """Detects and clusters name variants accounting for misspellings."""
    
    def __init__(self):
        self.name_clusters = defaultdict(list)
        self.canonical_names = {}
        self.name_counter = Counter()
        self.name_contexts = defaultdict(list)  # Store context for each name
        
    def add_name(self, name: str, context: str = ""):
        """Add a name and try to match it to existing clusters."""
        if not name or len(name.strip()) < 2:
            return
        
        # Filter out excluded words
        name_lower = name.strip().lower()
        if name_lower in EXCLUDED_WORDS.get("persons", set()):
            return
            
        name_clean = name.strip()
        self.name_counter[name_clean] += 1
        if context:
            self.name_contexts[name_clean].append(context)
        
        # Check against known misspellings
        for canonical, variants in COMMON_MISSPELLINGS.items():
            if name_lower in variants or canonical in name_lower:
                canonical_key = f"CANONICAL_{canonical}"
                if canonical_key not in self.name_clusters:
                    self.name_clusters[canonical_key] = []
                if name_clean not in self.name_clusters[canonical_key]:
                    self.name_clusters[canonical_key].append(name_clean)
                return
        
        # Try fuzzy matching against existing clusters
        matched = False
        for cluster_key, cluster_names in self.name_clusters.items():
            for cluster_name in cluster_names:
                sim = similarity(name_clean, cluster_name)
                # Use lower threshold if we have context clues
                threshold = SIMILARITY_THRESHOLD_LOW if context else SIMILARITY_THRESHOLD
                if sim >= threshold:
                    if name_clean not in cluster_names:
                        self.name_clusters[cluster_key].append(name_clean)
                    matched = True
                    break
            if matched:
                break
        
        # Create new cluster if no match
        if not matched:
            cluster_key = f"CLUSTER_{len(self.name_clusters)}"
            self.name_clusters[cluster_key] = [name_clean]
    
    def get_canonical_mapping(self) -> Dict[str, str]:
        """Get mapping from all variants to canonical name."""
        mapping = {}
        for cluster_key, variants in self.name_clusters.items():
            # Use most common variant as canonical
            variant_counts = [(v, self.name_counter[v]) for v in variants]
            variant_counts.sort(key=lambda x: x[1], reverse=True)
            canonical = variant_counts[0][0] if variant_counts else variants[0]
            
            for variant in variants:
                mapping[variant] = canonical
        
        return mapping

# ============================================================================
# DE-IDENTIFICATION ENGINE
# ============================================================================

class DeIdentifier:
    """Handles de-identification of transcripts."""
    
    def __init__(self, use_spacy: bool = True):
        self.person_counter = 0
        self.org_counter = 0
        self.location_counter = 0
        self.tribe_counter = 0
        self.mapping = {
            "persons": {},
            "organizations": {},
            "locations": {},
            "tribes": {},
            "numbers": {}
        }
        self.name_detector = NameVariantDetector()
        self.speaker_roles = {}  # Track which Person codes are interviewers vs interviewees
        
        # Initialize spaCy if available
        self.nlp = None
        self.use_spacy = False
        if use_spacy and SPACY_AVAILABLE:
            try:
                # Try to load medium model first (better accuracy)
                self.nlp = spacy.load("en_core_web_md")
                self.use_spacy = True
                print("  ✓ spaCy loaded (en_core_web_md)")
            except OSError:
                try:
                    # Fall back to small model
                    self.nlp = spacy.load("en_core_web_sm")
                    self.use_spacy = True
                    print("  ✓ spaCy loaded (en_core_web_sm)")
                except OSError:
                    print("  ⚠ spaCy not available. Install with: pip install spacy && python -m spacy download en_core_web_md")
                    self.use_spacy = False
        elif use_spacy and not SPACY_AVAILABLE:
            print("  ⚠ spaCy not installed. Install with: pip install spacy")
            print("  → Continuing without spaCy (using regex patterns only)")
        
    def extract_entities_with_spacy(self, text: str) -> Dict[str, List[str]]:
        """Extract entities using spaCy NER with improved filtering."""
        if not self.use_spacy or not self.nlp:
            return {"persons": [], "organizations": [], "locations": [], "tribes": []}
        
        entities = {
            "persons": [],
            "organizations": [],
            "locations": [],
            "tribes": []
        }
        
        # Known false positives to exclude
        FALSE_POSITIVE_PERSONS = {
            "facebook messenger", "cooper bay", "st. michael", "st michael",
            "chuck cheese", "hooper bay", "old harbor", "new lotto"
        }
        FALSE_POSITIVE_ORGS = {
            "flagstaff", "kotzebue", "kivalina", "hooper bay", "cooper bay",
            "st. michael", "st michael", "old harbor", "new lotto", "webvtt"
        }
        
        # Process text with spaCy (in chunks if very long)
        max_length = 1000000  # spaCy default
        if len(text) > max_length:
            chunks = [text[i:i+max_length] for i in range(0, len(text), max_length)]
        else:
            chunks = [text]
        
        extracted_persons = set()
        extracted_orgs = set()
        extracted_locs = set()
        
        for chunk in chunks:
            doc = self.nlp(chunk)
            
            for ent in doc.ents:
                ent_text = ent.text.strip()
                ent_lower = ent_text.lower()
                
                # Filter and validate entities
                if ent.label_ == "PERSON":
                    # Exclude known false positives
                    if any(fp in ent_lower for fp in FALSE_POSITIVE_PERSONS):
                        continue
                    
                    # Exclude if contains common non-name words
                    if any(word in ent_lower for word in ["messenger", "bay", "harbor", "cheese"]):
                        continue
                    
                    # Must look like a name (2-4 words, proper capitalization)
                    words = ent_text.split()
                    if len(words) < 2 or len(words) > 4:
                        continue
                    
                    # Basic validation
                    if (len(ent_text) > 3 and 
                        self.is_valid_name(ent_text) and 
                        ent_text not in extracted_persons):
                        entities["persons"].append(ent_text)
                        extracted_persons.add(ent_text)
                        # Get context (surrounding words)
                        start = max(0, ent.start_char - 50)
                        end = min(len(chunk), ent.end_char + 50)
                        context = chunk[start:end]
                        self.name_detector.add_name(ent_text, context)
                
                elif ent.label_ in ["ORG", "ORGANIZATION"]:
                    # Exclude known false positives (locations misidentified as orgs)
                    if any(fp in ent_lower for fp in FALSE_POSITIVE_ORGS):
                        continue
                    
                    # Exclude single-word place names that are likely locations
                    if len(ent_text.split()) == 1 and ent_text[0].isupper():
                        # Could be a location, skip
                        continue
                    
                    org = ent_text
                    if (len(org) > 5 and 
                        ent_lower not in EXCLUDED_WORDS.get("organizations", set()) and
                        org not in extracted_orgs and
                        not any(word in ent_lower for word in ["the cooperative", "a cooperative", "this co-op", "your cooperative"])):
                        entities["organizations"].append(org)
                        extracted_orgs.add(org)
                
                elif ent.label_ in ["GPE", "LOC", "LOCATION"]:  # Geopolitical entity or location
                    # Exclude if it's actually a person name pattern
                    words = ent_text.split()
                    if len(words) == 2 and all(w[0].isupper() for w in words if w):
                        # Could be a person name, be more careful
                        # Check if it's in our known locations list
                        if ent_lower not in ["hooper bay", "old harbor", "new lotto", "cooper bay", "st. michael"]:
                            # Might be a person, skip
                            continue
                    
                    loc = ent_text
                    if (len(loc) > 3 and 
                        self.is_valid_location(loc) and 
                        loc not in extracted_locs):
                        entities["locations"].append(loc)
                        extracted_locs.add(loc)
        
        return entities
    
    def is_valid_name(self, name: str) -> bool:
        """Check if a string is a valid person name (not a phrase)."""
        if not name or len(name.strip()) < 3:
            return False
        
        name_lower = name.strip().lower()
        words = name.split()
        
        # Must be 2-4 words (first name + last name, possibly middle)
        if len(words) < 2 or len(words) > 4:
            return False
        
        # All words must start with capital letter (proper noun)
        # Exception: Allow if spaCy identified it (more lenient)
        if not self.use_spacy:
            if not all(word[0].isupper() for word in words if word):
                return False
        
        # Exclude common phrases
        common_phrases = [
            "not able", "that you", "like you", "sure that", "glad you", "talking",
            "looking at", "asking if", "wondering if", "going to", "gonna",
            "the community", "the cooperative", "the project", "the future",
            "some of", "all of", "part of", "kind of", "sort of", "type of"
        ]
        if any(phrase in name_lower for phrase in common_phrases):
            return False
        
        # Exclude if contains common words
        common_words = {"the", "a", "an", "and", "or", "but", "if", "that", "this", 
                       "some", "all", "any", "each", "every", "other", "another",
                       "project", "future", "information", "things", "stuff"}
        if any(word.lower() in common_words for word in words):
            return False
        
        # Must not be in excluded list
        if name_lower in EXCLUDED_WORDS.get("persons", set()):
            return False
        
        return True
    
    def is_valid_location(self, loc: str) -> bool:
        """Check if a string is a valid location name."""
        if not loc or len(loc) < 3:
            return False
        
        loc_lower = loc.lower()
        words = loc.split()
        
        # Must start with capital letter (unless spaCy identified it)
        if not self.use_spacy:
            if not loc[0].isupper():
                return False
        
        # Exclude common phrases
        common_phrases = [
            "some", "project", "future", "information", "things", "stuff",
            "where the", "are here", "is because", "is included", "were happening",
            "on the", "in the", "at the", "from the", "to the"
        ]
        if any(phrase in loc_lower for phrase in common_phrases):
            return False
        
        # Exclude if contains common words
        common_words = {"some", "project", "future", "information", "things", "stuff",
                        "the", "a", "an", "and", "or", "but", "if", "that", "this"}
        if any(word.lower() in common_words for word in words):
            return False
        
        # Must not be in excluded list
        if loc_lower in EXCLUDED_WORDS.get("locations", set()):
            return False
        
        return True
    
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract entities using hybrid approach: regex patterns + spaCy NER."""
        entities = {
            "persons": [],
            "organizations": [],
            "locations": [],
            "tribes": []
        }
        
        # METHOD 1: Regex patterns for speaker labels (high precision)
        person_patterns = [
            # Pattern: "FirstName LastName:" at start of line (speaker labels only)
            r'^([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:',
        ]
        
        extracted_names = set()
        for pattern in person_patterns:
            matches = re.finditer(pattern, text, re.MULTILINE)
            for match in matches:
                name = match.group(1).strip()
                if self.is_valid_name(name) and name not in extracted_names:
                    entities["persons"].append(name)
                    extracted_names.add(name)
                    self.name_detector.add_name(name, "speaker_label")
        
        # METHOD 2: spaCy NER for names in dialogue (better recall, handles misspellings)
        if self.use_spacy:
            spacy_entities = self.extract_entities_with_spacy(text)
            
            # Merge spaCy results with regex results
            for name in spacy_entities["persons"]:
                if name not in extracted_names:
                    entities["persons"].append(name)
                    extracted_names.add(name)
            
            # Merge organizations
            extracted_orgs = set(entities["organizations"])
            for org in spacy_entities["organizations"]:
                if org not in extracted_orgs:
                    entities["organizations"].append(org)
                    extracted_orgs.add(org)
            
            # Merge locations
            extracted_locs = set(entities["locations"])
            for loc in spacy_entities["locations"]:
                if loc not in extracted_locs:
                    entities["locations"].append(loc)
                    extracted_locs.add(loc)
        
        # METHOD 3: Regex patterns for organizations (if spaCy not available or for specific patterns)
        org_patterns = [
            # Only specific organization types with proper capitalization
            r'\b([A-Z][A-Za-z\s&]{3,}(?:Cooperative|Co-op|Coop|Services|Organization|Corporation|Authority|Agency|Nation|Pueblo))\b',
        ]
        
        extracted_orgs = set(entities["organizations"])
        for pattern in org_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                org = match.group(1).strip()
                org_lower = org.lower()
                
                # Filter out generic phrases and check length
                if (len(org) > 10 and 
                    org_lower not in EXCLUDED_WORDS.get("organizations", set()) and
                    org not in extracted_orgs and
                    not any(word in org_lower for word in ["the cooperative", "a cooperative", "this co-op", "your cooperative"])):
                    entities["organizations"].append(org)
                    extracted_orgs.add(org)
        
        # METHOD 4: Regex patterns for locations (complement to spaCy)
        location_patterns = [
            # "City, State" pattern (e.g., "Phoenix, AZ")
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b',
            # "Reservation/Nation/Pueblo" pattern (e.g., "Tohono O'Odham Reservation")
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Reservation|Nation|Pueblo|Tribe)\b',
            # Known cities/states (Alaska, Arizona, New Mexico, etc.)
            r'\b(Alaska|Arizona|New Mexico|Wisconsin|California|Oregon|Washington|Idaho|Nevada|Texas|Oklahoma)\b',
            # Specific city names mentioned in context (Bethel, Anchorage, etc.)
            r'\b(Bethel|Anchorage|Phoenix|Gilbert|Flagstaff|Juneau|Kotzebue|Kivalina|Yakutat|Minto|Gamble|Hooper Bay|Old Harbor|New Lotto|Emmonak|Stebbins|St\.?\s*Michael|Gwichluk|Cooper Bay)\b',
        ]
        
        extracted_locs = set(entities["locations"])
        for pattern in location_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                # Get the location part (group 1 for most patterns)
                if match.lastindex >= 1:
                    loc = match.group(1).strip()
                else:
                    loc = match.group(0).strip()  # For state names
                
                if loc and self.is_valid_location(loc) and loc not in extracted_locs:
                    entities["locations"].append(loc)
                    extracted_locs.add(loc)
        
        # Extract tribe/nation names - very specific
        tribe_patterns = [
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Nation|Tribe|Pueblo)\b',
        ]
        
        extracted_tribes = set()
        for pattern in tribe_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                tribe = match.group(1).strip()
                if len(tribe) > 3 and tribe not in extracted_tribes and self.is_valid_location(tribe):
                    entities["tribes"].append(tribe)
                    extracted_tribes.add(tribe)
        
        return entities
    
    def create_codes(self, entities: Dict[str, List[str]]):
        """Create de-identification codes for entities."""
        canonical_mapping = self.name_detector.get_canonical_mapping()
        
        # Process persons
        seen_canonicals = set()
        for variant, canonical in canonical_mapping.items():
            if canonical not in seen_canonicals:
                self.person_counter += 1
                code = f"Person_{self.person_counter}"
                self.mapping["persons"][canonical] = code
                seen_canonicals.add(canonical)
            # Map variant to same code as canonical
            if canonical in self.mapping["persons"]:
                self.mapping["persons"][variant] = self.mapping["persons"][canonical]
        
        # Process organizations - filter out generic ones
        for org in set(entities["organizations"]):
            org_lower = org.lower()
            if org_lower not in EXCLUDED_WORDS.get("organizations", set()):
                if org not in self.mapping["organizations"]:
                    self.org_counter += 1
                    self.mapping["organizations"][org] = f"Organization_{self.org_counter}"
        
        # Process locations - filter out common words
        for loc in set(entities["locations"]):
            loc_lower = loc.lower()
            if loc_lower not in EXCLUDED_WORDS.get("locations", set()):
                if loc not in self.mapping["locations"]:
                    self.location_counter += 1
                    self.mapping["locations"][loc] = f"Location_{self.location_counter}"
        
        # Process tribes
        for tribe in set(entities["tribes"]):
            if tribe not in self.mapping["tribes"]:
                self.tribe_counter += 1
                self.mapping["tribes"][tribe] = f"Tribe_{self.tribe_counter}"
    
    def deidentify_text(self, text: str, remove_timestamps: bool = True, format_dialogue: bool = True, 
                       use_citation_system: bool = True, segments_with_timestamps: Optional[List[Dict[str, str]]] = None) -> Tuple[str, Dict[str, Dict[str, str]]]:
        """
        Replace identified entities with codes and clean format.
        Returns: (deidentified_text, timestamp_table)
        """
        deidentified = text
        
        # First, correct misspellings
        deidentified = correct_misspellings(deidentified)
        
        # Remove timestamps if requested (but we'll preserve them for citation system)
        if remove_timestamps and not use_citation_system:
            deidentified = remove_webvtt_timestamps(deidentified)
        elif remove_timestamps and use_citation_system:
            # Still remove WEBVTT formatting but preserve timestamp data
            deidentified = remove_webvtt_timestamps(deidentified)
        
        # Replace persons (longest matches first)
        person_items = sorted(self.mapping["persons"].items(), key=lambda x: len(x[0]), reverse=True)
        for original, code in person_items:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(original) + r'\b'
            deidentified = re.sub(pattern, code, deidentified, flags=re.IGNORECASE)
        
        # Replace organizations
        org_items = sorted(self.mapping["organizations"].items(), key=lambda x: len(x[0]), reverse=True)
        for original, code in org_items:
            pattern = r'\b' + re.escape(original) + r'\b'
            deidentified = re.sub(pattern, code, deidentified, flags=re.IGNORECASE)
        
        # Replace locations
        loc_items = sorted(self.mapping["locations"].items(), key=lambda x: len(x[0]), reverse=True)
        for original, code in loc_items:
            pattern = r'\b' + re.escape(original) + r'\b'
            deidentified = re.sub(pattern, code, deidentified, flags=re.IGNORECASE)
        
        # Replace tribes
        tribe_items = sorted(self.mapping["tribes"].items(), key=lambda x: len(x[0]), reverse=True)
        for original, code in tribe_items:
            pattern = r'\b' + re.escape(original) + r'\b'
            deidentified = re.sub(pattern, code, deidentified, flags=re.IGNORECASE)
        
        # Replace specific dollar amounts with brackets
        deidentified = re.sub(r'\$[\d,]+(?:\s*(?:million|thousand|billion))?', '[Financial_Amount]', deidentified, flags=re.IGNORECASE)
        deidentified = re.sub(r'[\d,]+(?:\s*(?:million|thousand|billion))\s+dollars?', '[Financial_Amount]', deidentified, flags=re.IGNORECASE)
        
        # Replace specific years (but keep relative references)
        deidentified = re.sub(r'\b(19|20)\d{2}\b', '[Year]', deidentified)
        
        # Format with citation system or regular dialogue
        timestamp_table = {}
        if format_dialogue:
            # Create speaker role mapping (first person is usually interviewer)
            speaker_mapping = {}
            person_codes = sorted(set(self.mapping["persons"].values()), key=lambda x: int(x.split('_')[1]))
            for i, code in enumerate(person_codes):
                if i == 0 or i == 2:  # Usually Person_1 and Person_3 are interviewers
                    speaker_mapping[code] = "Interviewer"
                else:
                    speaker_mapping[code] = "Interviewee"
            
            if use_citation_system:
                deidentified, timestamp_table = format_with_citation_system(deidentified, speaker_mapping, segments_with_timestamps)
            else:
                deidentified = format_as_dialogue(deidentified, speaker_mapping)
        
        return deidentified, timestamp_table

# ============================================================================
# KEYWORD TAGGING ENGINE
# ============================================================================

class KeywordTagger:
    """Tags research-relevant keywords in text."""
    
    def __init__(self):
        self.tags = defaultdict(list)
        self.line_tags = defaultdict(list)  # Tags by line number
        
    def tag_text(self, text: str) -> Dict[str, List[Tuple[int, str, str]]]:
        """
        Tag text with research keywords.
        Returns: {tag_category: [(line_num, matched_text, context)]}
        """
        lines = text.split('\n')
        all_tags = defaultdict(list)
        
        # Tag by research category
        for category, keywords in RESEARCH_CATEGORIES.items():
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                for line_num, line in enumerate(lines, 1):
                    matches = re.finditer(pattern, line, re.IGNORECASE)
                    for match in matches:
                        context = line[max(0, match.start()-50):match.end()+50]
                        all_tags[f"CATEGORY_{category}"].append((line_num, match.group(), context))
        
        # Tag by survey question
        for q_tag, keywords in SURVEY_QUESTION_TAGS.items():
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                for line_num, line in enumerate(lines, 1):
                    matches = re.finditer(pattern, line, re.IGNORECASE)
                    for match in matches:
                        context = line[max(0, match.start()-50):match.end()+50]
                        all_tags[f"QUESTION_{q_tag}"].append((line_num, match.group(), context))
        
        # Tag Indigenous-specific terms
        for term in INDIGENOUS_TERMS:
            pattern = r'\b' + re.escape(term) + r'\b'
            for line_num, line in enumerate(lines, 1):
                matches = re.finditer(pattern, line, re.IGNORECASE)
                for match in matches:
                    context = line[max(0, match.start()-50):match.end()+50]
                    all_tags["INDIGENOUS_TERM"].append((line_num, match.group(), context))
        
        # Extract quantitative metrics
        metric_patterns = [
            (r'\b(\d+)\s+members?\b', 'METRIC_Members'),
            (r'\b(\d+)\s+employees?\b', 'METRIC_Employees'),
            (r'\b(\d+)\s+partners?\b', 'METRIC_Partners'),
            (r'\b(\d+)\s+grants?\b', 'METRIC_Grants'),
            (r'\b(\d{4})\b', 'METRIC_Year'),
            (r'\$(\d+(?:,\d+)*(?:\.\d+)?)', 'METRIC_DollarAmount'),
        ]
        
        for pattern, tag in metric_patterns:
            for line_num, line in enumerate(lines, 1):
                matches = re.finditer(pattern, line, re.IGNORECASE)
                for match in matches:
                    context = line[max(0, match.start()-50):match.end()+50]
                    all_tags[tag].append((line_num, match.group(), context))
        
        return all_tags

# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_transcript(input_path: Path, output_dir: Path, use_spacy: bool = True) -> Dict:
    """Process a single transcript file."""
    print(f"\nProcessing: {input_path.name}")
    
    # Read transcript
    if input_path.suffix == '.docx':
        text = extract_text_from_docx(input_path)
    else:
        with open(input_path, 'r', encoding='utf-8') as f:
            text = f.read()
    
    if not text:
        print(f"  ⚠ Warning: Could not extract text from {input_path}")
        return {}
    
    print(f"  ✓ Extracted {len(text)} characters")
    
    # Initialize processors
    deidentifier = DeIdentifier(use_spacy=use_spacy)
    tagger = KeywordTagger()
    
    # Extract timestamps from original text (for citation system)
    segments_with_timestamps = None
    if "WEBVTT" in text:
        segments_with_timestamps = parse_webvtt_with_timestamps(text)
        if segments_with_timestamps:
            print(f"    Extracted {len(segments_with_timestamps)} timestamped segments")
    
    # Extract entities
    print("  → Extracting entities...")
    entities = deidentifier.extract_entities(text)
    print(f"    Found {len(set(entities['persons']))} unique persons, {len(set(entities['organizations']))} orgs, "
          f"{len(set(entities['locations']))} locations, {len(set(entities['tribes']))} tribes")
    if deidentifier.use_spacy:
        print(f"    (Using hybrid approach: regex + spaCy)")
    
    # Create de-identification codes
    deidentifier.create_codes(entities)
    
    # De-identify text (with citation system)
    print("  → De-identifying and formatting text with citation system...")
    deidentified_text, timestamp_table = deidentifier.deidentify_text(
        text, 
        remove_timestamps=True, 
        format_dialogue=True,
        use_citation_system=True,
        segments_with_timestamps=segments_with_timestamps
    )
    
    # Tag keywords (on original text for accuracy)
    print("  → Tagging keywords...")
    tags = tagger.tag_text(text)
    total_tags = sum(len(tag_list) for tag_list in tags.values())
    print(f"    Found {total_tags} keyword matches across {len(tags)} categories")
    
    # Generate output files
    base_name = input_path.stem
    
    # 1. De-identified text (cleaned and formatted)
    deid_path = output_dir / f"{base_name}_deidentified.txt"
    with open(deid_path, 'w', encoding='utf-8') as f:
        f.write(deidentified_text)
    print(f"  ✓ Created: {deid_path.name}")
    
    # 2. Mapping file (JSON)
    mapping_path = output_dir / f"{base_name}_mapping.json"
    mapping_data = {
        "source_file": str(input_path.name),
        "persons": deidentifier.mapping["persons"],
        "organizations": deidentifier.mapping["organizations"],
        "locations": deidentifier.mapping["locations"],
        "tribes": deidentifier.mapping["tribes"],
        "name_variants": dict(deidentifier.name_detector.name_clusters),
        "spacy_used": deidentifier.use_spacy,
        "timestamp_table": timestamp_table
    }
    with open(mapping_path, 'w', encoding='utf-8') as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)
    print(f"  ✓ Created: {mapping_path.name}")
    
    # 3. Tags file (CSV)
    tags_path = output_dir / f"{base_name}_tags.csv"
    with open(tags_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Tag_Category', 'Line_Number', 'Matched_Text', 'Context'])
        for tag_category, tag_list in sorted(tags.items()):
            for line_num, matched, context in tag_list:
                writer.writerow([tag_category, line_num, matched, context])
    print(f"  ✓ Created: {tags_path.name}")
    
    # 4. Summary statistics
    summary = {
        "source_file": str(input_path.name),
        "original_length": len(text),
        "deidentified_length": len(deidentified_text),
        "entities_found": {
            "persons": len(set(deidentifier.mapping["persons"].values())),
            "organizations": len(deidentifier.mapping["organizations"]),
            "locations": len(deidentifier.mapping["locations"]),
            "tribes": len(deidentifier.mapping["tribes"])
        },
        "tags_found": {tag: len(tag_list) for tag, tag_list in tags.items()},
        "total_tags": total_tags,
        "spacy_used": deidentifier.use_spacy
    }
    
    return summary

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    print("=" * 80)
    print("DE-IDENTIFY AND TAG TRANSCRIPTS v1.4.0")
    print("=" * 80)
    
    # Setup paths
    script_dir = Path(__file__).parent
    input_dir = script_dir / "newer transcripts"
    output_dir = script_dir / "deidentified_transcripts"
    output_dir.mkdir(exist_ok=True)
    
    if not input_dir.exists():
        print(f"\n❌ Error: Input directory not found: {input_dir}")
        print("   Please ensure 'newer transcripts' directory exists.")
        sys.exit(1)
    
    # Find transcript files
    transcript_files = []
    for ext in ['.docx', '.txt']:
        transcript_files.extend(input_dir.glob(f"*{ext}"))
    
    if not transcript_files:
        print(f"\n❌ No transcript files found in {input_dir}")
        sys.exit(1)
    
    print(f"\nFound {len(transcript_files)} transcript file(s)")
    print(f"Output directory: {output_dir}")
    
    # Process each transcript
    summaries = []
    for transcript_file in sorted(transcript_files):
        try:
            summary = process_transcript(transcript_file, output_dir, use_spacy=True)
            if summary:
                summaries.append(summary)
        except Exception as e:
            print(f"  ❌ Error processing {transcript_file.name}: {e}")
            import traceback
            traceback.print_exc()
    
    # Generate overall summary
    if summaries:
        summary_path = output_dir / "processing_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summaries, f, indent=2)
        print(f"\n✓ Created summary: {summary_path.name}")
        
        # Print summary statistics
        print("\n" + "=" * 80)
        print("PROCESSING SUMMARY")
        print("=" * 80)
        total_entities = {
            "persons": 0,
            "organizations": 0,
            "locations": 0,
            "tribes": 0
        }
        total_tags = 0
        spacy_used_count = sum(1 for s in summaries if s.get("spacy_used", False))
        
        for summary in summaries:
            print(f"\n{summary['source_file']}:")
            print(f"  Entities: {summary['entities_found']}")
            print(f"  Tags: {summary['total_tags']} total")
            if summary.get("spacy_used"):
                print(f"  spaCy: Used")
            for key in total_entities:
                total_entities[key] += summary['entities_found'][key]
            total_tags += summary['total_tags']
        
        print(f"\nTOTALS:")
        print(f"  Entities found: {total_entities}")
        print(f"  Total tags: {total_tags}")
        if spacy_used_count > 0:
            print(f"  spaCy used: {spacy_used_count}/{len(summaries)} transcripts")
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"\nOutput files saved to: {output_dir}")

if __name__ == "__main__":
    main()

