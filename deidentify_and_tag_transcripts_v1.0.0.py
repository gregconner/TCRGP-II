#!/usr/bin/env python3
"""
De-identify and Tag Transcripts for Research v1.0.0

This program automatically:
1. De-identifies transcripts by replacing names, locations, and sensitive data with codes
2. Handles misspellings and name variants using fuzzy matching
3. Tags research-relevant keywords and concepts
4. Extracts quantitative metrics
5. Generates mapping files for traceability

Version: 1.0.0
"""

import re
import json
import csv
from pathlib import Path
from collections import defaultdict, Counter
from difflib import SequenceMatcher
from typing import Dict, List, Tuple, Set
import sys

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

# Fuzzy matching threshold
SIMILARITY_THRESHOLD = 0.75

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

# ============================================================================
# NAME VARIANT DETECTION
# ============================================================================

class NameVariantDetector:
    """Detects and clusters name variants accounting for misspellings."""
    
    def __init__(self):
        self.name_clusters = defaultdict(list)
        self.canonical_names = {}
        self.name_counter = Counter()
        
    def add_name(self, name: str, context: str = ""):
        """Add a name and try to match it to existing clusters."""
        if not name or len(name.strip()) < 2:
            return
            
        name_clean = name.strip()
        self.name_counter[name_clean] += 1
        
        # Check against known misspellings
        name_lower = name_clean.lower()
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
                if similarity(name_clean, cluster_name) >= SIMILARITY_THRESHOLD:
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
    
    def __init__(self):
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
        
    def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract potential entities from text."""
        entities = {
            "persons": [],
            "organizations": [],
            "locations": [],
            "tribes": []
        }
        
        # Extract potential person names (capitalized words, often in introductions)
        person_patterns = [
            r'(?:^|\.|\n)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*:',
            r'(?:my name is|i\'?m|i am)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            r'([A-Z][a-z]+\s+[A-Z][a-z]+)\s+(?:said|stated|mentioned|explained)',
        ]
        
        for pattern in person_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                name = match.group(1).strip()
                if len(name.split()) >= 2:  # Full names typically have 2+ words
                    entities["persons"].append(name)
                    self.name_detector.add_name(name)
        
        # Extract organization names (often after "of", "at", "with")
        org_patterns = [
            r'(?:of|at|with|from)\s+([A-Z][A-Za-z\s&]+(?:Cooperative|Co-op|Coop|Services|Organization|Nation|Pueblo))',
            r'([A-Z][A-Za-z\s&]+(?:Cooperative|Co-op|Coop|Services|Organization))',
        ]
        
        for pattern in org_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                org = match.group(1).strip()
                if len(org) > 5:  # Filter out short matches
                    entities["organizations"].append(org)
        
        # Extract locations (cities, states, reservations)
        location_patterns = [
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Reservation|Nation|Pueblo|Tribe)',
            r'(?:in|at|from|located in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Reservation',
        ]
        
        for pattern in location_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                loc = match.group(1).strip()
                if len(loc) > 3:
                    entities["locations"].append(loc)
        
        # Extract tribe/nation names
        tribe_patterns = [
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Nation|Tribe|Pueblo)',
            r'(?:from|member of|affiliated with)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Nation|Tribe)',
        ]
        
        for pattern in tribe_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                tribe = match.group(1).strip()
                if len(tribe) > 3:
                    entities["tribes"].append(tribe)
        
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
        
        # Process organizations
        for org in set(entities["organizations"]):
            if org not in self.mapping["organizations"]:
                self.org_counter += 1
                self.mapping["organizations"][org] = f"Organization_{self.org_counter}"
        
        # Process locations
        for loc in set(entities["locations"]):
            if loc not in self.mapping["locations"]:
                self.location_counter += 1
                self.mapping["locations"][loc] = f"Location_{self.location_counter}"
        
        # Process tribes
        for tribe in set(entities["tribes"]):
            if tribe not in self.mapping["tribes"]:
                self.tribe_counter += 1
                self.mapping["tribes"][tribe] = f"Tribe_{self.tribe_counter}"
    
    def deidentify_text(self, text: str) -> str:
        """Replace identified entities with codes."""
        deidentified = text
        
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
        
        return deidentified

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

def process_transcript(input_path: Path, output_dir: Path) -> Dict:
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
    deidentifier = DeIdentifier()
    tagger = KeywordTagger()
    
    # Extract entities
    print("  → Extracting entities...")
    entities = deidentifier.extract_entities(text)
    print(f"    Found {len(entities['persons'])} persons, {len(entities['organizations'])} orgs, "
          f"{len(entities['locations'])} locations, {len(entities['tribes'])} tribes")
    
    # Create de-identification codes
    deidentifier.create_codes(entities)
    
    # De-identify text
    print("  → De-identifying text...")
    deidentified_text = deidentifier.deidentify_text(text)
    
    # Tag keywords
    print("  → Tagging keywords...")
    tags = tagger.tag_text(text)
    total_tags = sum(len(tag_list) for tag_list in tags.values())
    print(f"    Found {total_tags} keyword matches across {len(tags)} categories")
    
    # Generate output files
    base_name = input_path.stem
    
    # 1. De-identified text
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
        "name_variants": dict(deidentifier.name_detector.name_clusters)
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
        "total_tags": total_tags
    }
    
    return summary

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    print("=" * 80)
    print("DE-IDENTIFY AND TAG TRANSCRIPTS v1.0.0")
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
            summary = process_transcript(transcript_file, output_dir)
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
        
        for summary in summaries:
            print(f"\n{summary['source_file']}:")
            print(f"  Entities: {summary['entities_found']}")
            print(f"  Tags: {summary['total_tags']} total")
            for key in total_entities:
                total_entities[key] += summary['entities_found'][key]
            total_tags += summary['total_tags']
        
        print(f"\nTOTALS:")
        print(f"  Entities found: {total_entities}")
        print(f"  Total tags: {total_tags}")
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"\nOutput files saved to: {output_dir}")

if __name__ == "__main__":
    main()

