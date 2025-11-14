#!/usr/bin/env python3
"""
Analyze Round 2 Interview Transcripts v1.0.0

This script analyzes the second round of interview transcripts to determine
if they provide answers to the nine TCRGPII survey questions, similar to
the analysis performed on the first round of interviews.
"""

from docx import Document
import re

# Survey questions
SURVEY_QUESTIONS = {
    "Q1": "Was the cooperative originally designed to support Tribal values and traditional systems?",
    "Q2": "Did the cooperative develop a marketing plan?",
    "Q3": "Does the cooperative utilize website/social media marketing?",
    "Q4": "Did you design the cooperative only among your group members or did you have outside assistance?",
    "Q5": "Were you aware of standard cooperative development approaches? Did you use these?",
    "Q6": "Have you had to settle major differences between the coop and the local community?",
    "Q7": "Do you keep community and Tribal leadership engaged?",
    "Q8": "Do you feel that your cooperative has been successful overall?",
    "Q9": "Did COVID have a significant impact on your co-op?"
}

# Interview file paths
ROUND2_INTERVIEWS = {
    "Alaska_Electric": "/Users/gregoryconner/TCRGP II/Alaska Village Electric Coop Recording.transcript_Anna Sattler_10.10.2025.docx",
    "Earth_Sky_Floral": "/Users/gregoryconner/TCRGP II/Updated Earth and Sky Floral from 10.31.25.docx",
    "TWU": "/Users/gregoryconner/TCRGP II/TWU Raw Transcript from Jodi 7.30.25.docx"
}

def extract_text_from_docx(docx_path):
    """Extract all text from a DOCX file."""
    try:
        doc = Document(docx_path)
        full_text = []
        for para in doc.paragraphs:
            if para.text.strip():  # Only add non-empty paragraphs
                full_text.append(para.text)
        return '\n'.join(full_text)
    except Exception as e:
        print(f"Error reading {docx_path}: {e}")
        return ""

def analyze_coverage(text, interview_name):
    """Analyze how well the interview covers each survey question."""
    
    # Convert to lowercase for matching
    text_lower = text.lower()
    
    coverage = {}
    
    # Q1: Tribal values and traditional systems
    q1_keywords = ['tribal value', 'traditional', 'culture', 'indigenous', 'native', 
                   'community value', 'heritage', 'elder', 'ceremony', 'spiritual']
    q1_matches = sum(1 for kw in q1_keywords if kw in text_lower)
    coverage['Q1'] = 'HIGH' if q1_matches >= 5 else 'MEDIUM' if q1_matches >= 2 else 'LOW'
    
    # Q2: Marketing plan
    q2_keywords = ['market', 'marketing plan', 'business plan', 'sales', 'promotion', 
                   'advertis', 'customer', 'brand']
    q2_matches = sum(1 for kw in q2_keywords if kw in text_lower)
    coverage['Q2'] = 'HIGH' if q2_matches >= 5 else 'MEDIUM' if q2_matches >= 2 else 'LOW'
    
    # Q3: Website/social media
    q3_keywords = ['website', 'social media', 'facebook', 'instagram', 'online', 
                   'digital', 'internet', 'web']
    q3_matches = sum(1 for kw in q3_keywords if kw in text_lower)
    coverage['Q3'] = 'HIGH' if q3_matches >= 4 else 'MEDIUM' if q3_matches >= 2 else 'LOW'
    
    # Q4: Outside assistance
    q4_keywords = ['consultant', 'developer', 'assistance', 'help', 'support organization', 
                   'partner', 'advisor', 'technical assistance']
    q4_matches = sum(1 for kw in q4_keywords if kw in text_lower)
    coverage['Q4'] = 'HIGH' if q4_matches >= 4 else 'MEDIUM' if q4_matches >= 2 else 'LOW'
    
    # Q5: Standard cooperative approaches
    q5_keywords = ['cooperative model', 'coop development', 'standard', 'best practice', 
                   'bylaws', 'governance', 'board']
    q5_matches = sum(1 for kw in q5_keywords if kw in text_lower)
    coverage['Q5'] = 'HIGH' if q5_matches >= 4 else 'MEDIUM' if q5_matches >= 2 else 'LOW'
    
    # Q6: Community differences/challenges
    q6_keywords = ['challenge', 'difficult', 'conflict', 'issue', 'problem', 'concern', 
                   'disagree', 'barrier']
    q6_matches = sum(1 for kw in q6_keywords if kw in text_lower)
    coverage['Q6'] = 'HIGH' if q6_matches >= 5 else 'MEDIUM' if q6_matches >= 2 else 'LOW'
    
    # Q7: Leadership engagement
    q7_keywords = ['tribal leader', 'council', 'chief', 'board', 'engage', 'communicate', 
                   'meeting', 'leadership']
    q7_matches = sum(1 for kw in q7_keywords if kw in text_lower)
    coverage['Q7'] = 'HIGH' if q7_matches >= 5 else 'MEDIUM' if q7_matches >= 2 else 'LOW'
    
    # Q8: Success
    q8_keywords = ['success', 'grow', 'profit', 'achieve', 'accomplish', 'positive', 
                   'benefit', 'impact', 'effective']
    q8_matches = sum(1 for kw in q8_keywords if kw in text_lower)
    coverage['Q8'] = 'HIGH' if q8_matches >= 5 else 'MEDIUM' if q8_matches >= 2 else 'LOW'
    
    # Q9: COVID impact
    q9_keywords = ['covid', 'pandemic', 'coronavirus', 'lockdown', 'quarantine', 
                   'remote work', 'virtual', 'zoom']
    q9_matches = sum(1 for kw in q9_keywords if kw in text_lower)
    coverage['Q9'] = 'HIGH' if q9_matches >= 3 else 'MEDIUM' if q9_matches >= 1 else 'NONE'
    
    return coverage

def main():
    """Main execution function."""
    print("=" * 80)
    print("ANALYZING ROUND 2 INTERVIEW TRANSCRIPTS v1.0.0")
    print("=" * 80)
    print()
    
    all_coverage = {}
    
    for interview_name, filepath in ROUND2_INTERVIEWS.items():
        print(f"Reading: {interview_name}...")
        text = extract_text_from_docx(filepath)
        
        if text:
            print(f"  ✓ Extracted {len(text)} characters")
            coverage = analyze_coverage(text, interview_name)
            all_coverage[interview_name] = coverage
        else:
            print(f"  ✗ Failed to extract text")
            all_coverage[interview_name] = {q: 'NONE' for q in SURVEY_QUESTIONS.keys()}
        print()
    
    # Summary report
    print("=" * 80)
    print("COVERAGE SUMMARY")
    print("=" * 80)
    print()
    
    print(f"{'Question':<8} {'Description':<60} ", end='')
    for interview_name in ROUND2_INTERVIEWS.keys():
        short_name = interview_name[:10]
        print(f"{short_name:<12}", end='')
    print()
    print("-" * (8 + 60 + 12 * len(ROUND2_INTERVIEWS)))
    
    for q_num, q_text in SURVEY_QUESTIONS.items():
        # Truncate question text if too long
        q_display = q_text[:57] + "..." if len(q_text) > 60 else q_text
        print(f"{q_num:<8} {q_display:<60} ", end='')
        
        for interview_name in ROUND2_INTERVIEWS.keys():
            coverage_level = all_coverage[interview_name][q_num]
            print(f"{coverage_level:<12}", end='')
        print()
    
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print()
    
    # Overall assessment
    print("OVERALL ASSESSMENT:")
    print()
    
    total_high = sum(1 for cov in all_coverage.values() for level in cov.values() if level == 'HIGH')
    total_medium = sum(1 for cov in all_coverage.values() for level in cov.values() if level == 'MEDIUM')
    total_low = sum(1 for cov in all_coverage.values() for level in cov.values() if level == 'LOW')
    total_none = sum(1 for cov in all_coverage.values() for level in cov.values() if level == 'NONE')
    total_cells = len(ROUND2_INTERVIEWS) * len(SURVEY_QUESTIONS)
    
    print(f"HIGH coverage:   {total_high}/{total_cells} ({100*total_high/total_cells:.1f}%)")
    print(f"MEDIUM coverage: {total_medium}/{total_cells} ({100*total_medium/total_cells:.1f}%)")
    print(f"LOW coverage:    {total_low}/{total_cells} ({100*total_low/total_cells:.1f}%)")
    print(f"NONE:            {total_none}/{total_cells} ({100*total_none/total_cells:.1f}%)")
    print()
    
    # Comparison to Round 1
    print("COMPARISON TO ROUND 1:")
    print("Round 1 had 100% HIGH/MEDIUM-HIGH coverage across all 4 interviews and 9 questions.")
    print(f"Round 2 has {100*(total_high+total_medium)/total_cells:.1f}% HIGH/MEDIUM coverage.")
    print()
    
    if (total_high + total_medium) / total_cells >= 0.80:
        print("✓ Round 2 interviews provide substantial coverage of survey questions")
        print("  (similar to Round 1 findings)")
    else:
        print("⚠ Round 2 interviews provide limited coverage of survey questions")
        print("  (different from Round 1 findings)")

if __name__ == "__main__":
    main()

