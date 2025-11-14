#!/usr/bin/env python3
"""
Analyze which survey questions from TCGRPII survey questions 11/14/25 
were answered in the original interview transcripts.

This script compares the survey questions against the interview content
to determine coverage and identify gaps.
"""

import re
from pathlib import Path

# Survey questions from the file
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
INTERVIEW_FILES = {
    "DM_RSF": "/Users/gregoryconner/TCRGP II/Interview with DM, RSF.txt",
    "RTZ": "/Users/gregoryconner/TCRGP II/Interview with RTZ Leadership.txt",
    "Allottees": "/Users/gregoryconner/TCRGP II/allottees.txt",
    "ManyNations": "/Users/gregoryconner/TCRGP II/manynations.txt"
}

def read_interview(filepath):
    """Read interview transcript from file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return ""

def analyze_question_coverage(interview_text, interview_name):
    """
    Analyze which survey questions were addressed in the interview.
    Returns a dictionary of question coverage with relevant excerpts.
    """
    coverage = {}
    interview_lower = interview_text.lower()
    
    # Q1: Tribal values and traditional systems
    tribal_values_keywords = ['tribal value', 'traditional value', 'traditional system', 
                              'indigenous value', 'cultural value', 'traditional teaching',
                              'indigenous culture', 'cultural security', 'traditional practice']
    matches = []
    for keyword in tribal_values_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q1'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 3 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q2: Marketing plan
    marketing_plan_keywords = ['marketing plan', 'marketing strategy', 'marketing department',
                               'market', 'advertis', 'promotion', 'brand']
    matches = []
    for keyword in marketing_plan_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q2'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if 'marketing plan' in interview_lower else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q3: Website/social media marketing
    digital_marketing_keywords = ['website', 'social media', 'facebook', 'twitter', 'instagram',
                                  'online', 'digital', 'internet']
    matches = []
    for keyword in digital_marketing_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q3'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 2 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q4: Group design vs outside assistance
    assistance_keywords = ['outside assist', 'consultant', 'coop developer', 'cooperative developer',
                          'technical assist', 'facilitator', 'expert', 'steering committee']
    matches = []
    for keyword in assistance_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q4'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 2 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q5: Standard cooperative development approaches
    coop_approaches_keywords = ['standard cooperative', 'cooperative development', 'cooperative model',
                                'cooperative structure', 'cooperative business', 'coop model']
    matches = []
    for keyword in coop_approaches_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q5'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 2 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q6: Settling differences with local community
    conflict_keywords = ['conflict', 'differences', 'issue', 'challenge', 'problem', 'dispute',
                        'local community', 'community issue', 'disagree', 'tension']
    matches = []
    for keyword in conflict_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q6'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 3 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q7: Keeping community and tribal leadership engaged
    engagement_keywords = ['tribal leadership', 'community engaged', 'leadership engaged',
                          'tribal council', 'chief', 'governor', 'board meet', 'member engagement']
    matches = []
    for keyword in engagement_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q7'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 2 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q8: Success of cooperative
    success_keywords = ['success', 'successful', 'achievement', 'accomplish', 'working well',
                       'profit', 'growth', 'sustain']
    matches = []
    for keyword in success_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q8'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if len(matches) >= 3 else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    # Q9: COVID impact
    covid_keywords = ['covid', 'pandemic', 'coronavirus', 'lockdown', 'closure', 'zoom',
                     'virtual', 'remote work']
    matches = []
    for keyword in covid_keywords:
        if keyword in interview_lower:
            matches.append(keyword)
    coverage['Q9'] = {
        'answered': len(matches) > 0,
        'keywords_found': matches,
        'confidence': 'HIGH' if 'covid' in interview_lower or 'pandemic' in interview_lower else 'MEDIUM' if len(matches) > 0 else 'NONE'
    }
    
    return coverage

def generate_report():
    """Generate comprehensive coverage report."""
    print("=" * 80)
    print("SURVEY QUESTION COVERAGE ANALYSIS")
    print("TCRGP II Survey Questions (11/14/25) vs Original Interviews")
    print("=" * 80)
    print()
    
    all_coverage = {}
    
    # Analyze each interview
    for interview_name, filepath in INTERVIEW_FILES.items():
        print(f"\n{'='*80}")
        print(f"INTERVIEW: {interview_name}")
        print('='*80)
        
        interview_text = read_interview(filepath)
        if not interview_text:
            print(f"Could not read interview file: {filepath}")
            continue
        
        coverage = analyze_question_coverage(interview_text, interview_name)
        all_coverage[interview_name] = coverage
        
        for q_id in sorted(coverage.keys()):
            q_text = SURVEY_QUESTIONS[q_id]
            result = coverage[q_id]
            
            print(f"\n{q_id}: {q_text}")
            print(f"  Answered: {'YES' if result['answered'] else 'NO'}")
            print(f"  Confidence: {result['confidence']}")
            if result['keywords_found']:
                print(f"  Keywords found: {', '.join(result['keywords_found'][:5])}")
    
    # Generate summary table
    print(f"\n\n{'='*80}")
    print("SUMMARY: QUESTION COVERAGE ACROSS ALL INTERVIEWS")
    print('='*80)
    print()
    print(f"{'Question':<10} {'DM/RSF':<12} {'RTZ':<12} {'Allottees':<12} {'ManyNations':<12}")
    print('-'*80)
    
    for q_id in sorted(SURVEY_QUESTIONS.keys()):
        row = f"{q_id:<10} "
        for interview_name in ['DM_RSF', 'RTZ', 'Allottees', 'ManyNations']:
            if interview_name in all_coverage:
                conf = all_coverage[interview_name][q_id]['confidence']
                symbol = '✓✓' if conf == 'HIGH' else '✓' if conf == 'MEDIUM' else '✗'
                row += f"{symbol:<12} "
            else:
                row += f"{'N/A':<12} "
        print(row)
    
    print()
    print("Legend: ✓✓ = HIGH confidence, ✓ = MEDIUM confidence, ✗ = Not answered")
    print()
    
    # Overall statistics
    print(f"\n{'='*80}")
    print("OVERALL STATISTICS")
    print('='*80)
    
    for interview_name in ['DM_RSF', 'RTZ', 'Allottees', 'ManyNations']:
        if interview_name not in all_coverage:
            continue
        
        high_count = sum(1 for q in all_coverage[interview_name].values() if q['confidence'] == 'HIGH')
        medium_count = sum(1 for q in all_coverage[interview_name].values() if q['confidence'] == 'MEDIUM')
        none_count = sum(1 for q in all_coverage[interview_name].values() if q['confidence'] == 'NONE')
        
        print(f"\n{interview_name}:")
        print(f"  HIGH confidence answers: {high_count}/{len(SURVEY_QUESTIONS)}")
        print(f"  MEDIUM confidence answers: {medium_count}/{len(SURVEY_QUESTIONS)}")
        print(f"  Not answered: {none_count}/{len(SURVEY_QUESTIONS)}")
        print(f"  Coverage rate: {((high_count + medium_count) / len(SURVEY_QUESTIONS) * 100):.1f}%")

if __name__ == "__main__":
    generate_report()

