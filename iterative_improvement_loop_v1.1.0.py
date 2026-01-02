#!/usr/bin/env python3
"""
Iterative Improvement Loop v1.1.0

Automated iterative improvement system for transcript cleaning.
Runs: clean → grade → improve → repeat until all A grades or no progress.

OPTIMIZED v1.1.0:
- Parallel grading using all available CPUs
- Removed manual input prompts for full automation
- Updated to use v1.8.0 cleaner
- Optimized for speed

Version: 1.1.0
"""

import subprocess
import json
import sys
import multiprocessing
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime
from functools import partial

def run_cleaner(transcript_files: List[Path], output_dir: Path, version: str) -> bool:
    """Run the cleaner on transcripts."""
    print(f"\n{'='*80}")
    print(f"RUNNING CLEANER (v{version})")
    print(f"{'='*80}")
    
    # Build command
    cmd = [
        sys.executable,
        f"deidentify_and_tag_transcripts_v{version}.py",
        "-i", str(transcript_files[0].parent),
        "-o", str(output_dir)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running cleaner: {e}")
        print(e.stderr)
        return False

def run_grader(raw_file: Path, cleaned_file: Path, mapping_file: Path, tags_file: Path, output_file: Path) -> Dict:
    """Run the grader on a cleaned transcript."""
    cmd = [
        sys.executable,
        "grade_transcript_cleaning_v1.0.0.py",
        str(raw_file),
        str(cleaned_file),
        "-m", str(mapping_file),
        "-t", str(tags_file),
        "-o", str(output_file)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        # Parse JSON output
        with open(output_file, 'r') as f:
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
            "spelling_matching": []
        },
        "common_issues": [],
        "improvement_priorities": []
    }
    
    for report in grading_reports:
        analysis["overall_scores"].append(report["overall_score"])
        
        for cat_key, cat_data in report["categories"].items():
            if cat_data["grade"] != "A":
                analysis["category_issues"][cat_key].append({
                    "transcript": report["transcript_name"],
                    "grade": cat_data["grade"],
                    "score": cat_data["score"],
                    "suggestions": cat_data["suggestions"]
                })
    
    # Calculate average score
    if analysis["overall_scores"]:
        analysis["average_score"] = sum(analysis["overall_scores"]) / len(analysis["overall_scores"])
    else:
        analysis["average_score"] = 0
    
    # Identify most common issues
    all_suggestions = []
    for report in grading_reports:
        for cat_data in report["categories"].values():
            all_suggestions.extend(cat_data["suggestions"])
    
    from collections import Counter
    suggestion_counts = Counter(all_suggestions)
    analysis["common_issues"] = [item for item, count in suggestion_counts.most_common(10)]
    
    # Determine improvement priorities
    category_scores = {}
    for cat_key in analysis["category_issues"]:
        issues = analysis["category_issues"][cat_key]
        if issues:
            avg_score = sum(issue["score"] for issue in issues) / len(issues)
            category_scores[cat_key] = avg_score
    
    # Sort by lowest scores (highest priority)
    analysis["improvement_priorities"] = sorted(
        category_scores.items(),
        key=lambda x: x[1]
    )
    
    return analysis

def generate_improvement_suggestions(analysis: Dict) -> List[str]:
    """Generate actionable improvement suggestions."""
    suggestions = []
    
    # Check each category
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
    """Main iterative improvement loop - OPTIMIZED v1.1.0."""
    print("=" * 80)
    print("ITERATIVE IMPROVEMENT LOOP v1.1.0 (OPTIMIZED)")
    print(f"Using {multiprocessing.cpu_count()} CPU cores")
    print("=" * 80)
    
    # Setup paths
    script_dir = Path(__file__).parent
    newer_dir = script_dir / "newer transcripts"
    output_base = script_dir / "iteration_outputs"
    output_base.mkdir(exist_ok=True)
    
    # Find 5 new transcripts (exclude Alaska)
    transcript_files = sorted([f for f in newer_dir.glob("*.docx") if "Alaska" not in f.name])
    
    if len(transcript_files) < 5:
        print(f"Warning: Only found {len(transcript_files)} new transcripts")
    
    print(f"\nFound {len(transcript_files)} new transcript(s) to process")
    
    # Initialize - UPDATED v1.1.0 to use latest version
    cleaner_version = "1.15.0"
    grader_version = "1.0.0"
    iteration = 0
    previous_scores = []
    no_progress_count = 0
    max_iterations = 5  # Safety limit
    
    while True:
        iteration += 1
        print(f"\n{'='*80}")
        print(f"ITERATION {iteration}")
        print(f"{'='*80}")
        
        # Create iteration directory
        iter_dir = output_base / f"iteration_{iteration}"
        iter_dir.mkdir(exist_ok=True)
        cleaned_dir = iter_dir / "cleaned"
        cleaned_dir.mkdir(exist_ok=True)
        grades_dir = iter_dir / "grades"
        grades_dir.mkdir(exist_ok=True)
        
        # Step 1: Run cleaner
        print(f"\nStep 1: Running cleaner v{cleaner_version}...")
        if not run_cleaner(transcript_files, cleaned_dir, cleaner_version):
            print("Error: Cleaner failed")
            break
        
        # Step 2: Run grader on each transcript (PARALLELIZED v1.1.0)
        print(f"\nStep 2: Running grader v{grader_version} (parallelized)...")
        grading_reports = []
        
        # Prepare grading tasks
        grading_tasks = []
        for raw_file in transcript_files:
            base_name = raw_file.stem
            cleaned_file = cleaned_dir / f"{base_name}_deidentified.txt"
            mapping_file = cleaned_dir / f"{base_name}_mapping.json"
            tags_file = cleaned_dir / f"{base_name}_tags.csv"
            grade_file = grades_dir / f"{base_name}_grade.json"
            
            if cleaned_file.exists():
                grading_tasks.append((raw_file, cleaned_file, mapping_file, tags_file, grade_file))
        
        # Run grading in parallel using all available CPUs
        num_workers = min(multiprocessing.cpu_count(), len(grading_tasks))
        print(f"  Using {num_workers} CPU cores for parallel grading...")
        
        if num_workers > 1 and len(grading_tasks) > 1:
            with multiprocessing.Pool(processes=num_workers) as pool:
                results = pool.map(run_grader_wrapper, grading_tasks)
                for filename, report in results:
                    if report:
                        grading_reports.append(report)
                        print(f"  ✓ Graded: {filename}")
        else:
            # Fallback to sequential if only one task or one CPU
            for raw_file, cleaned_file, mapping_file, tags_file, grade_file in grading_tasks:
                print(f"  Grading: {raw_file.name}")
                report = run_grader(raw_file, cleaned_file, mapping_file, tags_file, grade_file)
                if report:
                    grading_reports.append(report)
        
        # Step 3: Analyze grades
        print(f"\nStep 3: Analyzing grades...")
        analysis = analyze_grades(grading_reports)
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"ITERATION {iteration} RESULTS")
        print(f"{'='*80}")
        print(f"\nAverage Score: {analysis['average_score']:.1f}/100")
        print(f"\nCategory Grades:")
        for report in grading_reports:
            print(f"\n  {report['transcript_name']}: {report['overall_grade']} ({report['overall_score']:.1f}/100)")
            for cat_key, cat_data in report['categories'].items():
                if cat_data['grade'] != 'A':
                    print(f"    - {cat_key}: {cat_data['grade']} ({cat_data['score']:.1f})")
        
        # Check if all A grades
        all_a_grades = all(
            report['overall_grade'] == 'A' or report['overall_grade'] == 'A-'
            for report in grading_reports
        )
        
        if all_a_grades:
            print(f"\n{'='*80}")
            print("SUCCESS: All transcripts achieved A grades!")
            print(f"{'='*80}")
            break
        
        # Check for progress
        current_avg = analysis['average_score']
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
            break
        
        # Step 4: Generate improvement suggestions
        print(f"\nStep 4: Generating improvement suggestions...")
        suggestions = generate_improvement_suggestions(analysis)
        
        print(f"\nImprovement Priorities:")
        for priority, score in analysis['improvement_priorities'][:5]:
            print(f"  - {priority}: {score:.1f}")
        
        print(f"\nSuggestions:")
        for sugg in suggestions[:10]:
            print(f"  • {sugg}")
        
        # Save iteration summary
        summary_file = iter_dir / "iteration_summary.json"
        with open(summary_file, 'w') as f:
            json.dump({
                "iteration": iteration,
                "average_score": current_avg,
                "grading_reports": grading_reports,
                "analysis": analysis,
                "suggestions": suggestions
            }, f, indent=2)
        
        print(f"\n✓ Iteration {iteration} complete. Summary saved to {summary_file}")
        
        # Safety check: limit iterations
        if iteration >= max_iterations:
            print(f"\n{'='*80}")
            print(f"STOPPING: Reached maximum iterations ({max_iterations})")
            print(f"{'='*80}")
            break
        
        # Auto-continue (removed manual input for speed) - v1.1.0
        print(f"\nContinuing to iteration {iteration + 1}...")

if __name__ == "__main__":
    main()

