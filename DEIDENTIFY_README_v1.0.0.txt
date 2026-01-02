================================================================================
DE-IDENTIFY AND TAG TRANSCRIPTS v1.0.0
================================================================================

This program automatically de-identifies transcripts and tags research-relevant
keywords for analysis.

================================================================================
FEATURES
================================================================================

1. DE-IDENTIFICATION:
   - Replaces person names with codes (Person_1, Person_2, etc.)
   - Replaces organization names with codes (Organization_1, etc.)
   - Replaces locations with codes (Location_1, etc.)
   - Replaces tribe/nation names with codes (Tribe_1, etc.)
   - Replaces specific dollar amounts with [Financial_Amount]
   - Replaces specific years with [Year]
   - Handles misspellings and name variants using fuzzy matching

2. KEYWORD TAGGING:
   - Tags content by research category (Membership, Governance, Finance, etc.)
   - Tags content by survey question alignment (Q1-Q9)
   - Tags Indigenous-specific terminology
   - Extracts quantitative metrics (member counts, dollar amounts, years)
   - Creates searchable CSV files with line numbers and context

3. NAME VARIANT HANDLING:
   - Automatically detects name misspellings (e.g., "Burshia" vs "Brache")
   - Uses fuzzy string matching to cluster similar names
   - Creates canonical mappings for consistent de-identification
   - Handles common transcription errors

================================================================================
REQUIREMENTS
================================================================================

Python 3.6+
Required packages:
  - python-docx (for reading .docx files)
    Install with: pip install python-docx

Standard library modules (no installation needed):
  - re, json, csv, pathlib, collections, difflib, typing, sys

================================================================================
USAGE
================================================================================

1. Place transcript files in the "newer transcripts" directory
   - Supports .docx and .txt files

2. Run the program:
   python3 deidentify_and_tag_transcripts_v1.0.0.py

3. Output files will be created in "deidentified_transcripts" directory:
   - {filename}_deidentified.txt - De-identified version of transcript
   - {filename}_mapping.json - Mapping of codes to original names/entities
   - {filename}_tags.csv - All tagged keywords with line numbers and context
   - processing_summary.json - Overall statistics

================================================================================
OUTPUT FILES
================================================================================

1. DEIDENTIFIED TEXT (.txt):
   - Original transcript with all identifying information replaced by codes
   - Suitable for sharing/publication while maintaining research value

2. MAPPING FILE (.json):
   - Links codes back to original names/entities
   - Includes name variant clusters showing misspellings detected
   - KEEP THIS FILE SECURE - it contains identifying information

3. TAGS FILE (.csv):
   - All research keywords found in transcript
   - Columns: Tag_Category, Line_Number, Matched_Text, Context
   - Can be imported into Excel or analysis tools
   - Useful for finding specific topics across transcripts

4. SUMMARY FILE (.json):
   - Statistics on entities found and tags created
   - Useful for quality control and understanding data richness

================================================================================
TAG CATEGORIES
================================================================================

RESEARCH CATEGORIES:
- Membership: member, membership, constituent, participant
- Governance: board, director, leadership, governance, bylaws
- Finance: revenue, budget, financial, grant, funding, loan
- Employment: employee, staff, worker, job, contractor
- Partnerships: partner, collaboration, alliance, network
- Innovation: innovation, new practice, new model, development
- Operations: operation, supply chain, processing, equipment
- Markets: market, sales, customer, revenue stream, distribution
- Technology: digital, website, social media, online, technology
- Culture: traditional, tribal value, cultural, indigenous, heritage
- Geography: location, reservation, tribal land, community, region
- Risk: challenge, obstacle, risk, barrier, issue, problem
- Timeline: founded, established, year, started, created
- Success: success, growth, profit, sustainable, impact
- COVID: covid, pandemic, coronavirus, lockdown, quarantine

SURVEY QUESTION TAGS:
- Q1_TribalValues: tribal value, traditional system, cultural
- Q2_MarketingPlan: marketing plan, business plan, marketing strategy
- Q3_WebsiteSocial: website, social media, facebook, instagram, online
- Q4_OutsideAssistance: consultant, developer, assistance, help
- Q5_StandardApproaches: cooperative model, coop development, standard
- Q6_CommunityDifferences: challenge, conflict, issue, problem, barrier
- Q7_LeadershipEngagement: tribal leader, council, chief, board, engage
- Q8_Success: success, grow, profit, achieve, benefit, impact
- Q9_COVID: covid, pandemic, coronavirus, lockdown, quarantine

INDIGENOUS TERMS:
- sovereignty, tribal sovereignty, self-determination, matriarch, elder
- traditional knowledge, land-based, water rights, treaty, reservation

METRICS EXTRACTED:
- Member counts, employee counts, partner counts, grant counts
- Years (founding dates, etc.), dollar amounts

================================================================================
NAME VARIANT HANDLING
================================================================================

The program automatically handles common misspellings:
- "Burshia" variants: brache, berchet, berchet-gowazi, brochure, burche
- "Jodi" vs "Jody"
- Uses fuzzy matching (75% similarity threshold) to detect other variants

For each transcript, the program:
1. Extracts all potential names
2. Clusters similar names using fuzzy matching
3. Creates canonical mapping (most common spelling = canonical)
4. Replaces all variants with same code

This ensures consistent de-identification even with transcription errors.

================================================================================
CUSTOMIZATION
================================================================================

To add more keywords or categories, edit the constants at the top of the script:
- RESEARCH_CATEGORIES: Add keywords for research themes
- SURVEY_QUESTION_TAGS: Add keywords for survey question alignment
- INDIGENOUS_TERMS: Add Indigenous-specific terminology
- COMMON_MISSPELLINGS: Add known name variant patterns
- SIMILARITY_THRESHOLD: Adjust fuzzy matching sensitivity (0.0-1.0)

================================================================================
SECURITY NOTES
================================================================================

- The mapping file contains identifying information - keep it secure
- De-identified files are safe for sharing/publication
- Consider encrypting mapping files if storing long-term
- Review de-identified text to ensure no identifying information remains

================================================================================
VERSION HISTORY
================================================================================

Version 1.0.0 (Current):
- Initial release
- De-identification with fuzzy name matching
- Research keyword tagging
- Survey question alignment tagging
- Quantitative metric extraction
- Support for .docx and .txt files

================================================================================
TROUBLESHOOTING
================================================================================

Error: "python-docx not installed"
  Solution: pip install python-docx

Error: "Input directory not found"
  Solution: Ensure "newer transcripts" directory exists in same folder as script

No transcript files found:
  Solution: Check that files have .docx or .txt extensions

Low tag counts:
  - Keywords are case-insensitive but use word boundaries
  - Check if terms appear in transcript with different spelling
  - Consider adding synonyms to keyword lists

================================================================================
CONTACT
================================================================================

For questions or issues with this program, refer to the main project documentation
or contact the TCRGP II research team.

================================================================================

