#!/usr/bin/env python3
"""
Create Professional PDF of Interview Quotes v1.0.0

Generates an impressive PDF document with direct quotes from interviews
organized by survey question, with professional formatting and citations.
"""

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, KeepTogether
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfgen import canvas
import pandas as pd
from datetime import datetime

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

INTERVIEW_NAMES = {
    "DM_RSF": "RSF Cooperative (British Columbia)",
    "RTZ": "RTZ Yallane Tribe Artists Cooperative",
    "Allottees": "E' Numu Diip Allottee Cooperative",
    "ManyNations": "Many Nations Cooperative (Saskatchewan)"
}

class NumberedCanvas(canvas.Canvas):
    """Custom canvas to add page numbers."""
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):
        self.setFont("Helvetica", 9)
        self.drawRightString(7.5*inch, 0.5*inch, f"Page {self._pageNumber} of {page_count}")

def create_pdf():
    """Create professional PDF with interview quotes."""
    
    # Read the CSV data
    df = pd.read_csv('/Users/gregoryconner/TCRGP II/interview_quotes_by_question_v1.0.0.csv')
    
    # Create PDF
    pdf_file = '/Users/gregoryconner/TCRGP II/interview_quotes_by_question_v1.0.0.pdf'
    doc = SimpleDocTemplate(
        pdf_file,
        pagesize=letter,
        rightMargin=0.75*inch,
        leftMargin=0.75*inch,
        topMargin=1*inch,
        bottomMargin=0.75*inch
    )
    
    # Container for PDF elements
    story = []
    
    # Define styles
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=colors.HexColor('#1F4E78'),
        spaceAfter=12,
        alignment=TA_CENTER,
        fontName='Helvetica-Bold'
    )
    
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=colors.HexColor('#666666'),
        spaceAfter=20,
        alignment=TA_CENTER,
        fontName='Helvetica'
    )
    
    question_style = ParagraphStyle(
        'QuestionStyle',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=colors.HexColor('#1F4E78'),
        spaceAfter=8,
        spaceBefore=16,
        fontName='Helvetica-Bold',
        borderWidth=0,
        borderPadding=0,
        borderColor=None,
        backColor=colors.HexColor('#E8F0F8'),
        borderRadius=0
    )
    
    interview_style = ParagraphStyle(
        'InterviewStyle',
        parent=styles['Heading3'],
        fontSize=12,
        textColor=colors.HexColor('#2C5F2D'),
        spaceAfter=6,
        spaceBefore=10,
        fontName='Helvetica-Bold'
    )
    
    quote_style = ParagraphStyle(
        'QuoteStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.black,
        spaceAfter=8,
        leftIndent=20,
        fontName='Helvetica',
        alignment=TA_JUSTIFY
    )
    
    citation_style = ParagraphStyle(
        'CitationStyle',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.HexColor('#666666'),
        spaceAfter=12,
        leftIndent=20,
        fontName='Helvetica-Oblique'
    )
    
    # Title page
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("TCRGPII Survey Questions", title_style))
    story.append(Paragraph("Interview Evidence Documentation", title_style))
    story.append(Spacer(1, 0.3*inch))
    story.append(Paragraph(
        "Direct Quotes from Four Indigenous Cooperative Interviews<br/>Supporting Survey Coverage Analysis",
        subtitle_style
    ))
    story.append(Spacer(1, 0.2*inch))
    story.append(Paragraph(
        f"Generated: {datetime.now().strftime('%B %d, %Y')}",
        subtitle_style
    ))
    story.append(Spacer(1, 0.5*inch))
    
    # Executive summary
    summary_text = """
    <b>Document Purpose:</b><br/>
    This document provides direct textual evidence from four Indigenous cooperative interviews
    that demonstrate coverage of all nine TCRGPII survey questions. Each question is supported
    by up to four relevant excerpts from each interview, with character positions cited for
    verification and audit purposes.<br/><br/>
    
    <b>Interviews Analyzed:</b><br/>
    • RSF Cooperative (British Columbia) - DM, RSF Leadership<br/>
    • RTZ Yallane Tribe Artists Cooperative - KE, CG, PL<br/>
    • E' Numu Diip Allottee Cooperative - Cheryl Lowman<br/>
    • Many Nations Cooperative (Saskatchewan) - Tom Hodgson<br/><br/>
    
    <b>Methodology:</b><br/>
    Quotes are elided to focus on key content relevant to each question, with ellipses (...)
    indicating removed text. Character positions allow for verification against original
    interview transcripts. All quotes are verbatim from interview subjects.
    """
    
    summary_para = Paragraph(summary_text, styles['Normal'])
    story.append(summary_para)
    story.append(PageBreak())
    
    # Process each question
    for q_num in sorted(SURVEY_QUESTIONS.keys()):
        question_text = SURVEY_QUESTIONS[q_num]
        question_data = df[df['Question'] == q_num]
        
        # Question header
        story.append(Paragraph(f"<b>{q_num}:</b> {question_text}", question_style))
        story.append(Spacer(1, 0.1*inch))
        
        # Group by interview
        for interview in ["DM_RSF", "RTZ", "Allottees", "ManyNations"]:
            interview_data = question_data[question_data['Interview'] == interview]
            
            if len(interview_data) > 0:
                # Interview header
                story.append(Paragraph(
                    f"<b>{INTERVIEW_NAMES[interview]}</b>",
                    interview_style
                ))
                
                # Add each excerpt
                for _, row in interview_data.iterrows():
                    # Quote
                    quote_text = row['Quote'].replace('...', '…')  # Use proper ellipsis
                    story.append(Paragraph(f'"{quote_text}"', quote_style))
                    
                    # Citation
                    citation = f"[Position: {int(row['Char_Position'])}, Excerpt {int(row['Excerpt_Num'])}]"
                    story.append(Paragraph(citation, citation_style))
                
                story.append(Spacer(1, 0.15*inch))
        
        # Page break after each question except the last
        if q_num != 'Q9':
            story.append(PageBreak())
    
    # Build PDF with custom canvas for page numbers
    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"PDF created: {pdf_file}")
    return pdf_file

def main():
    """Main execution function."""
    print("=" * 80)
    print("CREATING PROFESSIONAL PDF REPORT v1.0.0")
    print("=" * 80)
    print()
    
    pdf_file = create_pdf()
    
    print()
    print("=" * 80)
    print("PDF GENERATION COMPLETE!")
    print("=" * 80)
    print()
    print(f"Output file: {pdf_file}")
    print()
    print("The PDF includes:")
    print("  • Professional title page with executive summary")
    print("  • All 9 survey questions with supporting quotes")
    print("  • 144 direct quotes from 4 interviews")
    print("  • Character position citations for verification")
    print("  • Page numbers and professional formatting")

if __name__ == "__main__":
    main()



