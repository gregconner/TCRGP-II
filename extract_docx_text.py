#!/usr/bin/env python3
"""
Extract text from DOCX files for analysis
"""

from docx import Document
import sys

def extract_text(docx_path):
    """Extract all text from a DOCX file."""
    doc = Document(docx_path)
    full_text = []
    for para in doc.paragraphs:
        full_text.append(para.text)
    return '\n'.join(full_text)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 extract_docx_text.py <docx_file>")
        sys.exit(1)
    
    docx_file = sys.argv[1]
    text = extract_text(docx_file)
    print(text)



