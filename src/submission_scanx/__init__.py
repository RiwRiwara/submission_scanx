"""
Submission ScanX - Thai Government Financial Disclosure Document Extraction

This package provides tools to extract structured data from Thai NACC
financial disclosure PDF documents.

Modules:
    phase0_ocr: PDF to OCR extraction using Azure Document Intelligence
    phase1_process: JSON processing and page matching
    phase1c_text_extract: Extract text content from each page
    phase1d_metadata: Map pages to extraction steps
    pipeline: Main pipeline runner
"""

__version__ = "0.1.0"

from .pipeline import run_pipeline, run_phase0, run_phase1
from .phase1_process import process_phase1c, process_phase1d
