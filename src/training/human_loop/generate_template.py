#!/usr/bin/env python3
"""
Generate human_loop template from Train_doc_info.csv

This script reads the doc_info CSV and generates a pre_pdf.json template
with all PDF documents, including page counts if available.

Usage:
    python generate_template.py                    # Generate for training data
    python generate_template.py --final            # Generate for test final data
    python generate_template.py --output custom.json  # Custom output file
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Any, Optional


def get_pdf_page_count(pdf_path: Path) -> Optional[int]:
    """
    Get the number of pages in a PDF file.
    
    Tries multiple methods:
    1. PyPDF2 (most common)
    2. pypdf
    3. pdfplumber
    """
    if not pdf_path.exists():
        return None
    
    # Try PyPDF2
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(str(pdf_path))
        return len(reader.pages)
    except ImportError:
        pass
    except Exception:
        pass
    
    # Try pypdf
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(pdf_path))
        return len(reader.pages)
    except ImportError:
        pass
    except Exception:
        pass
    
    # Try pdfplumber
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except ImportError:
        pass
    except Exception:
        pass
    
    return None


def generate_template(
    input_dir: Path,
    output_path: Path,
    is_final: bool = False,
    count_pages: bool = True
) -> Dict[str, Any]:
    """
    Generate human_loop template from doc_info CSV.
    
    Args:
        input_dir: Input directory containing doc_info CSV and PDFs
        output_path: Path to save the generated template
        is_final: If True, process test final data
        count_pages: If True, attempt to count PDF pages
    
    Returns:
        Generated template dict
    """
    # Determine paths
    if is_final:
        doc_info_csv = input_dir / "Test final_doc_info.csv"
        pdf_dir = input_dir / "Test final_pdf"
    else:
        doc_info_csv = input_dir / "Train_doc_info.csv"
        pdf_dir = input_dir / "Train_pdf" / "pdf"
    
    if not doc_info_csv.exists():
        raise FileNotFoundError(f"Doc info CSV not found: {doc_info_csv}")
    
    # Read CSV
    documents = []
    with open(doc_info_csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pdf_filename = row.get("doc_location_url", "")
            if not pdf_filename:
                continue
            
            pdf_stem = Path(pdf_filename).stem
            doc_id = row.get("doc_id", "")
            nacc_id = row.get("nacc_id", "")
            
            # Try to get page count
            total_pages = None
            if count_pages:
                pdf_path = pdf_dir / pdf_filename
                total_pages = get_pdf_page_count(pdf_path)
            
            doc_entry = {
                "pdf_name": pdf_stem,
                "doc_id": doc_id,
                "nacc_id": nacc_id,
                "total_pages": total_pages,
                "ignore_pages": [],
                "notes": ""
            }
            documents.append(doc_entry)
    
    # Create template
    template = {
        "_description": "Human-in-the-loop configuration for Phase 0 OCR. Define pages to ignore for each PDF to reduce OCR cost.",
        "_usage": "Set USE_HUNMAN_IN_LOOP=TRUE in .env to enable this configuration.",
        "_format": {
            "pdf_name": "PDF filename without extension (stem)",
            "doc_id": "Document ID from doc_info CSV",
            "nacc_id": "NACC ID from doc_info CSV",
            "total_pages": "Total number of pages in the PDF (required if ignoring pages)",
            "ignore_pages": "Array of page numbers (1-indexed) to skip during OCR. Example: [2, 3, 15] to ignore pages 2, 3, and 15",
            "notes": "Optional notes about why pages are ignored"
        },
        "_generated_from": str(doc_info_csv),
        "documents": documents
    }
    
    # Save template
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(template, f, ensure_ascii=False, indent=2)
    
    print(f"{'='*60}")
    print("Human Loop Template Generator")
    print(f"{'='*60}")
    print(f"Mode: {'Test Final' if is_final else 'Training'}")
    print(f"Source CSV: {doc_info_csv}")
    print(f"Output: {output_path}")
    print(f"Documents: {len(documents)}")
    
    # Count how many have page counts
    with_pages = sum(1 for d in documents if d.get("total_pages"))
    print(f"With page counts: {with_pages}/{len(documents)}")
    print(f"{'='*60}")
    
    if not count_pages or with_pages < len(documents):
        print("\nNOTE: Some documents don't have page counts.")
        print("You can manually add total_pages after reviewing the PDFs.")
        print("Install PyPDF2 (pip install PyPDF2) for automatic page counting.")
    
    return template


def main():
    parser = argparse.ArgumentParser(
        description="Generate human_loop template from doc_info CSV"
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="Generate template for test final data instead of training"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Custom output file path"
    )
    parser.add_argument(
        "--no-count",
        action="store_true",
        help="Don't attempt to count PDF pages"
    )
    
    args = parser.parse_args()
    
    # Determine paths
    src_dir = Path(__file__).parent.parent.parent
    
    if args.final:
        input_dir = src_dir / "test final" / "test final input"
        default_output = Path(__file__).parent / "pre_pdf_final.json"
    else:
        input_dir = src_dir / "training" / "train input"
        default_output = Path(__file__).parent / "pre_pdf.json"
    
    output_path = Path(args.output) if args.output else default_output
    
    generate_template(
        input_dir=input_dir,
        output_path=output_path,
        is_final=args.final,
        count_pages=not args.no_count
    )


if __name__ == "__main__":
    main()
