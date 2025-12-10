"""
Phase 0: PDF to OCR Extraction using Azure Document Intelligence

This module handles:
1. Reading PDF files from training or test directories
2. Using Azure Document Intelligence to extract text and layout
3. Saving raw JSON output for further processing
"""

import os
import json
import time
import argparse
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)


def load_human_loop_config(is_final: bool = False) -> Dict[str, Any]:
    """
    Load human-in-the-loop configuration for ignore pages.
    
    Args:
        is_final: If True, load from test final human_loop directory
    
    Returns:
        Dict mapping pdf filename (stem) to ignore configuration
    """
    src_dir = Path(__file__).parent.parent
    
    if is_final:
        config_path = src_dir / "test final" / "human_loop" / "pre_pdf.json"
    else:
        config_path = src_dir / "training" / "human_loop" / "pre_pdf.json"
    
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return {}
            config = json.load(f)
            # Convert list to dict for easy lookup by pdf name
            return {item.get("pdf_name", ""): item for item in config.get("documents", [])}
    except (json.JSONDecodeError, Exception) as e:
        print(f"Warning: Failed to load human_loop config: {e}")
        return {}


def is_human_loop_enabled() -> bool:
    """Check if human-in-the-loop mode is enabled."""
    return os.getenv("USE_HUNMAN_IN_LOOP", "FALSE").upper() == "TRUE"


# Azure Document Intelligence pricing (per page)
# S0 tier - Read model: $1.50 per 1,000 pages = $0.0015 per page
# (1M+ pages: $0.60 per 1,000 = $0.0006 per page)
COST_PER_PAGE = 0.0015


def get_report_path(is_final: bool = False) -> Path:
    """Get path to the OCR usage report file."""
    src_dir = Path(__file__).parent.parent
    if is_final:
        return src_dir / "test final" / "human_loop" / "ocr_usage_report.json"
    return src_dir / "training" / "human_loop" / "ocr_usage_report.json"


def load_report(is_final: bool = False) -> Dict[str, Any]:
    """Load existing report or create empty structure."""
    report_path = get_report_path(is_final)
    if report_path.exists():
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        "_description": "OCR usage report for cost tracking and analysis",
        "summary": {
            "total_runs": 0,
            "total_pages_processed": 0,
            "total_pages_ignored": 0,
            "total_estimated_cost": 0.0,
            "total_cost_saved": 0.0
        },
        "runs": []
    }


def save_report(report: Dict[str, Any], is_final: bool = False) -> Path:
    """Save report to file."""
    report_path = get_report_path(is_final)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report_path


def generate_run_report(
    pdf_stats: List[Dict[str, Any]],
    is_final: bool = False,
    human_loop_enabled: bool = False
) -> Dict[str, Any]:
    """
    Generate a run report with usage statistics.
    
    Args:
        pdf_stats: List of per-PDF statistics
        is_final: If True, for test final mode
        human_loop_enabled: If human-in-the-loop was enabled
    
    Returns:
        Run report dictionary
    """
    total_pages = sum(s.get("total_pages", 0) for s in pdf_stats)
    pages_processed = sum(s.get("pages_processed", 0) for s in pdf_stats)
    pages_ignored = sum(s.get("pages_ignored", 0) for s in pdf_stats)
    pages_skipped_existing = sum(s.get("pages_skipped", 0) for s in pdf_stats)
    
    estimated_cost = pages_processed * COST_PER_PAGE
    cost_saved = pages_ignored * COST_PER_PAGE
    
    return {
        "timestamp": datetime.now().isoformat(),
        "mode": "final" if is_final else "training",
        "human_loop_enabled": human_loop_enabled,
        "statistics": {
            "pdfs_total": len(pdf_stats),
            "pdfs_processed": sum(1 for s in pdf_stats if s.get("status") == "processed"),
            "pdfs_skipped": sum(1 for s in pdf_stats if s.get("status") == "skipped"),
            "pdfs_error": sum(1 for s in pdf_stats if s.get("status") == "error"),
            "total_pages": total_pages,
            "pages_processed": pages_processed,
            "pages_ignored": pages_ignored,
            "pages_skipped_existing": pages_skipped_existing
        },
        "cost": {
            "estimated_cost_usd": round(estimated_cost, 4),
            "cost_saved_usd": round(cost_saved, 4),
            "cost_per_page_usd": COST_PER_PAGE
        },
        "pdf_details": pdf_stats
    }


def append_run_to_report(
    run_report: Dict[str, Any],
    is_final: bool = False
) -> Dict[str, Any]:
    """
    Append a run report to the main report and update summary.
    
    Args:
        run_report: The run report to append
        is_final: If True, for test final mode
    
    Returns:
        Updated main report
    """
    report = load_report(is_final)
    
    # Append run
    report["runs"].append(run_report)
    
    # Update summary
    report["summary"]["total_runs"] += 1
    report["summary"]["total_pages_processed"] += run_report["statistics"]["pages_processed"]
    report["summary"]["total_pages_ignored"] += run_report["statistics"]["pages_ignored"]
    report["summary"]["total_estimated_cost"] = round(
        report["summary"]["total_estimated_cost"] + run_report["cost"]["estimated_cost_usd"], 4
    )
    report["summary"]["total_cost_saved"] = round(
        report["summary"]["total_cost_saved"] + run_report["cost"]["cost_saved_usd"], 4
    )
    report["summary"]["last_run"] = run_report["timestamp"]
    
    # Save and return
    save_report(report, is_final)
    return report


def print_cost_report(run_report: Dict[str, Any], report: Dict[str, Any]):
    """Print a formatted cost report."""
    stats = run_report["statistics"]
    cost = run_report["cost"]
    summary = report["summary"]
    
    print(f"\n{'='*60}")
    print("📊 OCR Usage Report")
    print(f"{'='*60}")
    print(f"Timestamp: {run_report['timestamp']}")
    print(f"Mode: {run_report['mode'].upper()}")
    print(f"Human-in-the-Loop: {'ENABLED' if run_report['human_loop_enabled'] else 'DISABLED'}")
    
    print(f"\n📄 This Run:")
    print(f"   PDFs: {stats['pdfs_processed']} processed, {stats['pdfs_skipped']} skipped, {stats['pdfs_error']} errors")
    print(f"   Pages: {stats['pages_processed']} processed, {stats['pages_ignored']} ignored")
    print(f"   💰 Cost: ${cost['estimated_cost_usd']:.4f}")
    if cost['cost_saved_usd'] > 0:
        print(f"   💵 Saved: ${cost['cost_saved_usd']:.4f} (by ignoring pages)")
    
    print(f"\n📈 All-Time Summary (Run #{summary['total_runs']}):")
    print(f"   Total pages processed: {summary['total_pages_processed']}")
    print(f"   Total pages ignored: {summary['total_pages_ignored']}")
    print(f"   💰 Total cost: ${summary['total_estimated_cost']:.4f}")
    print(f"   💵 Total saved: ${summary['total_cost_saved']:.4f}")
    print(f"{'='*60}")


def show_usage_report(is_final: bool = False):
    """Display the full usage report."""
    report = load_report(is_final)
    summary = report["summary"]
    runs = report.get("runs", [])
    
    mode_str = "Test Final" if is_final else "Training"
    
    print(f"\n{'='*70}")
    print(f"📊 OCR Usage Report - {mode_str}")
    print(f"{'='*70}")
    
    if summary["total_runs"] == 0:
        print("No OCR runs recorded yet.")
        print(f"{'='*70}")
        return
    
    print(f"\n📈 Summary:")
    print(f"   Total runs: {summary['total_runs']}")
    print(f"   Last run: {summary.get('last_run', 'N/A')}")
    print(f"   Total pages processed: {summary['total_pages_processed']}")
    print(f"   Total pages ignored: {summary['total_pages_ignored']}")
    print(f"   💰 Total estimated cost: ${summary['total_estimated_cost']:.4f}")
    print(f"   💵 Total cost saved: ${summary['total_cost_saved']:.4f}")
    
    if runs:
        print(f"\n📋 Run History (last 10):")
        print(f"{'─'*70}")
        for run in runs[-10:]:
            ts = run.get("timestamp", "?")[:19]
            stats = run.get("statistics", {})
            cost = run.get("cost", {})
            hil = "✓" if run.get("human_loop_enabled") else "✗"
            print(f"   {ts} | PDFs:{stats.get('pdfs_processed', 0):3d} | "
                  f"Pages:{stats.get('pages_processed', 0):4d} | "
                  f"Ignored:{stats.get('pages_ignored', 0):3d} | "
                  f"${cost.get('estimated_cost_usd', 0):.2f} | HIL:{hil}")
        print(f"{'─'*70}")
    
    report_path = get_report_path(is_final)
    print(f"\n📁 Report file: {report_path}")
    print(f"{'='*70}")


def clear_usage_report(is_final: bool = False):
    """Clear/reset the usage report."""
    report_path = get_report_path(is_final)
    mode_str = "Test Final" if is_final else "Training"
    
    if report_path.exists():
        # Create backup
        backup_path = report_path.with_suffix(".json.bak")
        import shutil
        shutil.copy(report_path, backup_path)
        print(f"Backed up existing report to: {backup_path}")
    
    # Create fresh report
    new_report = {
        "_description": "OCR usage report for cost tracking and analysis",
        "summary": {
            "total_runs": 0,
            "total_pages_processed": 0,
            "total_pages_ignored": 0,
            "total_estimated_cost": 0.0,
            "total_cost_saved": 0.0
        },
        "runs": []
    }
    save_report(new_report, is_final)
    print(f"✓ Cleared OCR usage report for {mode_str} mode")
    print(f"  Report file: {report_path}")


def get_azure_client():
    """Initialize Azure Document Intelligence client."""
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.core.credentials import AzureKeyCredential

    endpoint = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT")
    api_key = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_API_KEY")

    if not endpoint or not api_key:
        raise ValueError(
            "Missing Azure Document Intelligence credentials. "
            "Please set AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT and "
            "AZURE_DOCUMENT_INTELLIGENCE_API_KEY in .env file."
        )

    return DocumentIntelligenceClient(
        endpoint=endpoint,
        credential=AzureKeyCredential(api_key)
    )


def extract_pdf_to_json(
    pdf_path: Path,
    client,
    model_id: str = "prebuilt-read",
    ignore_pages: Optional[List[int]] = None,
    total_pages: Optional[int] = None
) -> Dict[str, Any]:
    """
    Extract text and layout from a PDF using Azure Document Intelligence.

    Args:
        pdf_path: Path to the PDF file
        client: Azure Document Intelligence client
        model_id: Model to use for analysis (default: prebuilt-read)
        ignore_pages: List of page numbers to ignore (1-indexed). These pages
                      will have empty lines in the output to reduce OCR cost.
        total_pages: Total number of pages in the PDF (for creating placeholder entries)

    Returns:
        Dict containing extracted content with pages and lines
    """
    if ignore_pages is None:
        ignore_pages = []
    
    # Read PDF as bytes
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()
    
    # Determine which pages to analyze (all pages except ignored ones)
    pages_param = None
    if ignore_pages and total_pages:
        pages_to_analyze = [p for p in range(1, total_pages + 1) if p not in ignore_pages]
        if pages_to_analyze:
            # Format as "1-3,5,7-10" style for Azure API
            pages_param = ",".join(str(p) for p in pages_to_analyze)

    # Call Azure Document Intelligence API
    analyze_kwargs = {
        "body": pdf_bytes,
        "content_type": "application/pdf"
    }
    if pages_param:
        analyze_kwargs["pages"] = pages_param
    
    poller = client.begin_analyze_document(
        model_id,
        **analyze_kwargs
    )

    result = poller.result()

    # Convert to our JSON format
    output = {
        "file_name": pdf_path.name,
        "content": result.content,
        "pages": []
    }

    # Build a set of OCR'd page numbers for quick lookup
    ocr_pages = {page.page_number: page for page in (result.pages or [])}
    
    # Determine total pages to include in output
    max_page = total_pages or max(ocr_pages.keys(), default=0)
    
    # Build output pages, including placeholder for ignored pages
    for page_num in range(1, max_page + 1):
        if page_num in ignore_pages:
            # Ignored page: empty lines placeholder
            page_data = {
                "page_number": page_num,
                "lines": [],
                "_ignored": True
            }
        elif page_num in ocr_pages:
            # OCR'd page: include full data
            page = ocr_pages[page_num]
            page_data = {
                "page_number": page.page_number,
                "width": page.width,
                "height": page.height,
                "unit": page.unit,
                "lines": []
            }
            if page.lines:
                for line in page.lines:
                    line_data = {
                        "content": line.content,
                        "polygon": line.polygon if line.polygon else None
                    }
                    page_data["lines"].append(line_data)
        else:
            # Page not in OCR result and not ignored (shouldn't happen normally)
            page_data = {
                "page_number": page_num,
                "lines": [],
                "_missing": True
            }
        
        output["pages"].append(page_data)

    # Add paragraphs if available
    if result.paragraphs:
        output["paragraphs"] = [
            {
                "content": p.content,
                "role": p.role if hasattr(p, 'role') else None
            }
            for p in result.paragraphs
        ]

    # Add tables if available
    if result.tables:
        output["tables"] = []
        for table in result.tables:
            table_data = {
                "row_count": table.row_count,
                "column_count": table.column_count,
                "cells": [
                    {
                        "row_index": cell.row_index,
                        "column_index": cell.column_index,
                        "content": cell.content,
                        "row_span": cell.row_span if hasattr(cell, 'row_span') else 1,
                        "column_span": cell.column_span if hasattr(cell, 'column_span') else 1
                    }
                    for cell in table.cells
                ]
            }
            output["tables"].append(table_data)

    return output


def get_pdf_list(input_dir: Path, doc_info_csv: Path) -> List[Dict[str, str]]:
    """
    Get list of PDFs to process from doc_info.csv.

    Args:
        input_dir: Directory containing input files
        doc_info_csv: Path to doc_info CSV file

    Returns:
        List of dicts with doc_id and pdf_filename
    """
    import csv

    pdf_list = []

    with open(doc_info_csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pdf_list.append({
                "doc_id": row.get("doc_id", ""),
                "pdf_filename": row.get("doc_location_url", ""),
                "nacc_id": row.get("nacc_id", "")
            })

    return pdf_list


def process_pdfs(
    input_dir: Path,
    output_dir: Path,
    is_final: bool = False,
    limit: Optional[int] = None,
    skip_existing: bool = True
) -> Dict[str, Any]:
    """
    Process all PDFs in the input directory.

    Args:
        input_dir: Base input directory (train input or test final input)
        output_dir: Directory to save extracted JSON files
        is_final: If True, process test final data
        limit: Maximum number of files to process (for testing)
        skip_existing: Skip files that already have output

    Returns:
        Dict with processing statistics
    """
    # Determine paths based on mode
    if is_final:
        doc_info_csv = input_dir / "Test final_doc_info.csv"
        pdf_dir = input_dir / "Test final_pdf"
    else:
        doc_info_csv = input_dir / "Train_doc_info.csv"
        pdf_dir = input_dir / "Train_pdf" / "pdf"

    if not doc_info_csv.exists():
        raise FileNotFoundError(f"Doc info CSV not found: {doc_info_csv}")

    if not pdf_dir.exists():
        raise FileNotFoundError(f"PDF directory not found: {pdf_dir}")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get PDF list
    pdf_list = get_pdf_list(input_dir, doc_info_csv)

    if limit:
        pdf_list = pdf_list[:limit]

    # Initialize Azure client
    client = get_azure_client()

    # Load human-in-the-loop config if enabled
    human_loop_enabled = is_human_loop_enabled()
    human_loop_config = {}
    if human_loop_enabled:
        human_loop_config = load_human_loop_config(is_final)

    stats = {
        "total": len(pdf_list),
        "processed": 0,
        "skipped": 0,
        "errors": [],
        "pages_ignored": 0
    }
    
    # Track per-PDF stats for reporting
    pdf_stats = []

    print(f"{'='*60}")
    print(f"Phase 0: PDF to OCR Extraction")
    print(f"{'='*60}")
    print(f"Mode: {'Test Final' if is_final else 'Training'}")
    print(f"Input: {pdf_dir}")
    print(f"Output: {output_dir}")
    print(f"Total PDFs: {len(pdf_list)}")
    if human_loop_enabled:
        print(f"Human-in-the-loop: ENABLED ({len(human_loop_config)} docs configured)")
    print(f"{'='*60}")

    for i, pdf_info in enumerate(pdf_list, 1):
        pdf_filename = pdf_info["pdf_filename"]
        pdf_path = pdf_dir / pdf_filename

        # Output filename (same as PDF but with .json extension)
        output_filename = Path(pdf_filename).stem + ".json"
        output_path = output_dir / output_filename

        # Skip if already exists
        if skip_existing and output_path.exists():
            print(f"[{i}/{len(pdf_list)}] SKIP (exists): {pdf_filename[:50]}...")
            stats["skipped"] += 1
            # Track skipped PDF in stats
            pdf_stats.append({
                "pdf_name": Path(pdf_filename).stem,
                "status": "skipped",
                "pages_skipped": 0  # We don't know page count for skipped
            })
            continue

        if not pdf_path.exists():
            error_msg = f"PDF not found: {pdf_path}"
            print(f"[{i}/{len(pdf_list)}] ERROR: {error_msg}")
            stats["errors"].append((pdf_filename, error_msg))
            pdf_stats.append({
                "pdf_name": Path(pdf_filename).stem,
                "status": "error",
                "error": error_msg
            })
            continue

        try:
            # Get ignore pages from human_loop config
            pdf_stem = Path(pdf_filename).stem
            ignore_pages = []
            total_pages = None
            if human_loop_enabled and pdf_stem in human_loop_config:
                doc_config = human_loop_config[pdf_stem]
                ignore_pages = doc_config.get("ignore_pages", [])
                total_pages = doc_config.get("total_pages")
            
            ignore_info = f" (ignoring {len(ignore_pages)} pages)" if ignore_pages else ""
            print(f"[{i}/{len(pdf_list)}] Processing: {pdf_filename[:50]}...{ignore_info}", end=" ", flush=True)

            # Extract content (with ignore pages support)
            result = extract_pdf_to_json(pdf_path, client, ignore_pages=ignore_pages, total_pages=total_pages)

            # Add metadata
            result["_metadata"] = {
                "doc_id": pdf_info["doc_id"],
                "nacc_id": pdf_info["nacc_id"],
                "source_pdf": str(pdf_path),
                "phase": "phase0_ocr",
                "human_loop_enabled": human_loop_enabled,
                "ignored_pages": ignore_pages if ignore_pages else None
            }

            # Save JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            actual_pages_processed = len([p for p in result['pages'] if not p.get('_ignored')])
            print(f"OK ({len(result['pages'])} pages, {actual_pages_processed} OCR'd)")
            stats["processed"] += 1
            stats["pages_ignored"] += len(ignore_pages)
            
            # Track PDF stats for reporting
            pdf_stats.append({
                "pdf_name": pdf_stem,
                "status": "processed",
                "total_pages": len(result['pages']),
                "pages_processed": actual_pages_processed,
                "pages_ignored": len(ignore_pages),
                "ignored_page_numbers": ignore_pages if ignore_pages else None
            })

            # Rate limiting
            time.sleep(0.5)

        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: {error_msg[:50]}")
            stats["errors"].append((pdf_filename, error_msg))
            pdf_stats.append({
                "pdf_name": Path(pdf_filename).stem,
                "status": "error",
                "error": error_msg[:100]
            })

    # Generate and save run report
    run_report = generate_run_report(pdf_stats, is_final, human_loop_enabled)
    main_report = append_run_to_report(run_report, is_final)
    
    # Print cost report
    print_cost_report(run_report, main_report)

    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print(f"Processed: {stats['processed']}/{stats['total']}")
    print(f"Skipped: {stats['skipped']}")
    print(f"Errors: {len(stats['errors'])}")
    if human_loop_enabled:
        print(f"Pages ignored (cost saving): {stats['pages_ignored']}")

    if stats["errors"]:
        print("\nErrors:")
        for fname, err in stats["errors"][:5]:
            print(f"  - {fname[:40]}: {err[:50]}")
        if len(stats["errors"]) > 5:
            print(f"  ... and {len(stats['errors']) - 5} more")

    print(f"{'='*60}")

    return stats


def process_single_pdf(
    pdf_path: Path,
    output_dir: Path,
    skip_existing: bool = True
) -> Dict[str, Any]:
    """
    Process a single PDF file.

    Args:
        pdf_path: Path to the PDF file
        output_dir: Directory to save output JSON
        skip_existing: Skip if output already exists

    Returns:
        Dict with processing result
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    output_filename = pdf_path.stem + ".json"
    output_path = output_dir / output_filename

    if skip_existing and output_path.exists():
        print(f"SKIP (exists): {pdf_path.name}")
        return {'status': 'skipped', 'file': pdf_path.name}

    if not pdf_path.exists():
        print(f"ERROR: PDF not found: {pdf_path}")
        return {'status': 'error', 'file': pdf_path.name, 'error': 'PDF not found'}

    try:
        print(f"Processing: {pdf_path.name}...", end=" ", flush=True)

        client = get_azure_client()
        result = extract_pdf_to_json(pdf_path, client)

        # Add metadata
        result["_metadata"] = {
            "source_pdf": str(pdf_path),
            "phase": "phase0_ocr"
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"OK ({len(result['pages'])} pages)")
        return {'status': 'success', 'file': pdf_path.name, 'pages': len(result['pages'])}

    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return {'status': 'error', 'file': pdf_path.name, 'error': str(e)}


def fix_errors(
    input_dir: Path,
    output_dir: Path,
    is_final: bool = False
) -> Dict[str, Any]:
    """
    Retry processing only files that failed or are missing.

    Args:
        input_dir: Base input directory
        output_dir: Directory with output JSON files
        is_final: If True, process test final data

    Returns:
        Dict with processing statistics
    """
    # Determine PDF directory
    if is_final:
        doc_info_csv = input_dir / "Test final_doc_info.csv"
        pdf_dir = input_dir / "Test final_pdf"
    else:
        doc_info_csv = input_dir / "Train_doc_info.csv"
        pdf_dir = input_dir / "Train_pdf" / "pdf"

    if not doc_info_csv.exists():
        raise FileNotFoundError(f"Doc info CSV not found: {doc_info_csv}")

    # Get PDF list
    pdf_list = get_pdf_list(input_dir, doc_info_csv)

    # Find files that need processing (missing output or PDF not found errors)
    files_to_process = []
    for pdf_info in pdf_list:
        pdf_filename = pdf_info["pdf_filename"]
        pdf_path = pdf_dir / pdf_filename
        output_filename = Path(pdf_filename).stem + ".json"
        output_path = output_dir / output_filename

        # Check if output exists
        if not output_path.exists():
            # Check if PDF exists
            if pdf_path.exists():
                files_to_process.append(pdf_info)
            else:
                print(f"WARN: PDF still not found: {pdf_filename}")

    if not files_to_process:
        print("No files to fix. All PDFs processed successfully!")
        return {'total': 0, 'processed': 0, 'errors': []}

    print(f"{'='*60}")
    print(f"Phase 0: Fix Errors (Retry Failed Files)")
    print(f"{'='*60}")
    print(f"Mode: {'Test Final' if is_final else 'Training'}")
    print(f"Files to retry: {len(files_to_process)}")
    print(f"{'='*60}")

    client = get_azure_client()

    stats = {
        'total': len(files_to_process),
        'processed': 0,
        'errors': []
    }

    for i, pdf_info in enumerate(files_to_process, 1):
        pdf_filename = pdf_info["pdf_filename"]
        pdf_path = pdf_dir / pdf_filename
        output_filename = Path(pdf_filename).stem + ".json"
        output_path = output_dir / output_filename

        try:
            print(f"[{i}/{len(files_to_process)}] Retrying: {pdf_filename[:50]}...", end=" ", flush=True)

            result = extract_pdf_to_json(pdf_path, client)

            result["_metadata"] = {
                "doc_id": pdf_info["doc_id"],
                "nacc_id": pdf_info["nacc_id"],
                "source_pdf": str(pdf_path),
                "phase": "phase0_ocr"
            }

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)

            print(f"OK ({len(result['pages'])} pages)")
            stats['processed'] += 1

            time.sleep(0.5)

        except Exception as e:
            error_msg = str(e)
            print(f"ERROR: {error_msg[:50]}")
            stats['errors'].append((pdf_filename, error_msg))

    print(f"\n{'='*60}")
    print("Fix Summary")
    print(f"{'='*60}")
    print(f"Retried: {stats['processed']}/{stats['total']}")
    print(f"Errors: {len(stats['errors'])}")
    print(f"{'='*60}")

    return stats


def main():
    """Main entry point for Phase 0."""
    parser = argparse.ArgumentParser(
        description="Phase 0: Extract text from PDFs using Azure Document Intelligence"
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="Process test final data instead of training data"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of PDFs to process (for testing)"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip existing files, reprocess all"
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Retry only failed/missing files"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Process a single PDF file (path to PDF)"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Override input directory"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory"
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Show OCR usage report without running processing"
    )
    parser.add_argument(
        "--report-clear",
        action="store_true",
        help="Clear/reset the OCR usage report"
    )

    args = parser.parse_args()
    
    # Report mode - show usage report
    if args.report:
        show_usage_report(args.final)
        return
    
    # Clear report mode
    if args.report_clear:
        clear_usage_report(args.final)
        return

    # Determine base paths
    src_dir = Path(__file__).parent.parent

    # Single file mode
    if args.file:
        pdf_path = Path(args.file)
        if not pdf_path.is_absolute():
            pdf_path = Path.cwd() / pdf_path

        if args.output_dir:
            output_dir = Path(args.output_dir)
        elif args.final:
            output_dir = src_dir / "result" / "final" / "processing_input" / "extract_raw"
        else:
            output_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_raw"

        process_single_pdf(pdf_path, output_dir, skip_existing=not args.no_skip)
        return

    # Determine input/output directories
    if args.input_dir:
        input_dir = Path(args.input_dir)
    elif args.final:
        input_dir = src_dir / "test final" / "test final input"
    else:
        input_dir = src_dir / "training" / "train input"

    if args.output_dir:
        output_dir = Path(args.output_dir)
    elif args.final:
        output_dir = src_dir / "result" / "final" / "processing_input" / "extract_raw"
    else:
        output_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_raw"

    # Fix mode - retry only errors
    if args.fix:
        fix_errors(
            input_dir=input_dir,
            output_dir=output_dir,
            is_final=args.final
        )
        return

    # Normal processing
    process_pdfs(
        input_dir=input_dir,
        output_dir=output_dir,
        is_final=args.final,
        limit=args.limit,
        skip_existing=not args.no_skip
    )


if __name__ == "__main__":
    main()
