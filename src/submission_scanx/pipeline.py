"""
Main Pipeline Runner for Submission ScanX

This module provides a unified CLI to run the complete extraction pipeline:
- Phase 0: PDF to OCR using Azure Document Intelligence
- Phase 1: JSON processing and page matching
  - 1a+1b: Page type identification and matching -> page_matched/
  - 1c: Text extraction -> text_each_page/
  - 1d: Page metadata (AI) -> page_metadata/
  - 1e: Data mapping to CSV -> mapping_output/
- Phase 2: LLM-based data correction
  - 2a: Fix submitter_position.csv OCR errors

Usage:
  poetry run scanx --phase 1 --all           # Run all Phase 1 (clean + 1a-1e)
  poetry run scanx --phase 1 --final --all   # Run all Phase 1 on final data
  poetry run scanx --phase 1e                # Run only Phase 1e
  poetry run scanx --phase 2a                # Run Phase 2a (position fix)
  poetry run scanx --phase 2a --final        # Run Phase 2a on final data
"""

import argparse
import shutil
from pathlib import Path
from typing import Optional


def get_paths(is_final: bool = False) -> dict:
    """Get standard paths for pipeline processing."""
    src_dir = Path(__file__).parent.parent

    if is_final:
        base = src_dir / "result" / "final"
        input_csv = src_dir / "test final" / "test final input"
    else:
        base = src_dir / "result" / "from_train"
        input_csv = src_dir / "training" / "train input"

    return {
        'src_dir': src_dir,
        'base': base,
        'input_csv': input_csv,
        'extract_raw': base / "processing_input" / "extract_raw",
        'page_matched': base / "processing_input" / "extract_matched",  # Consistent with phase1_process.py
        'text_each_page': base / "processing_input" / "text_each_page",
        'page_metadata': base / "processing_input" / "page_metadata",
        'mapping_output': base / "mapping_output",
        'utils_dir': src_dir / "utils",
        # Phase 2 paths
        'phase2_output': base / "output_phase_2",
        'phase2_report': base / "output_phase_2" / "report",
    }


def clean_output_folders(paths: dict, phases: list = None):
    """Clean output folders before processing."""
    if phases is None:
        phases = ['page_matched', 'text_each_page', 'page_metadata', 'mapping_output']

    for phase in phases:
        folder = paths.get(phase)
        if folder and folder.exists():
            print(f"  Cleaning {folder.name}/...")
            shutil.rmtree(folder)
        if folder:
            folder.mkdir(parents=True, exist_ok=True)


def run_phase0(
    is_final: bool = False,
    limit: Optional[int] = None,
    skip_existing: bool = True,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None
):
    """Run Phase 0: PDF to OCR extraction."""
    from .phase0_ocr import process_pdfs

    paths = get_paths(is_final)

    if input_dir is None:
        if is_final:
            input_dir = paths['src_dir'] / "test final" / "test final input"
        else:
            input_dir = paths['src_dir'] / "training" / "train input"

    if output_dir is None:
        output_dir = paths['extract_raw']

    return process_pdfs(
        input_dir=input_dir,
        output_dir=output_dir,
        is_final=is_final,
        limit=limit,
        skip_existing=skip_existing
    )


def run_phase1ab(
    is_final: bool = False,
    skip_existing: bool = True,
    clean: bool = False,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    template_path: Optional[Path] = None
):
    """Run Phase 1a+1b: Page type identification and matching."""
    from .phase1_process import run_phase1 as run_full_phase1

    paths = get_paths(is_final)

    if template_path is None:
        template_path = paths['utils_dir'] / "template-docs_raw.json"

    if input_dir is None:
        input_dir = paths['extract_raw']

    if output_dir is None:
        output_dir = paths['page_matched']

    print("\n" + "="*60)
    print("PHASE 1a+1b: Page Type ID + Matching")
    print("="*60)
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}\n")

    return run_full_phase1(input_dir, output_dir, template_path, is_final, skip_existing, clean=clean)


def run_phase1c(
    is_final: bool = False,
    skip_existing: bool = True,
    clean: bool = False,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None
):
    """Run Phase 1c: Text extraction from matched pages."""
    from .phase1_process import process_phase1c

    paths = get_paths(is_final)

    if input_dir is None:
        input_dir = paths['page_matched']

    if output_dir is None:
        output_dir = paths['text_each_page']

    print("\n" + "="*60)
    print("PHASE 1c: Text Extraction")
    print("="*60)
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}\n")

    return process_phase1c(input_dir, output_dir, clean=clean, skip_existing=skip_existing)


def run_phase1d(
    is_final: bool = False,
    skip_existing: bool = True,
    clean: bool = False,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None
):
    """Run Phase 1d: Page metadata extraction (AI-based)."""
    from .phase1d_metadata import process_phase1d

    paths = get_paths(is_final)

    if input_dir is None:
        input_dir = paths['page_matched']

    if output_dir is None:
        output_dir = paths['page_metadata']

    print("\n" + "="*60)
    print("PHASE 1d: Page Metadata Mapping (AI)")
    print("="*60)
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}\n")

    return process_phase1d(input_dir, output_dir, skip_existing=skip_existing, clean=clean)


def run_phase1e(
    is_final: bool = False,
    skip_existing: bool = False,
    clean: bool = False,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    csv_dir: Optional[Path] = None
):
    """Run Phase 1e: Data mapping and extraction to CSV."""
    from .phase1e_mapping import process_phase1e

    paths = get_paths(is_final)

    if input_dir is None:
        input_dir = paths['page_matched']

    if output_dir is None:
        output_dir = paths['mapping_output']

    if csv_dir is None:
        csv_dir = paths['input_csv']

    print("\n" + "="*60)
    print("PHASE 1e: Data Mapping and Extraction")
    print("="*60)
    print(f"Input JSON: {input_dir}")
    print(f"Input CSV: {csv_dir}")
    print(f"Output: {output_dir}\n")

    return process_phase1e(
        input_dir=input_dir,
        output_dir=output_dir,
        csv_dir=csv_dir,
        skip_existing=skip_existing,
        clean=clean
    )


def run_phase2a(
    is_final: bool = False,
    input_csv: Optional[Path] = None,
    output_csv: Optional[Path] = None,
    batch_size: int = 10
):
    """Run Phase 2a: LLM-based position data correction."""
    from .phase2_subtask.phase2a_position_fix import run_phase2a as run_phase2a_fix

    paths = get_paths(is_final)

    if input_csv is None:
        input_csv = paths['mapping_output'] / "submitter_position.csv"

    if output_csv is None:
        output_csv = input_csv  # Overwrite by default

    report_dir = paths['phase2_report']

    print("\n" + "=" * 60)
    print("PHASE 2a: LLM Position Data Correction")
    print("=" * 60)
    print(f"Input: {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Report: {report_dir}\n")

    return run_phase2a_fix(
        input_csv=input_csv,
        output_csv=output_csv,
        report_dir=report_dir,
        batch_size=batch_size
    )


def run_phase2b(
    is_final: bool = False,
    batch_size: int = 5
):
    """Run Phase 2b: LLM-based statement data correction."""
    from .phase2_subtask.phase2b_statement_fix import run_phase2b as run_phase2b_fix

    paths = get_paths(is_final)

    # Determine CSV input directory
    src_dir = Path(__file__).parent.parent
    if is_final:
        csv_dir = src_dir / "test final" / "test final input"
    else:
        csv_dir = src_dir / "training" / "train input"

    statement_csv = paths['mapping_output'] / "statement.csv"
    statement_detail_csv = paths['mapping_output'] / "statement_detail.csv"
    text_dir = paths['base'] / "processing_input" / "text_each_page"
    metadata_dir = paths['base'] / "processing_input" / "page_metadata"
    report_dir = paths['phase2_report']

    print("\n" + "=" * 60)
    print("PHASE 2b: LLM Statement Data Correction")
    print("=" * 60)
    print(f"Statement: {statement_csv}")
    print(f"Statement Detail: {statement_detail_csv}")
    print(f"Text Dir: {text_dir}")
    print(f"Metadata Dir: {metadata_dir}")
    print(f"Report: {report_dir}\n")

    return run_phase2b_fix(
        statement_csv=statement_csv,
        statement_detail_csv=statement_detail_csv,
        text_dir=text_dir,
        metadata_dir=metadata_dir,
        csv_dir=csv_dir,
        report_dir=report_dir,
        batch_size=batch_size
    )


def run_phase1_all(is_final: bool = False):
    """Run complete Phase 1 pipeline (1a through 1e) with clean output."""
    paths = get_paths(is_final)

    print("="*70)
    print("PHASE 1 COMPLETE: Running all sub-phases (1a-1e)")
    print("="*70)
    print(f"Mode: {'Test Final' if is_final else 'Training'}")
    print("="*70)

    # Clean all output folders first
    print("\n[Clean] Removing existing output folders...")
    clean_output_folders(paths)

    results = {}

    # Phase 1a+1b: Page matching
    results['phase1ab'] = run_phase1ab(is_final=is_final, skip_existing=False, clean=False)

    # Phase 1c: Text extraction
    results['phase1c'] = run_phase1c(is_final=is_final, skip_existing=False, clean=False)

    # Phase 1d: Page metadata (AI)
    results['phase1d'] = run_phase1d(is_final=is_final, skip_existing=False, clean=False)

    # Phase 1e: Data mapping
    results['phase1e'] = run_phase1e(is_final=is_final, skip_existing=False, clean=False)

    print("\n" + "="*70)
    print("PHASE 1 COMPLETE: All sub-phases finished!")
    print("="*70)
    print(f"Output folders:")
    print(f"  - page_matched: {paths['page_matched']}")
    print(f"  - text_each_page: {paths['text_each_page']}")
    print(f"  - page_metadata: {paths['page_metadata']}")
    print(f"  - mapping_output: {paths['mapping_output']}")
    print("="*70)

    return results


def run_pipeline(
    phases: str = "all",
    is_final: bool = False,
    limit: Optional[int] = None,
    skip_existing: bool = False,
    run_all: bool = False,
    include_1d: bool = False,
    batch_size: int = 10
):
    """
    Run the complete pipeline or selected phases.

    Args:
        phases: Which phases to run ("0", "1", "1c", "1d", "1e", "2a", "all")
        is_final: Process test final data instead of training
        limit: Maximum number of PDFs to process (Phase 0)
        skip_existing: Skip already processed files
        run_all: Run complete Phase 1 (1a-1e) with clean
        include_1d: Also run Phase 1d when running phase 1
        batch_size: Batch size for Phase 2 LLM processing
    """
    print("="*70)
    print("Submission ScanX - Document Extraction Pipeline")
    print("="*70)
    print(f"Mode: {'Test Final' if is_final else 'Training'}")
    print(f"Phases: {phases}" + (" --all" if run_all else ""))
    print("="*70 + "\n")

    results = {}

    # Phase 0: OCR
    if phases in ("0", "all"):
        print("\n" + "="*70)
        print("PHASE 0: PDF to OCR Extraction")
        print("="*70 + "\n")
        results['phase0'] = run_phase0(
            is_final=is_final,
            limit=limit,
            skip_existing=skip_existing
        )

    # Phase 1 with --all flag: run complete pipeline
    if phases == "1" and run_all:
        results['phase1_all'] = run_phase1_all(is_final=is_final)

    # Phase 1 without --all: run 1a+1b+1c (default behavior)
    elif phases == "1" and not run_all:
        results['phase1ab'] = run_phase1ab(is_final=is_final, skip_existing=skip_existing)
        results['phase1c'] = run_phase1c(is_final=is_final, skip_existing=skip_existing)

        if include_1d:
            results['phase1d'] = run_phase1d(is_final=is_final, skip_existing=skip_existing)

    # Individual phase runs
    elif phases == "1c":
        results['phase1c'] = run_phase1c(is_final=is_final, skip_existing=skip_existing)

    elif phases == "1d":
        results['phase1d'] = run_phase1d(is_final=is_final, skip_existing=skip_existing)

    elif phases == "1e":
        results['phase1e'] = run_phase1e(is_final=is_final, skip_existing=skip_existing)

    # Phase 2a: LLM position fix
    elif phases == "2a":
        results['phase2a'] = run_phase2a(is_final=is_final, batch_size=batch_size)

    # Phase 2b: LLM statement fix
    elif phases == "2b":
        results['phase2b'] = run_phase2b(is_final=is_final, batch_size=batch_size)

    # "all" phases (0 + 1 complete)
    elif phases == "all":
        results['phase1_all'] = run_phase1_all(is_final=is_final)

    print("\n" + "="*70)
    print("Pipeline Complete")
    print("="*70)

    return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Submission ScanX - Document Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run Phase 1 complete (clean + 1a-1e) on training data
  poetry run scanx --phase 1 --all

  # Run Phase 1 complete on final test data
  poetry run scanx --phase 1 --final --all

  # Run only Phase 1e (data mapping)
  poetry run scanx --phase 1e

  # Run Phase 1d only (AI metadata)
  poetry run scanx --phase 1d

  # Run Phase 0 (OCR extraction)
  poetry run scanx --phase 0

  # Run complete pipeline (Phase 0 + Phase 1 all)
  poetry run scanx

  # Process only first 5 PDFs
  poetry run scanx --phase 0 --limit 5

  # Run Phase 2a (LLM position fix) on training data
  poetry run scanx --phase 2a

  # Run Phase 2a on final test data
  poetry run scanx --phase 2a --final

  # Run Phase 2b (LLM statement fix) on training data
  poetry run scanx --phase 2b

  # Run Phase 2b on final test data
  poetry run scanx --phase 2b --final
        """
    )

    parser.add_argument(
        "--phase",
        choices=["0", "1", "1c", "1d", "1e", "2a", "2b", "all"],
        default="all",
        help="Phase to run (0=OCR, 1=page processing, 1c=text, 1d=AI metadata, 1e=data mapping, 2a=LLM position fix, 2b=LLM statement fix)"
    )
    parser.add_argument(
        "--all",
        dest="run_all",
        action="store_true",
        help="Run complete Phase 1 (1a-1e) with clean output folders"
    )
    parser.add_argument(
        "--1d",
        dest="include_1d",
        action="store_true",
        help="Also run Phase 1d when running --phase 1 (without --all)"
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
        help="Maximum number of PDFs to process in Phase 0"
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="Skip existing files (default: always regenerate)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for Phase 2 LLM processing (default: 10)"
    )

    args = parser.parse_args()

    run_pipeline(
        phases=args.phase,
        is_final=args.final,
        limit=args.limit,
        skip_existing=args.skip,
        run_all=args.run_all,
        include_1d=args.include_1d,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
