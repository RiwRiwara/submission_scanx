"""
Main Pipeline Runner for Submission ScanX

This module provides a unified CLI to run the complete extraction pipeline:
- Phase 0: PDF to OCR using Azure Document Intelligence
- Phase 1: JSON processing and page matching
"""

import argparse
from pathlib import Path
from typing import Optional


def run_phase0(
    is_final: bool = False,
    limit: Optional[int] = None,
    skip_existing: bool = True,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None
):
    """Run Phase 0: PDF to OCR extraction."""
    from .phase0_ocr import process_pdfs

    src_dir = Path(__file__).parent.parent

    if input_dir is None:
        if is_final:
            input_dir = src_dir / "test final" / "test final input"
        else:
            input_dir = src_dir / "training" / "train input"

    if output_dir is None:
        if is_final:
            output_dir = src_dir / "result" / "final" / "processing_input" / "extract_raw"
        else:
            output_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_raw"

    return process_pdfs(
        input_dir=input_dir,
        output_dir=output_dir,
        is_final=is_final,
        limit=limit,
        skip_existing=skip_existing
    )


def run_phase1(
    is_final: bool = False,
    phase: str = "all",
    skip_existing: bool = True,
    input_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    template_path: Optional[Path] = None
):
    """Run Phase 1: JSON processing and page matching."""
    from .phase1_process import (
        process_phase1a,
        process_phase1b,
        process_phase1c,
        run_phase1 as run_full_phase1
    )
    from .phase1d_metadata import process_phase1d

    src_dir = Path(__file__).parent.parent
    utils_dir = src_dir / "utils"

    if template_path is None:
        template_path = utils_dir / "template-docs_raw.json"

    # Phase 1c only: text_each_page
    if phase == "1c":
        if input_dir is None:
            if is_final:
                input_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
            else:
                input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

        if output_dir is None:
            if is_final:
                output_dir = src_dir / "result" / "final" / "processing_input" / "text_each_page"
            else:
                output_dir = src_dir / "result" / "from_train" / "processing_input" / "text_each_page"

        return process_phase1c(input_dir, output_dir, clean=not skip_existing, skip_existing=skip_existing)

    # Phase 1d only: page_metadata
    if phase == "1d":
        if input_dir is None:
            if is_final:
                input_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
            else:
                input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

        if output_dir is None:
            if is_final:
                output_dir = src_dir / "result" / "final" / "processing_input" / "page_metadata"
            else:
                output_dir = src_dir / "result" / "from_train" / "processing_input" / "page_metadata"

        return process_phase1d(input_dir, output_dir, skip_existing=False, clean=True)

    # Phases 1a, 1b
    if input_dir is None:
        if is_final:
            input_dir = src_dir / "result" / "final" / "processing_input" / "extract_raw"
        else:
            input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_raw"

    if output_dir is None:
        if is_final:
            output_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
        else:
            output_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

    if phase == "1a":
        return process_phase1a(input_dir, output_dir, skip_existing)
    elif phase == "1b":
        return process_phase1b(input_dir, output_dir, template_path, skip_existing=skip_existing)
    else:
        # Run phases: 1a + 1b + 1c (not 1d by default)
        results = {}

        # Phase 1a + 1b
        results['phase1ab'] = run_full_phase1(input_dir, output_dir, template_path, is_final, skip_existing)

        # Phase 1c: text_each_page (reads from extract_matched)
        if is_final:
            text_input = src_dir / "result" / "final" / "processing_input" / "extract_matched"
            text_output = src_dir / "result" / "final" / "processing_input" / "text_each_page"
        else:
            text_input = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"
            text_output = src_dir / "result" / "from_train" / "processing_input" / "text_each_page"

        print("\n" + "="*60)
        print("PHASE 1c: Text Extraction")
        print("="*60 + "\n")
        results['phase1c'] = process_phase1c(text_input, text_output, clean=not skip_existing, skip_existing=skip_existing)

        return results


def run_pipeline(
    phases: str = "all",
    is_final: bool = False,
    limit: Optional[int] = None,
    skip_existing: bool = True,
    include_1d: bool = False
):
    """
    Run the complete pipeline or selected phases.

    Args:
        phases: Which phases to run ("0", "1", "1a", "1b", "1c", "1d", "all")
        is_final: Process test final data instead of training
        limit: Maximum number of PDFs to process (Phase 0)
        skip_existing: Skip already processed files
        include_1d: Also run Phase 1d (metadata mapping with AI)
    """
    from .phase1d_metadata import process_phase1d

    print("="*70)
    print("Submission ScanX - Document Extraction Pipeline")
    print("="*70)
    print(f"Mode: {'Test Final' if is_final else 'Training'}")
    print(f"Phases: {phases}" + (" + 1d" if include_1d and phases == "1" else ""))
    print("="*70 + "\n")

    results = {}
    src_dir = Path(__file__).parent.parent

    if phases in ("0", "all"):
        print("\n" + "="*70)
        print("PHASE 0: PDF to OCR Extraction")
        print("="*70 + "\n")
        results['phase0'] = run_phase0(
            is_final=is_final,
            limit=limit,
            skip_existing=skip_existing
        )

    if phases in ("1", "1a", "1b", "1c", "1d", "all"):
        phase_arg = phases if phases in ("1a", "1b", "1c", "1d") else "all"
        results['phase1'] = run_phase1(
            is_final=is_final,
            phase=phase_arg,
            skip_existing=skip_existing
        )

    # Run Phase 1d if --1d flag is set (only when running phase 1 or all)
    if include_1d and phases in ("1", "all"):
        if is_final:
            meta_input = src_dir / "result" / "final" / "processing_input" / "extract_matched"
            meta_output = src_dir / "result" / "final" / "processing_input" / "page_metadata"
        else:
            meta_input = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"
            meta_output = src_dir / "result" / "from_train" / "processing_input" / "page_metadata"

        print("\n" + "="*60)
        print("PHASE 1d: Page Metadata Mapping (AI)")
        print("="*60 + "\n")
        results['phase1d'] = process_phase1d(meta_input, meta_output, skip_existing=False, clean=True)

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
  # Run complete pipeline on training data
  python -m submission_scanx.pipeline

  # Run only Phase 0 (OCR extraction)
  python -m submission_scanx.pipeline --phase 0

  # Run only Phase 1 (1a + 1b + 1c)
  python -m submission_scanx.pipeline --phase 1

  # Run Phase 1 with AI metadata mapping (1a + 1b + 1c + 1d)
  python -m submission_scanx.pipeline --phase 1 --1d

  # Run Phase 1d only (AI metadata mapping)
  python -m submission_scanx.pipeline --phase 1d

  # Run on test final data
  python -m submission_scanx.pipeline --final

  # Process only first 5 PDFs (for testing)
  python -m submission_scanx.pipeline --phase 0 --limit 5

  # Reprocess all files (don't skip existing)
  python -m submission_scanx.pipeline --no-skip
        """
    )

    parser.add_argument(
        "--phase",
        choices=["0", "1", "1a", "1b", "1c", "1d", "all"],
        default="all",
        help="Phase to run (0=OCR, 1=1a+1b+1c, 1a=page ID, 1b=matching, 1c=text, 1d=AI metadata)"
    )
    parser.add_argument(
        "--1d",
        dest="include_1d",
        action="store_true",
        help="Also run Phase 1d (AI metadata mapping) when running phase 1"
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
        "--no-skip",
        action="store_true",
        help="Don't skip existing files, reprocess all"
    )

    args = parser.parse_args()

    run_pipeline(
        phases=args.phase,
        is_final=args.final,
        limit=args.limit,
        skip_existing=not args.no_skip,
        include_1d=args.include_1d
    )


if __name__ == "__main__":
    main()
