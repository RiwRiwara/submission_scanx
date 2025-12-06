"""
Phase 1e: Data Mapping and Extraction Pipeline

This phase extracts structured data from processed JSON documents and generates
CSV output files. Uses the step-based pipeline from phase1_mapping/.

Pipeline steps:
- Step 1: submitter_position.csv - Submitter positions (current and historical)
- Step 2: submitter_old_name.csv - Submitter old/changed names
- Step 3: spouse_info.csv, spouse_old_name.csv, spouse_position.csv - Spouse data
- Step 4: relative_info.csv - Relatives (parents, children, siblings)
- Step 5: statement.csv, statement_detail.csv - Income/expense/tax statements
- Step 6: asset.csv - Asset records summary
- Step 7: asset_land_info.csv - Land asset details
- Step 8: asset_building_info.csv - Building asset details
- Step 9: asset_vehicle_info.csv - Vehicle asset details
- Step 10: asset_other_asset_info.csv - Other asset details
- Step 11: summary.csv - Final aggregated summary

Input: extract_matched/*.json (JSON files with page metadata)
Output: mapping_output/*.csv (13 CSV files)
"""

import os
import sys
import time
from pathlib import Path
from typing import Optional

# Add src to path for utils imports
_src_dir = str(Path(__file__).parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.data_loader import PipelineDataLoader

# Import step functions from phase1_mapping
from .phase1_mapping.step_1 import run_step_1
from .phase1_mapping.step_2 import run_step_2
from .phase1_mapping.step_3 import run_step_3
from .phase1_mapping.step_4 import run_step_4
from .phase1_mapping.step_5 import run_step_5
from .phase1_mapping.step_6 import run_step_6
from .phase1_mapping.step_7 import run_step_7
from .phase1_mapping.step_8 import run_step_8
from .phase1_mapping.step_9 import run_step_9
from .phase1_mapping.step_10 import run_step_10
from .phase1_mapping.step_11 import run_step_11


def run_phase1e(
    input_dir: Path,
    output_dir: Path,
    csv_dir: Optional[Path] = None,
    skip_existing: bool = False,
    clean: bool = False
) -> dict:
    """
    Run Phase 1e: Data mapping and extraction.

    Args:
        input_dir: Directory containing extract_matched/*.json files
        output_dir: Base output directory for mapping results
        csv_dir: Directory containing input CSV files (doc_info, nacc_detail, submitter_info).
                 If None, uses input_dir's parent to find them.
        skip_existing: Skip if output files already exist
        clean: Remove existing output files before processing

    Returns:
        Dictionary with processing statistics
    """
    start_time = time.time()

    print("=" * 60)
    print("Phase 1e: Data Mapping Pipeline")
    print("=" * 60)
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")

    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clean if requested
    if clean:
        import glob
        for f in glob.glob(str(output_dir / "*.csv")):
            os.remove(f)
        print("  Cleaned existing output files")

    # Skip if output already exists and skip_existing is True
    summary_file = output_dir / "summary.csv"
    if skip_existing and summary_file.exists():
        print("  Output already exists, skipping...")
        return {'skipped': True}

    # Determine CSV input directory
    # Look for CSV files in csv_dir or standard locations
    if csv_dir is None:
        # Try to find CSV files in standard locations
        src_dir = Path(__file__).parent.parent

        # Check if we're in training or final mode based on input_dir
        if 'final' in str(input_dir).lower():
            csv_candidates = [
                input_dir.parent,  # Same parent as extract_matched
                src_dir / "test final" / "test final input",
                input_dir,
            ]
        else:
            csv_candidates = [
                input_dir.parent,  # Same parent as extract_matched
                src_dir / "training" / "train input",
                input_dir,
            ]

        csv_dir = None
        for candidate in csv_candidates:
            if candidate.exists():
                # Check for required CSV files
                csv_patterns = ['*doc_info.csv', '*nacc_detail.csv', '*submitter_info.csv']
                found_all = True
                for pattern in csv_patterns:
                    if not list(candidate.glob(pattern)):
                        found_all = False
                        break
                if found_all:
                    csv_dir = candidate
                    break

        if csv_dir is None:
            print("  Warning: Could not find CSV input files. Using input_dir.")
            csv_dir = input_dir

    print(f"CSV input: {csv_dir}")

    # Determine if we're in final test mode
    is_final_mode = 'final' in str(input_dir).lower() or 'final' in str(csv_dir).lower()

    # Create data loader with explicit is_final flag
    print("\n[Init] Loading input data...")
    data_loader = PipelineDataLoader(str(csv_dir), normalize_layout=True, is_final=is_final_mode)
    print(f"  - Mode: {'Test Final' if is_final_mode else 'Training'}")
    print(f"  - Found {len(data_loader.doc_info_list)} documents to process")

    # Update JSON directory to point to input_dir (extract_matched)
    data_loader._paths['json_dir'] = str(input_dir)

    # Run all steps
    stats = {'steps': {}}

    # Step 1: Extract submitter positions
    print("\n[Step 1] Extracting submitter positions...")
    step_start = time.time()
    run_step_1(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_1'] = time.time() - step_start

    # Step 2: Extract submitter old names
    print("\n[Step 2] Extracting submitter old names...")
    step_start = time.time()
    run_step_2(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_2'] = time.time() - step_start

    # Step 3: Extract spouse info, old names, and positions
    print("\n[Step 3] Extracting spouse information...")
    step_start = time.time()
    run_step_3(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_3'] = time.time() - step_start

    # Step 4: Extract relative information
    print("\n[Step 4] Extracting relative information...")
    step_start = time.time()
    run_step_4(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_4'] = time.time() - step_start

    # Step 5: Extract statement and statement_detail
    print("\n[Step 5] Extracting statement information...")
    step_start = time.time()
    run_step_5(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_5'] = time.time() - step_start

    # Step 6: Extract assets
    print("\n[Step 6] Extracting asset information...")
    step_start = time.time()
    run_step_6(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_6'] = time.time() - step_start

    # Step 7: Extract land asset detailed information
    print("\n[Step 7] Extracting land asset information...")
    step_start = time.time()
    run_step_7(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_7'] = time.time() - step_start

    # Step 8: Extract building asset detailed information
    print("\n[Step 8] Extracting building asset information...")
    step_start = time.time()
    run_step_8(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_8'] = time.time() - step_start

    # Step 9: Extract vehicle asset detailed information
    print("\n[Step 9] Extracting vehicle asset information...")
    step_start = time.time()
    run_step_9(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_9'] = time.time() - step_start

    # Step 10: Extract other asset detailed information
    print("\n[Step 10] Extracting other asset information...")
    step_start = time.time()
    run_step_10(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_10'] = time.time() - step_start

    # Step 11: Generate summary.csv
    print("\n[Step 11] Generating summary...")
    step_start = time.time()
    run_step_11(str(csv_dir), str(output_dir), data_loader)
    stats['steps']['step_11'] = time.time() - step_start

    elapsed = time.time() - start_time
    stats['total_time'] = elapsed

    print("\n" + "=" * 60)
    print("Phase 1e Complete!")
    print(f"Output saved to: {output_dir}")
    print(f"Total time: {elapsed:.2f} seconds")
    print("=" * 60)

    return stats


def process_phase1e(
    input_dir: Path,
    output_dir: Path,
    csv_dir: Optional[Path] = None,
    skip_existing: bool = False,
    clean: bool = False
) -> dict:
    """
    Alias for run_phase1e for consistency with other phase processors.
    """
    return run_phase1e(input_dir, output_dir, csv_dir, skip_existing, clean)


def main():
    """CLI entry point for phase1e_mapping."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Phase 1e: Data Mapping Pipeline',
        epilog="""
Examples:
  # Run with explicit paths
  poetry run scanx-mapping -i ./page_matched -o ./mapping_output --csv-dir ./train_input

  # Run on training data (auto-detect paths)
  poetry run scanx-mapping

  # Run on final test data
  poetry run scanx-mapping --final

  # Clean and reprocess
  poetry run scanx-mapping --clean --no-skip
        """
    )
    parser.add_argument('--input', '-i', type=str, default=None,
                        help='Input directory containing JSON files (default: auto-detect)')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output directory for CSV files (default: auto-detect)')
    parser.add_argument('--csv-dir', type=str, default=None,
                        help='Directory containing input CSV files (default: auto-detect)')
    parser.add_argument('--final', action='store_true',
                        help='Process final test data instead of training')
    parser.add_argument('--no-skip', action='store_true',
                        help='Do not skip existing output files')
    parser.add_argument('--clean', action='store_true',
                        help='Remove existing output files before processing')

    args = parser.parse_args()

    # Auto-detect paths if not provided
    src_dir = Path(__file__).parent.parent

    if args.final:
        base = src_dir / "result" / "final"
        default_csv = src_dir / "test final" / "test final input"
    else:
        base = src_dir / "result" / "from_train"
        default_csv = src_dir / "training" / "train input"

    input_dir = Path(args.input) if args.input else base / "processing_input" / "page_matched"
    output_dir = Path(args.output) if args.output else base / "mapping_output"
    csv_dir = Path(args.csv_dir) if args.csv_dir else default_csv

    run_phase1e(
        input_dir=input_dir,
        output_dir=output_dir,
        csv_dir=csv_dir,
        skip_existing=not args.no_skip,
        clean=args.clean
    )


if __name__ == '__main__':
    main()
