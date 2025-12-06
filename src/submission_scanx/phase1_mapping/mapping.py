"""
Main pipeline runner for Asset Declaration OCR Pipeline

Usage:
    python main.py           # Run with training data
    python main.py --final   # Run with final test data

Optimization Notes:
- Uses shared PipelineDataLoader for centralized data loading with caching
- JSON files are loaded once and reused across all steps
- CSV input files are loaded once and shared across steps
"""

import os
import sys
import argparse
import time
from pathlib import Path

# Add src directory to path for utils imports
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.data_loader import PipelineDataLoader
from .step_1 import run_step_1
from .step_2 import run_step_2
from .step_3 import run_step_3
from .step_4 import run_step_4
from .step_5 import run_step_5
from .step_6 import run_step_6
from .step_7 import run_step_7
from .step_8 import run_step_8
from .step_9 import run_step_9
from .step_10 import run_step_10
from .step_11 import run_step_11


def run_pipeline(input_dir: str, output_dir: str, mode: str = "training"):
    """
    Run the full pipeline with specified input/output directories.
    
    Uses a shared PipelineDataLoader for optimal caching of JSON and CSV files.
    """
    start_time = time.time()
    
    print("=" * 60)
    print("Asset Declaration OCR Pipeline (Optimized)")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    
    # Create shared data loader for caching
    print("\n[Init] Loading input data...")
    data_loader = PipelineDataLoader(input_dir)
    print(f"  - Found {len(data_loader.doc_info_list)} documents to process")

    # Step 1: Extract submitter positions
    print("\n[Step 1] Extracting submitter positions...")
    run_step_1(input_dir, output_dir, data_loader)

    # Step 2: Extract submitter old names
    print("\n[Step 2] Extracting submitter old names...")
    run_step_2(input_dir, output_dir, data_loader)

    # Step 3: Extract spouse info, old names, and positions
    print("\n[Step 3] Extracting spouse information...")
    run_step_3(input_dir, output_dir, data_loader)

    # Step 4: Extract relative information
    print("\n[Step 4] Extracting relative information...")
    run_step_4(input_dir, output_dir, data_loader)

    # Step 5: Extract statement and statement_detail
    print("\n[Step 5] Extracting statement information...")
    run_step_5(input_dir, output_dir, data_loader)

    # Step 6: Extract assets
    print("\n[Step 6] Extracting asset information...")
    run_step_6(input_dir, output_dir, data_loader)

    # Step 7: Extract land asset detailed information
    print("\n[Step 7] Extracting land asset information...")
    run_step_7(input_dir, output_dir, data_loader)

    # Step 8: Extract building asset detailed information
    print("\n[Step 8] Extracting building asset information...")
    run_step_8(input_dir, output_dir, data_loader)

    # Step 9: Extract vehicle asset detailed information
    print("\n[Step 9] Extracting vehicle asset information...")
    run_step_9(input_dir, output_dir, data_loader)

    # Step 10: Extract other asset detailed information
    print("\n[Step 10] Extracting other asset information...")
    run_step_10(input_dir, output_dir, data_loader)

    # Step 11: Generate summary.csv
    print("\n[Step 11] Generating summary...")
    run_step_11(input_dir, output_dir, data_loader)

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("Pipeline completed!")
    print(f"Output saved to: {output_dir}")
    print(f"Total time: {elapsed:.2f} seconds")
    print("=" * 60)


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(description='Asset Declaration OCR Pipeline')
    parser.add_argument('--final', action='store_true',
                        help='Run with final test data (final_test folder)')
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.dirname(__file__))

    if args.final:
        # Final test mode
        input_dir = os.path.join(base_dir, 'final_test')
        output_dir = os.path.join(base_dir, 'final_test', 'output')
        mode = "final_test"
    else:
        # Training mode (default)
        input_dir = os.path.join(base_dir, 'pipeline_input')
        output_dir = os.path.join(base_dir, 'pipeline_output')
        mode = "training"

    # Create output directory if not exists
    os.makedirs(output_dir, exist_ok=True)

    run_pipeline(input_dir, output_dir, mode)


if __name__ == '__main__':
    main()
