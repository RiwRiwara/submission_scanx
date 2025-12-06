"""
Step 3: Extract spouse information from JSON extract files (Orchestrator)

This step orchestrates the extraction of spouse data using sub-steps:
- step_3_1: spouse_info.csv - Basic spouse information
- step_3_2: spouse_old_name.csv - Spouse old name history
- step_3_3: spouse_position.csv - Spouse current positions

The spouse data is typically found on page 5 (คู่สมรส page) of the document.
"""

import os
from typing import Dict, List, Tuple

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.data_loader import PipelineDataLoader

# Import sub-steps
from .step_3_1 import extract_spouse_info, run_step_3_1
from .step_3_2 import extract_spouse_old_name, run_step_3_2
from .step_3_3 import extract_spouse_position, run_step_3_3
# find_spouse_page is available via sub-steps if needed externally
# from step_3_common import find_spouse_page


def extract_spouse_data(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Extract all spouse data (info, old names, positions) from JSON content.

    This is a convenience function that calls all three sub-steps and returns
    combined results. Used when you want to process a single document.

    Args:
        json_content: Parsed JSON content from OCR
        nacc_detail: NACC detail record
        submitter_info: Submitter info record

    Returns:
        Tuple of (spouse_infos, spouse_old_names, spouse_positions)
    """
    # Extract spouse info first to get the spouse_info dict
    spouse_infos = extract_spouse_info(json_content, nacc_detail, submitter_info)

    # Get spouse_info for old_name extraction (to get title)
    spouse_info_dict = None
    if spouse_infos:
        spouse_info_dict = spouse_infos[0]

    # Extract old names (needs spouse_info for title lookup)
    spouse_old_names = extract_spouse_old_name(
        json_content, nacc_detail, submitter_info, spouse_info_dict
    )

    # Extract positions
    spouse_positions = extract_spouse_position(json_content, nacc_detail, submitter_info)

    return spouse_infos, spouse_old_names, spouse_positions


def run_step_3(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 3 to extract all spouse information.

    This orchestrator runs all three sub-steps in sequence:
    1. step_3_1: Extract spouse_info.csv
    2. step_3_2: Extract spouse_old_name.csv
    3. step_3_3: Extract spouse_position.csv

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    print("=" * 60)
    print("Step 3: Extracting spouse information")
    print("=" * 60)

    # Create shared data loader if not provided
    loader = data_loader or PipelineDataLoader(input_dir)

    # Run step 3.1: spouse_info
    print("\n--- Step 3.1: Extracting spouse_info ---")
    spouse_infos = run_step_3_1(input_dir, output_dir, loader)

    # Build spouse_info map for step 3.2 (to get titles)
    spouse_info_map = {}
    for info in spouse_infos:
        nacc_id = info.get('nacc_id')
        if nacc_id:
            spouse_info_map[nacc_id] = info

    # Run step 3.2: spouse_old_name
    print("\n--- Step 3.2: Extracting spouse_old_name ---")
    run_step_3_2(input_dir, output_dir, loader, spouse_info_map)

    # Run step 3.3: spouse_position
    print("\n--- Step 3.3: Extracting spouse_position ---")
    run_step_3_3(input_dir, output_dir, loader)

    print("\n" + "=" * 60)
    print("Step 3 completed")
    print("=" * 60)

    return spouse_infos


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_3(input_dir, output_dir)
