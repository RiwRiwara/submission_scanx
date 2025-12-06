"""
Step 2: Extract submitter_old_name.csv from JSON extract files

This step extracts old name information (ชื่อเดิม/ชื่อสกุลเดิม) for submitters from OCR JSON files.
The old name data is typically found on page 4 (personal info section) of the document.

Detection uses polygon-based spatial matching to find values near the labels.

Output columns:
- submitter_id, nacc_id, index, title, first_name, last_name,
  title_en, first_name_en, last_name_en, latest_submitted_date
"""

import os
import re
from typing import List, Dict, Optional, Tuple

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import (
    clean_ocr_text,
    get_polygon_center,
    get_polygon_bounds,
    is_in_x_range,
    is_y_close,
    is_valid_thai_name,
    extract_title_and_name,
    format_disclosure_date,
    Y_TOLERANCE,
)
from utils.data_loader import PipelineDataLoader, CSVWriter


# X position thresholds for old name detection
OLD_FIRST_NAME_LABEL_X_MIN = 1.0
OLD_FIRST_NAME_LABEL_X_MAX = 2.0
OLD_FIRST_NAME_VALUE_X_MIN = 1.6
OLD_FIRST_NAME_VALUE_X_MAX = 3.5

OLD_LAST_NAME_LABEL_X_MIN = 3.5
OLD_LAST_NAME_LABEL_X_MAX = 4.6
OLD_LAST_NAME_VALUE_X_MIN = 4.4
OLD_LAST_NAME_VALUE_X_MAX = 7.0


def extract_old_name_by_polygon(page_lines: List[Dict]) -> Dict[str, str]:
    """
    Extract old name information using polygon-based spatial detection.

    Strategy:
    1. First find the "ชื่อเดิม" and "ชื่อสกุลเดิม" labels and their y-positions
    2. Look for values to the right of each label at the same y-level
    3. Only consider text as a value if it's a valid Thai name
    """
    result = {
        'old_first_name': '',
        'old_last_name': ''
    }

    # Step 1: Find label positions
    old_first_name_label_y = None
    old_last_name_label_y = None

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])

        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Check if this is the old first name label
        if 'ชื่อเดิม' in content and 'ชื่อสกุลเดิม' not in content:
            if OLD_FIRST_NAME_LABEL_X_MIN <= cx <= OLD_FIRST_NAME_LABEL_X_MAX:
                old_first_name_label_y = cy
                # Check if value is in the same line (after the label)
                after_label = content.split('ชื่อเดิม')[-1].strip()
                after_label = re.sub(r'^[.\s:]+', '', after_label).strip()
                if is_valid_thai_name(after_label):
                    result['old_first_name'] = clean_ocr_text(after_label)

        # Check if this is the old last name label
        if 'ชื่อสกุลเดิม' in content or 'นามสกุลเดิม' in content:
            if OLD_LAST_NAME_LABEL_X_MIN <= cx <= OLD_LAST_NAME_LABEL_X_MAX:
                old_last_name_label_y = cy
                # Check if value is in the same line (after the label)
                pattern = r'(?:ชื่อสกุลเดิม|นามสกุลเดิม)'
                after_label = re.split(pattern, content)[-1].strip()
                after_label = re.sub(r'^[.\s:]+', '', after_label).strip()
                if is_valid_thai_name(after_label):
                    result['old_last_name'] = clean_ocr_text(after_label)

    # Step 2: If labels were found but no values in same line,
    # look for values at the same y-level
    if old_first_name_label_y is not None and not result['old_first_name']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Skip labels and lines not at the right y-level
            if 'ชื่อเดิม' in content or 'ชื่อสกุลเดิม' in content:
                continue

            # Check if in the value x-range and close to the label y-position
            if (OLD_FIRST_NAME_VALUE_X_MIN <= cx <= OLD_FIRST_NAME_VALUE_X_MAX and
                abs(cy - old_first_name_label_y) <= Y_TOLERANCE):
                if is_valid_thai_name(content):
                    result['old_first_name'] = clean_ocr_text(content)
                    break

    if old_last_name_label_y is not None and not result['old_last_name']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Skip labels and lines not at the right y-level
            if 'ชื่อเดิม' in content or 'ชื่อสกุลเดิม' in content:
                continue

            # Check if in the value x-range and close to the label y-position
            if (OLD_LAST_NAME_VALUE_X_MIN <= cx <= OLD_LAST_NAME_VALUE_X_MAX and
                abs(cy - old_last_name_label_y) <= Y_TOLERANCE):
                if is_valid_thai_name(content):
                    result['old_last_name'] = clean_ocr_text(content)
                    break

    return result


def extract_submitter_old_name(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict
) -> List[Dict]:
    """Extract submitter old name from JSON content using polygon-based detection"""

    old_names = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    # Parse disclosure date for latest_submitted_date
    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    # Get current name from submitter_info
    current_title = submitter_info.get('title', '')
    current_first_name = submitter_info.get('first_name', '')
    current_last_name = submitter_info.get('last_name', '')

    # Look for old name in personal info page (flexibly find by page_type)
    pages = json_content.get('pages', [])

    old_name_data = None

    # Find personal_info pages using _page_info metadata
    personal_pages = []
    for idx, page in enumerate(pages):
        page_info = page.get('_page_info', {})
        if page_info.get('page_type') == 'personal_info':
            # Check if it's submitter's page (not spouse's page)
            lines = page.get('lines', [])
            header_text = ' '.join(l.get('content', '') for l in lines[:20])
            if 'หน้า 2' not in header_text and 'คู่สมรส' not in header_text:
                personal_pages.append(idx)
    
    # Fallback: use index 3 if no personal_info page found
    if not personal_pages and len(pages) > 3:
        personal_pages = [3]
    
    # Extract from first personal info page
    for target_page_index in personal_pages[:1]:
        page = pages[target_page_index]
        page_lines = page.get('lines', [])

        # Use polygon-based extraction
        old_name_data = extract_old_name_by_polygon(page_lines)

    # If we found old name data, create record
    if old_name_data and (old_name_data.get('old_first_name') or old_name_data.get('old_last_name')):
        old_first_name = old_name_data.get('old_first_name', '')
        old_last_name = old_name_data.get('old_last_name', '')

        # If only old_last_name is found, the first name stays the same
        if old_last_name and not old_first_name:
            old_first_name = current_first_name

        old_names.append({
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'index': 1,
            'title': current_title,
            'first_name': old_first_name or current_first_name,
            'last_name': old_last_name or current_last_name,
            'title_en': '',
            'first_name_en': '',
            'last_name_en': '',
            'latest_submitted_date': latest_submitted_date
        })

    return old_names


def run_step_2(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 2 to extract submitter old names.
    
    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    # Use shared data loader if provided, otherwise create new one
    loader = data_loader or PipelineDataLoader(input_dir)

    all_old_names = []

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']

        old_names = extract_submitter_old_name(
            json_content, nacc_detail, submitter_info
        )
        all_old_names.extend(old_names)

    # Write output using CSVWriter
    fieldnames = [
        'submitter_id', 'nacc_id', 'index', 'title', 'first_name', 'last_name',
        'title_en', 'first_name_en', 'last_name_en', 'latest_submitted_date'
    ]

    writer = CSVWriter(output_dir, 'submitter_old_name.csv', fieldnames)
    count = writer.write_rows(all_old_names)

    print(f"Extracted {count} old names to {writer.output_path}")
    return all_old_names


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_2(input_dir, output_dir)
