"""
Step 3.2: Extract spouse old name information from JSON extract files

This step extracts spouse_old_name.csv containing:
- Old first name (ชื่อเดิม)
- Old last name (ชื่อสกุลเดิม / นามสกุลเดิม)

The old name data is typically found around y=1.8-2.2 on the spouse page.
"""

import os
import re
from typing import List, Dict, Optional

import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import (
    clean_ocr_text,
    get_polygon_center,
    is_valid_thai_name,
    extract_title_and_name,
    format_disclosure_date,
    OCR_NOISE_PATTERNS,
)
from utils.data_loader import PipelineDataLoader, CSVWriter
from .step_3_common import find_spouse_page


def clean_extracted_name(name: str) -> str:
    """
    Post-process extracted name to remove OCR noise that might have slipped through.
    Returns cleaned name or empty string if the name is just noise.
    """
    if not name:
        return ''

    # Strip whitespace and common punctuation/quotes that OCR might add
    name = name.strip()
    name = name.strip('"\'`.,;:!?()[]{}•·')
    name = name.strip()

    # Check if the entire name matches noise patterns
    for pattern in OCR_NOISE_PATTERNS:
        if re.match(pattern, name):
            return ''

    # Specific patterns that indicate form labels/OCR noise (not names)
    noise_patterns_exact = [
        r'^น?เดือนปี.*$',           # นเดือนปี... or เดือนปี...
        r'^วันเดือนปี.*$',          # วันเดือนปี...
        r'^\d+\s*ปี.*$',            # 66 ปี...
        r'^ปี.*$',                   # ปี...
        r'^อายุ.*$',                 # อายุ...
        r'^สถานภาพ.*$',              # สถานภาพ...
        r'^ชื่อเดิม.*$',             # ชื่อเดิม...
        r'^ชื่อสกุลเดิม.*$',          # ชื่อสกุลเดิม...
        r'^นามสกุลเดิม.*$',           # นามสกุลเดิม...
        r'^[•·].*รายละเอียด.*$',      # • รายละเอียดอื่น ๆ... (footer text)
        r'^รายละเอียดอื่น.*$',        # รายละเอียดอื่น ๆ... (footer text)
        r'.*เลขประจำตัวประชาชน.*$',  # Contains เลขประจำตัวประชาชน (footer text)
        r'.*ผู้กู้ยืม.*$',           # Contains ผู้กู้ยืม (footer text)
        r'.*บัตรเครดิต.*$',          # Contains บัตรเครดิต (footer text)
        r'^คน$',                     # Just "คน"
        r'^อาย$',                    # Just "อาย" (partial "อายุ")
        r'^พ่อ$',                    # Just "พ่อ"
        r'^แม่$',                    # Just "แม่"
        r'^อาด$',                    # Just "อาด" (partial word)
        r'^สึก$',                    # Just "สึก" (partial word)
    ]

    for pattern in noise_patterns_exact:
        if re.match(pattern, name):
            return ''

    # Additional cleaning for last_name specific issues
    # Remove trailing patterns like "ปี", "เกิด :", numbers followed by ปี
    name = re.sub(r'\s*\d+\s*ปี\s*$', '', name)
    name = re.sub(r'\s*เกิด\s*:?\s*$', '', name)
    name = re.sub(r'\s*น?เดือนปี\s*เกิด\s*:?\s*$', '', name)
    name = re.sub(r'\s*วันเดือนปี\s*เกิด\s*:?\s*$', '', name)

    # If after cleaning nothing substantial remains, return empty
    cleaned = re.sub(r'[.\s:]+', '', name)
    if len(cleaned) < 2:
        return ''

    return name.strip()


def find_spouse_page_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> Optional[Dict]:
    """
    Find spouse page using page_metadata first, then fallback to content detection.

    Args:
        pages: List of page dictionaries from JSON
        loader: PipelineDataLoader instance for page_metadata lookup
        doc_location_url: Document location URL for metadata lookup

    Returns:
        Page dict if found, None otherwise

    Note: In page_metadata, step_2 is mapped to spouse info page.
    This is because the mapper has:
    - step_2 = "Spouse info" (not submitter_old_name as originally planned)
    """
    # Try page_metadata first - use step_2 (spouse info page in metadata)
    if loader and doc_location_url:
        # In page_metadata, step_2 is spouse info page
        spouse_pages = loader.get_step_pages(doc_location_url, 'step_2')
        if spouse_pages:
            # Get the first spouse page from metadata
            page_num = spouse_pages[0]
            page_idx = page_num - 1  # Convert 1-indexed to 0-indexed
            if 0 <= page_idx < len(pages):
                return pages[page_idx]

    # Fallback to content-based detection
    return find_spouse_page(pages)


# =============================================================================
# REGION CONSTANTS
# =============================================================================

# Spouse old name region - ชื่อเดิม and ชื่อสกุลเดิม fields
# These are typically around y=2.0-3.0 on the spouse page (page 2 of form)
# Based on actual data: ชื่อเดิม at y=2.22, ชื่อสกุลเดิม at y=2.21
OLD_NAME_Y_MIN = 1.8
OLD_NAME_Y_MAX = 3.2

# X ranges for old name fields
OLD_FIRST_NAME_X_MIN = 1.6
OLD_FIRST_NAME_X_MAX = 3.5
OLD_LAST_NAME_X_MIN = 4.4
OLD_LAST_NAME_X_MAX = 7.0

# Garbage text patterns that should never be extracted as names
GARBAGE_TEXT_PATTERNS = [
    r'.*รายละเอียดอื่น.*',          # Footer text
    r'.*เลขประจำตัวประชาชน.*',      # Footer text
    r'.*ผู้กู้ยืม.*',               # Footer text
    r'.*บัตรเครดิต.*',              # Footer text
    r'.*ภาพถ่าย.*',                 # Footer text
    r'^[•·].*',                      # Bullet point text
    r'^ลับ.*',                       # Confidential marker
    r'^หน้า\s*\d+.*',               # Page number
    r'^ลงชื่อ.*',                    # Signature line
    r'.*หมายเหตุ.*',                # Notes section
]


def is_garbage_text(text: str) -> bool:
    """Check if text is garbage/footer text that should never be a name."""
    if not text:
        return True
    text = text.strip()
    for pattern in GARBAGE_TEXT_PATTERNS:
        if re.match(pattern, text):
            return True
    return False


# =============================================================================
# EXTRACTION FUNCTIONS
# =============================================================================

def extract_spouse_old_name_from_page(page_lines: List[Dict]) -> Dict[str, str]:
    """
    Extract spouse old name using polygon-based detection.

    The old name section is typically around y=1.9-2.3 on spouse page:
    - "ชื่อเดิม" label followed by value (or dots if empty)
    - "ชื่อสกุลเดิม" / "นามสกุลเดิม" label followed by value (or dots if empty)

    IMPORTANT: Only extract if there's an actual name value after the label.
    If the field just has dots "......" it means no old name exists.
    """
    result = {
        'old_title': '',
        'old_first_name': '',
        'old_last_name': ''
    }

    # Find label positions and extract values
    old_first_name_label_y = None
    old_last_name_label_y = None
    old_first_name_label_x = None
    old_last_name_label_x = None

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])

        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Skip garbage text immediately
        if is_garbage_text(content):
            continue

        if not (OLD_NAME_Y_MIN <= cy <= OLD_NAME_Y_MAX):
            continue

        # Check for old first name label: "ชื่อเดิม"
        if 'ชื่อเดิม' in content and 'ชื่อสกุลเดิม' not in content and 'นามสกุลเดิม' not in content:
            old_first_name_label_y = cy
            old_first_name_label_x = cx
            # Check if value is in the same line after the label
            # Format can be: "ชื่อเดิม นางสาว มัลลิกา" or "ชื่อเดิม-" or "ชื่อเดิม ...."
            after_label = content.split('ชื่อเดิม')[-1].strip()
            # Remove leading dots, dashes and punctuation
            after_label = re.sub(r'^[-.\s:]+', '', after_label).strip()
            # Remove trailing dots
            after_label = re.sub(r'[-.\s:]+$', '', after_label).strip()
            # Remove title prefixes if present (นาย, นาง, นางสาว, etc.)
            # Handle OCR issues where นางสาว might be split as "นาง สาว" or "นาง" followed by "สาว"
            title_match = re.match(r'^(นางสาว|นาง\s*สาว|นาย|นาง|พ\.?ต\.?อ\.?|พันตำรวจเอก|ร\.?ต\.?|พ\.?อ\.?)\s*(.*)$', after_label)
            if title_match:
                title = title_match.group(1)
                # Normalize title
                if 'สาว' in title and 'นาง' in title:
                    title = 'นางสาว'
                result['old_title'] = title
                after_label = title_match.group(2).strip()
                # Handle case where first_name starts with "สาว" due to split
                if after_label.startswith('สาว'):
                    result['old_title'] = 'นางสาว'
                    after_label = after_label[3:].strip()  # Remove "สาว"
            # Only accept if it's a valid Thai name (not just dots or garbage)
            if after_label and is_valid_thai_name(after_label) and not is_garbage_text(after_label):
                result['old_first_name'] = clean_ocr_text(after_label)

        # Check for old last name label: "ชื่อสกุลเดิม" or "นามสกุลเดิม"
        if 'ชื่อสกุลเดิม' in content or 'นามสกุลเดิม' in content:
            old_last_name_label_y = cy
            old_last_name_label_x = cx
            # Check if value is in the same line
            pattern = r'(?:ชื่อสกุลเดิม|นามสกุลเดิม)'
            after_label = re.split(pattern, content)[-1].strip()
            after_label = re.sub(r'^[-.\s:]+', '', after_label).strip()
            after_label = re.sub(r'[-.\s:]+$', '', after_label).strip()
            if after_label and is_valid_thai_name(after_label) and not is_garbage_text(after_label):
                result['old_last_name'] = clean_ocr_text(after_label)

    # Look for old first name value in separate line (if label found but no value yet)
    if old_first_name_label_y is not None and not result['old_first_name']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Skip garbage text
            if is_garbage_text(content):
                continue

            # Skip labels and dots-only lines
            if 'ชื่อเดิม' in content or 'ชื่อสกุลเดิม' in content or 'นามสกุลเดิม' in content:
                continue
            if re.match(r'^[.\s*]+$', content):
                continue

            # Old first name value should be:
            # - To the right of the label (cx > old_first_name_label_x)
            # - In the x range for first name values
            # - Close to label y position
            if (old_first_name_label_x and cx > old_first_name_label_x and
                OLD_FIRST_NAME_X_MIN <= cx <= OLD_FIRST_NAME_X_MAX and
                abs(cy - old_first_name_label_y) <= 0.3):
                if is_valid_thai_name(content):
                    result['old_first_name'] = clean_ocr_text(content)
                    break

    # Look for old last name value in separate line (if label found but no value yet)
    if old_last_name_label_y is not None and not result['old_last_name']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])

            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Skip garbage text
            if is_garbage_text(content):
                continue

            # Skip labels and dots-only lines
            if 'ชื่อเดิม' in content or 'ชื่อสกุลเดิม' in content or 'นามสกุลเดิม' in content:
                continue
            if re.match(r'^[.\s*]+$', content):
                continue

            # Old last name value should be:
            # - To the right of the label
            # - In the x range for last name values
            # - Close to label y position
            if (old_last_name_label_x and cx > old_last_name_label_x and
                OLD_LAST_NAME_X_MIN <= cx <= OLD_LAST_NAME_X_MAX and
                abs(cy - old_last_name_label_y) <= 0.3):
                if is_valid_thai_name(content):
                    result['old_last_name'] = clean_ocr_text(content)
                    break

    # NO FALLBACK - if we didn't find labels or values, return empty
    # This is intentional - we should NOT extract garbage text as names

    return result


# =============================================================================
# MAIN EXTRACTION
# =============================================================================

def extract_spouse_old_name(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict,
    spouse_info: Dict = None,
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Dict]:
    """Extract spouse old name from JSON content."""

    spouse_old_names = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    pages = json_content.get('pages', [])
    page = find_spouse_page_with_metadata(pages, loader, doc_location_url)

    if page is None:
        return spouse_old_names

    page_lines = page.get('lines', [])
    old_name_data = extract_spouse_old_name_from_page(page_lines)

    # Clean extracted names to remove OCR noise
    old_title = old_name_data.get('old_title', '')
    old_first_name = clean_extracted_name(old_name_data.get('old_first_name', ''))
    old_last_name = clean_extracted_name(old_name_data.get('old_last_name', ''))

    # Fix case where OCR split "นางสาว" and "สาว" prefix ended up in first_name
    if old_first_name and old_first_name.startswith('สาว'):
        if old_title == 'นาง' or old_title == '':
            old_title = 'นางสาว'
            old_first_name = old_first_name[3:].strip()

    # Also handle "นางสาว" prefix in first_name (OCR merged it)
    if old_first_name:
        name_match = re.match(r'^(นางสาว|นาง\s*สาว|นาง|นาย)\s*(.+)$', old_first_name)
        if name_match:
            if not old_title:
                title = name_match.group(1)
                if 'สาว' in title:
                    old_title = 'นางสาว'
                else:
                    old_title = title
            old_first_name = name_match.group(2).strip()

    if old_first_name or old_last_name:
        # Determine title for old name
        # Priority: extracted old_title > spouse_info title adjusted for maiden name
        if not old_title and spouse_info:
            old_title = spouse_info.get('title', '')
            # If married woman (นาง), old title is likely นางสาว
            if old_title == 'นาง':
                old_title = 'นางสาว'

        spouse_old_names.append({
            'spouse_id': None,  # Will be assigned by orchestrator
            'index': 1,
            'title': old_title,
            'first_name': old_first_name or (spouse_info.get('first_name', '') if spouse_info else ''),
            'last_name': old_last_name,  # Don't fall back to current last name for maiden name
            'title_en': '',
            'first_name_en': '',
            'last_name_en': '',
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'latest_submitted_date': latest_submitted_date
        })

    return spouse_old_names


def run_step_3_2(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None,
                 spouse_info_map: Dict = None):
    """
    Run step 3.2 to extract spouse old names.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance
        spouse_info_map: Optional map of nacc_id -> spouse_info for title lookup
    """
    loader = data_loader or PipelineDataLoader(input_dir)

    all_spouse_old_names = []
    next_spouse_id = 1

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']
        doc_info = context.get('doc_info', {})
        nacc_id = nacc_detail.get('nacc_id')
        doc_location_url = doc_info.get('doc_location_url', '')

        # Get spouse info if available
        spouse_info = None
        if spouse_info_map and nacc_id in spouse_info_map:
            spouse_info = spouse_info_map[nacc_id]

        spouse_old_names = extract_spouse_old_name(
            json_content, nacc_detail, submitter_info, spouse_info,
            loader=loader, doc_location_url=doc_location_url
        )

        for old_name in spouse_old_names:
            old_name['spouse_id'] = next_spouse_id
            next_spouse_id += 1

        all_spouse_old_names.extend(spouse_old_names)

    # Write output
    fields = [
        'spouse_id', 'index', 'title', 'first_name', 'last_name',
        'title_en', 'first_name_en', 'last_name_en',
        'submitter_id', 'nacc_id', 'latest_submitted_date'
    ]
    writer = CSVWriter(output_dir, 'spouse_old_name.csv', fields)
    count = writer.write_rows(all_spouse_old_names)
    print(f"Extracted {count} spouse old names to {writer.output_path}")

    return all_spouse_old_names


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')
    run_step_3_2(input_dir, output_dir)
