"""
Step 3.1: Extract spouse basic information from JSON extract files

This step extracts spouse_info.csv containing:
- Personal info: title, first_name, last_name, age
- Marriage status: status, status_date, status_month, status_year
- Address: sub_district, district, province, post_code

The spouse data is typically found on page 5 (คู่สมรส page) of the document.

Marriage status detection uses OCR text analysis (date field parsing).
"""

import os
import re
from typing import List, Dict, Optional, Tuple

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
    parse_marriage_date,
    is_empty_date_field,
    parse_age,
    format_disclosure_date,
)
from utils.data_loader import PipelineDataLoader, CSVWriter

# Import shared spouse utilities
from .step_3_common import find_spouse_page

# Import Thai location lookup for auto-filling missing location data
from utils.thai_location_lookup import fill_missing_location, clean_invalid_location



# =============================================================================
# REGION CONSTANTS
# =============================================================================

# Spouse name region
SPOUSE_NAME_Y_MIN = 1.0
SPOUSE_NAME_Y_MAX = 2.5

# Marriage status region
MARRIAGE_STATUS_Y_MIN = 2.7
MARRIAGE_STATUS_Y_MAX = 3.9

# Address region
ADDRESS_Y_MIN = 6.0
ADDRESS_Y_MAX = 8.5


# =============================================================================
# EXTRACTION FUNCTIONS
# =============================================================================

def extract_spouse_info_from_page(page_lines: List[Dict]) -> Dict:
    """
    Extract spouse basic info from page lines.

    Uses flexible detection combining:
    1. Text pattern matching (keywords like ชื่อและชื่อสกุล, อายุ, ปี)
    2. Polygon position detection (spatial coordinates)
    3. Adaptive region detection based on found labels

    Handles both dedicated spouse pages (data at top) and combined pages (data at y > 4.0).

    Args:
        page_lines: List of line dicts from OCR
    """
    result = {
        'full_name': '',
        'title': '',
        'first_name': '',
        'last_name': '',
        'age': None,
        'status': '',
        'status_date': None,
        'status_month': None,
        'status_year': None,
        'sub_district': '',
        'district': '',
        'province': '',
        'post_code': ''
    }

    # First, find the "คู่สมรส" header to determine the spouse section's y-offset
    spouse_header_variants = ['คู่สมรส', 'คสมรส', 'คู่สมร', 'กู่สมรส', 'ดูสมรส']
    spouse_section_y_offset = 0.0  # Default: spouse data at top of page

    for line in page_lines:
        content = line.get('content', '').strip()
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue
        cx, cy = get_polygon_center(polygon)

        content_clean = content.rstrip('.').rstrip()
        if content_clean in spouse_header_variants or any(content_clean.startswith(v) and len(content_clean) < 15 for v in spouse_header_variants):
            if cx < 3.0:  # Header on left side
                if cy > 4.0:
                    spouse_section_y_offset = cy - 1.2
                break

    # Calculate spouse region bounds
    spouse_name_y_min = SPOUSE_NAME_Y_MIN + spouse_section_y_offset
    spouse_region_max_y = 4.0 + spouse_section_y_offset

    # First pass: Find key labels and their positions
    name_label_pos = None
    age_label_pos = None

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue
        cx, cy = get_polygon_center(polygon)

        if 'ชื่อและชื่อสกุล' in content and spouse_name_y_min - 0.5 <= cy <= spouse_region_max_y:
            if 'บิดา' not in content and 'มารดา' not in content:
                name_label_pos = (cx, cy)
        if 'อายุ' in content and spouse_name_y_min - 0.5 <= cy <= spouse_region_max_y:
            age_label_pos = (cx, cy)

    # Extract name
    _extract_name(page_lines, result, spouse_name_y_min, spouse_region_max_y, name_label_pos, spouse_section_y_offset)

    # Extract age
    _extract_age(page_lines, result, spouse_region_max_y, age_label_pos, spouse_section_y_offset)

    # Extract marriage status
    _extract_marriage_status(page_lines, result)

    # Extract address
    _extract_address(page_lines, result)

    return result


def _extract_name(page_lines: List[Dict], result: Dict, spouse_name_y_min: float,
                  spouse_region_max_y: float, name_label_pos: Optional[Tuple[float, float]],
                  spouse_section_y_offset: float):
    """Extract spouse name from page lines."""

    # Labels and patterns to skip
    skip_patterns = [
        'โปรดระบุ', 'คำนำหน้า', 'ชื่อเดิม', 'ชื่อสกุลเดิม', 'นามสกุลเดิม',
        'บิดา', 'มารดา', 'บุตร', 'ญาติ', 'พี่น้อง',
        'ผู้ยื่น', 'สถานภาพ', 'ที่อยู่', 'หมายเหตุ', 'กรณี',
        '...', '....', ':', '*',
        'ผู้ให้กู้', 'เจ้าหนี้', 'หนี้สิน', 'ยกไปกรอก', 'จำนวนเงิน',
        'รายละเอียดประกอบ', 'บัญชีฯ', 'หน้า 7', 'หน้า ๗'
    ]

    def is_valid_name_content(text):
        if not text or len(text) < 2:
            return False
        for skip in skip_patterns:
            if skip in text:
                return False
        if not re.search(r'[ก-๙]', text):
            return False
        return True

    # Method 1: Name in same line as label
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue
        cx, cy = get_polygon_center(polygon)

        if spouse_name_y_min - 0.3 <= cy <= spouse_region_max_y:
            if 'บิดา' in content or 'มารดา' in content:
                continue

            if 'ชื่อและชื่อสกุล' in content:
                name_part = content.split('ชื่อและชื่อสกุล')[-1].strip()
                name_part = re.sub(r'^[.\s:]+', '', name_part).strip()
                name_part = re.sub(r'^\(ภาษา[ไทยอังกฤษ]+\)\s*', '', name_part).strip()
                if is_valid_thai_name(name_part):
                    result['full_name'] = name_part
                    title, first, last = extract_title_and_name(name_part)
                    result['title'] = title
                    result['first_name'] = first
                    result['last_name'] = last
                    return

    # Method 2: Name near label position
    if not result['full_name'] and name_label_pos:
        label_x, label_y = name_label_pos
        name_candidates = []

        title_patterns = [
            'นาย', 'นาง', 'นางสาว',
            'น.ส.', 'นส.', 'นาส.', 'น.ส',
            'พล', 'ดร.', 'พัน', 'ร้อย', 'นาวา', 'เรือ',
        ]

        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            cx, cy = get_polygon_center(polygon)

            if abs(cy - label_y) < 0.5 and cx > label_x:
                if 'ชื่อและชื่อสกุล' not in content and 'อายุ' not in content:
                    cleaned = content.strip().rstrip('.')
                    if is_valid_name_content(cleaned):
                        name_candidates.append({'content': cleaned, 'cx': cx, 'cy': cy})

        name_candidates.sort(key=lambda x: x['cx'])
        for cand in name_candidates:
            content = cand['content']
            if any(t in content for t in title_patterns):
                combined_name = content
                for other in name_candidates:
                    if other['cx'] > cand['cx'] and abs(other['cy'] - cand['cy']) < 0.3:
                        other_content = other['content']
                        if is_valid_name_content(other_content) and not any(t in other_content for t in title_patterns):
                            combined_name = content + ' ' + other_content
                            break

                result['full_name'] = clean_ocr_text(combined_name)
                title, first, last = extract_title_and_name(combined_name)
                result['title'] = title
                result['first_name'] = first
                result['last_name'] = last
                return

    # Method 2b: Name without title, near label position
    if not result['full_name'] and name_label_pos:
        label_x, label_y = name_label_pos
        best_candidate = None
        best_distance = float('inf')

        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            cx, cy = get_polygon_center(polygon)

            if abs(cy - label_y) < 0.3 and cx > label_x:
                if 'ชื่อและชื่อสกุล' in content or 'อายุ' in content or 'ปี' in content:
                    continue
                if any(skip in content for skip in skip_patterns):
                    continue

                cleaned = content.strip().rstrip('.')
                if is_valid_name_content(cleaned) and 4 <= len(cleaned) <= 50:
                    if ' ' in cleaned:
                        distance = abs(cy - label_y)
                        if distance < best_distance:
                            best_distance = distance
                            best_candidate = cleaned

        if best_candidate:
            result['full_name'] = clean_ocr_text(best_candidate)
            title, first, last = extract_title_and_name(best_candidate)
            result['title'] = title
            result['first_name'] = first
            result['last_name'] = last
            return

    # Method 3: Search by text pattern in spouse name region
    title_patterns = [
        'นาย', 'นาง', 'นางสาว',
        'น.ส.', 'นส.', 'นาส.', 'น.ส',
        'พล.', 'พล', 'พัน', 'ร้อย', 'นาวา', 'เรือ',
        'ดร.', 'ศ.', 'รศ.', 'ผศ.',
    ]
    if not result['full_name']:
        name_region_lines = []
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            cx, cy = get_polygon_center(polygon)

            if spouse_name_y_min <= cy <= spouse_name_y_min + 1.0 and 1.5 <= cx <= 5.5:
                if 'ชื่อและชื่อสกุล' not in content and is_valid_name_content(content):
                    name_region_lines.append({'content': content, 'cx': cx, 'cy': cy})

        name_region_lines.sort(key=lambda x: (x['cy'], x['cx']))

        for line_info in name_region_lines:
            content = line_info['content']
            if any(t in content for t in title_patterns):
                combined_name = content
                for other in name_region_lines:
                    if other['cx'] > line_info['cx'] and abs(other['cy'] - line_info['cy']) < 0.3:
                        other_content = other['content']
                        if is_valid_name_content(other_content) and not any(t in other_content for t in title_patterns):
                            combined_name = content + ' ' + other_content
                            break

                result['full_name'] = clean_ocr_text(combined_name)
                title, first, last = extract_title_and_name(combined_name)
                result['title'] = title
                result['first_name'] = first
                result['last_name'] = last
                return


def _extract_age(page_lines: List[Dict], result: Dict, spouse_region_max_y: float,
                 age_label_pos: Optional[Tuple[float, float]], spouse_section_y_offset: float):
    """Extract spouse age from page lines."""

    # Method 1: Age with "อายุ" or "ปี" keyword
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue
        cx, cy = get_polygon_center(polygon)

        if cy < spouse_region_max_y:
            if 'อายุ' in content or ('ปี' in content and re.search(r'\d+\s*ปี', content)):
                age = parse_age(content)
                if age and 15 <= age <= 120:
                    result['age'] = age
                    return

    # Method 2: Age near "อายุ" label position
    if not result['age'] and age_label_pos:
        label_x, label_y = age_label_pos
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            cx, cy = get_polygon_center(polygon)

            if abs(cy - label_y) < 0.5 and cx > label_x - 1:
                match = re.search(r'(\d{2})', content)
                if match:
                    age_val = int(match.group(1))
                    if 15 <= age_val <= 99:
                        result['age'] = age_val
                        return

    # Method 3: Age by position
    if not result['age']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            cx, cy = get_polygon_center(polygon)

            age_y_min = 0.8 + spouse_section_y_offset
            age_y_max = 2.5 + spouse_section_y_offset
            if 5.0 <= cx <= 8.0 and age_y_min <= cy <= age_y_max:
                if 'ปี' in content or 'อายุ' in content:
                    age = parse_age(content)
                    if age and 15 <= age <= 120:
                        result['age'] = age
                        return
                elif re.match(r'^\d{2}\s*$', content.strip()):
                    age_val = int(content.strip())
                    if 15 <= age_val <= 99:
                        result['age'] = age_val
                        return
                elif re.match(r'^(\d{2})\s*[gปgี]', content.strip()):
                    match = re.match(r'^(\d{2})', content.strip())
                    if match:
                        age_val = int(match.group(1))
                        if 15 <= age_val <= 99:
                            result['age'] = age_val
                            return

    # Method 4: Search spouse region for age pattern
    if not result['age']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue
            cx, cy = get_polygon_center(polygon)

            if cy < spouse_region_max_y:
                match = re.search(r'อายุ[.\s]*(\d{2})\s*ปี?', content)
                if match:
                    age_val = int(match.group(1))
                    if 15 <= age_val <= 99:
                        result['age'] = age_val
                        return

                match = re.search(r'(\d{2})\s*ปี', content)
                if match:
                    age_val = int(match.group(1))
                    if 15 <= age_val <= 99:
                        result['age'] = age_val
                        return


def _extract_marriage_status(page_lines: List[Dict], result: Dict):
    """
    Extract marriage status and date from page lines using OCR text analysis.

    Args:
        page_lines: List of line dicts from OCR
        result: Dict to store extracted data
    """

    # Collect all lines in status area
    status_lines = []
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue
        cx, cy = get_polygon_center(polygon)
        if MARRIAGE_STATUS_Y_MIN <= cy <= MARRIAGE_STATUS_Y_MAX:
            status_lines.append({'content': content, 'cx': cx, 'cy': cy, 'polygon': polygon})

    # Check for "จดทะเบียนสมรส" and extract date
    registered_marriage_found = False
    date_content = ''
    for line_info in status_lines:
        content = line_info['content']
        if 'จดทะเบียนสมรส' in content:
            registered_marriage_found = True
            date_content = content
            day, month, year = parse_marriage_date(content)
            if day:
                result['status_date'] = day
            if month:
                result['status_month'] = month
            if year:
                result['status_year'] = year
            break

    # If date not found in same line, look in adjacent lines
    if registered_marriage_found and not result['status_date']:
        for line_info in status_lines:
            content = line_info['content']
            if 'เมื่อวันที่' in content or re.search(r'\d{1,2}\s*[/\s]*[ก-๙]+', content):
                day, month, year = parse_marriage_date(content)
                if day:
                    result['status_date'] = day
                if month:
                    result['status_month'] = month
                if year:
                    result['status_year'] = year
                if day or month or year:
                    break

    # Determine status using OCR-based logic
    if registered_marriage_found:
        if is_empty_date_field(date_content):
            # Empty date field typically means "อยู่กินกันฉันสามีภริยา"
            result['status'] = 'อยู่กินกันฉันสามีภริยา'
        elif result['status_date'] or result['status_month'] or result['status_year']:
            # Has date - registered marriage
            result['status'] = 'จดทะเบียนสมรส'
        else:
            # Default to registered marriage if found
            result['status'] = 'จดทะเบียนสมรส'


def _extract_address(page_lines: List[Dict], result: Dict):
    """Extract address information from page lines using pattern matching."""

    # Content patterns to skip (labels, placeholders)
    skip_patterns = ['ถนน', 'ซอย', 'หมู่ที่', '...', '....', 'บ้านเลขที่', 'เลขที่']

    def is_valid_location_name(text):
        """Check if text looks like a valid location name."""
        if not text or len(text) < 2:
            return False
        # Must be mostly Thai
        if not re.search(r'[ก-๙]', text):
            return False
        # Skip placeholders and labels
        for skip in skip_patterns:
            if skip in text:
                return False
        # Skip if too short or too long
        if not (2 <= len(text) <= 30):
            return False
        return True

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Only look in address region
        if not (ADDRESS_Y_MIN <= cy <= ADDRESS_Y_MAX):
            continue

        # Sub-district: look for ตำบล/แขวง keyword
        if not result['sub_district']:
            if 'ตำบล' in content or 'แขวง' in content:
                # Extract value after the label (exclude label text itself)
                match = re.search(r'(?:ตำบล/แขวง|ตำบล|แขวง)[.\s/:]*([ก-๙]{2,20})', content)
                if match:
                    value = match.group(1).strip()
                    # Skip if value is just the label
                    if value not in ['ตำบล', 'แขวง', 'เขต'] and is_valid_location_name(value):
                        result['sub_district'] = value

        # District: look for อำเภอ/เขต keyword
        if not result['district']:
            if 'อำเภอ' in content or 'เขต' in content:
                match = re.search(r'(?:อำเภอ/เขต|อำเภอ|เขต)[.\s/:]*([ก-๙]{2,20})', content)
                if match:
                    value = match.group(1).strip()
                    # Skip if value is just the label
                    if value not in ['อำเภอ', 'เขต', 'ตำบล', 'แขวง'] and is_valid_location_name(value):
                        result['district'] = value

        # Province: look for จังหวัด keyword or กรุงเทพ
        if not result['province']:
            if 'กรุงเทพ' in content:
                result['province'] = 'กรุงเทพมหานคร'
            elif 'จังหวัด' in content:
                match = re.search(r'จังหวัด[.\s/:]*([ก-๙]{2,20})', content)
                if match:
                    value = match.group(1).strip()
                    if is_valid_location_name(value):
                        result['province'] = value

        # Post code: look for 5-digit number with รหัสไปรษณีย์ label
        if not result['post_code']:
            if 'รหัสไปรษณีย์' in content:
                match = re.search(r'(\d{5})', content)
                if match:
                    result['post_code'] = match.group(1)


# =============================================================================
# MAIN EXTRACTION
# =============================================================================

def extract_spouse_info(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict
) -> List[Dict]:
    """
    Extract spouse basic info from JSON content.

    Args:
        json_content: Parsed JSON from OCR
        nacc_detail: NACC detail record
        submitter_info: Submitter info record
    """

    spouse_infos = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    pages = json_content.get('pages', [])
    page = find_spouse_page(pages)

    if page is None:
        return spouse_infos

    page_lines = page.get('lines', [])
    spouse_info = extract_spouse_info_from_page(page_lines)

    # Check if we have meaningful data
    has_meaningful_data = (
        spouse_info.get('first_name') or
        spouse_info.get('last_name') or
        spouse_info.get('age') or
        spouse_info.get('status') or
        spouse_info.get('province')
    )

    if has_meaningful_data:
        # Clean last_name - remove age suffix like "41 ปี", "ปี", etc.
        last_name = spouse_info.get('last_name', '')
        if last_name:
            # Remove patterns like "41 ปี", "๔๑ ปี", just "ปี" at end
            last_name = re.sub(r'\s*\d+\s*ปี\s*$', '', last_name)
            last_name = re.sub(r'\s*[๐-๙]+\s*ปี\s*$', '', last_name)
            last_name = re.sub(r'\s+ปี\s*$', '', last_name)
            last_name = last_name.strip()

        # First clean invalid location values (remove OCR garbage)
        cleaned_location = clean_invalid_location(
            spouse_info.get('sub_district', ''),
            spouse_info.get('district', ''),
            spouse_info.get('province', ''),
            spouse_info.get('post_code', '')
        )

        # Then use Thai location lookup to fill missing location data
        location = fill_missing_location(
            cleaned_location.get('sub_district', ''),
            cleaned_location.get('district', ''),
            cleaned_location.get('province', ''),
            cleaned_location.get('post_code', '')
        )

        spouse_infos.append({
            'spouse_id': None,  # Will be assigned by orchestrator
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'title': spouse_info.get('title', ''),
            'first_name': spouse_info.get('first_name', ''),
            'last_name': last_name,
            'title_en': '',
            'first_name_en': '',
            'last_name_en': '',
            'age': spouse_info.get('age', ''),
            'status': spouse_info.get('status', ''),
            'status_date': spouse_info.get('status_date', ''),
            'status_month': spouse_info.get('status_month', ''),
            'status_year': spouse_info.get('status_year', ''),
            'sub_district': location.get('sub_district', ''),
            'district': location.get('district', ''),
            'province': location.get('province', ''),
            'post_code': location.get('post_code', ''),
            'phone_number': '',
            'mobile_number': '',
            'email': '',
            'latest_submitted_date': latest_submitted_date
        })

    return spouse_infos


def run_step_3_1(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 3.1 to extract spouse basic information.
    """
    loader = data_loader or PipelineDataLoader(input_dir)

    all_spouse_infos = []
    next_spouse_id = 1

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']

        spouse_infos = extract_spouse_info(json_content, nacc_detail, submitter_info)

        for spouse_info in spouse_infos:
            spouse_info['spouse_id'] = next_spouse_id
            next_spouse_id += 1

        all_spouse_infos.extend(spouse_infos)

    # Write output
    fields = [
        'spouse_id', 'submitter_id', 'nacc_id', 'title', 'first_name', 'last_name',
        'title_en', 'first_name_en', 'last_name_en', 'age', 'status',
        'status_date', 'status_month', 'status_year',
        'sub_district', 'district', 'province', 'post_code',
        'phone_number', 'mobile_number', 'email', 'latest_submitted_date'
    ]
    writer = CSVWriter(output_dir, 'spouse_info.csv', fields)
    count = writer.write_rows(all_spouse_infos)
    print(f"Extracted {count} spouse infos to {writer.output_path}")

    return all_spouse_infos


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')
    run_step_3_1(input_dir, output_dir)
