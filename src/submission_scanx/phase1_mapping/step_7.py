"""
Step 7: Extract land asset detailed information from JSON extract files

This step extracts:
- asset_land_info.csv - Land asset details (doc number, area, location)

Uses page_metadata and text_each_page for targeted page extraction.
Falls back to content-based detection for compatibility.

Key patterns detected:
- Land type keywords: โฉนด, ส.ป.ก, น.ส.3, น.ส.4, ห้องชุด
- Location patterns: ต./อ./จ., ตำบล/อำเภอ/จังหวัด, เขต/แขวง
- Area columns: ไร่, งาน, ตร.ว. (detected by position relative to headers)
"""

import os
import re
import json
import csv
from typing import List, Dict, Optional, Tuple

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import (
    get_polygon_center,
    clean_text,
    format_disclosure_date,
)
from utils.data_loader import PipelineDataLoader, CSVWriter

# Import Thai location lookup for auto-filling missing location data
from utils.thai_location_lookup import fill_missing_location, lookup_district, lookup_province


# Land document type keywords
LAND_DOC_TYPES = ['โฉนด', 'น.ส.3', 'น.ส.3ก', 'น.ส.4', 'ส.ป.ก', 'ห้องชุด', 'กรรมสิทธิ์']


# Thai numeral conversion
THAI_DIGITS = {'๐': '0', '๑': '1', '๒': '2', '๓': '3', '๔': '4',
               '๕': '5', '๖': '6', '๗': '7', '๘': '8', '๙': '9'}

# Land document type keywords
LAND_TYPE_KEYWORDS = ['โฉนด', 'ส.ป.ก', 'น.ส.3', 'น.ส.4', 'ห้องชุด', 'นส.3', 'นส.4',
                      'น.ส.๓', 'น.ส.๔', 'โฉนดที่ดิน', 'หนังสือรับรอง']

# Location keywords
LOCATION_KEYWORDS = ['ต.', 'อ.', 'จ.', 'ตำบล', 'อำเภอ', 'จังหวัด', 'เขต', 'แขวง',
                     'กรุงเทพ', 'กทม', 'ถนน', 'ซอย']


def convert_thai_numerals(text: str) -> str:
    """Convert Thai numerals to Arabic numerals"""
    if not text:
        return text
    for thai, arabic in THAI_DIGITS.items():
        text = text.replace(thai, arabic)
    return text


def parse_area_value(text: str) -> Optional[float]:
    """Parse area value (rai/ngan/sq_wa) from text."""
    if not text:
        return None

    text = text.strip()
    text = convert_thai_numerals(text)

    # Skip if just dashes, dots, empty or markers
    if re.match(r'^[\-\s./*:ก-ฮ]+$', text) or text in ['-', '', 'l', 'I', 'o', 'O']:
        return None

    # Remove commas
    text = text.replace(',', '')

    # Handle space-separated numbers like "61 0" - take only first number
    parts = text.split()
    if len(parts) > 1:
        text = parts[0]

    # Extract number pattern
    match = re.search(r'([\d]+(?:\.\d+)?)', text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None

    return None


def parse_fraction(text: str) -> Optional[float]:
    """Parse fractional values like "5/10" and return decimal value."""
    if not text:
        return None

    text = text.strip()
    text = convert_thai_numerals(text)

    # Match fraction pattern: numerator/denominator
    match = re.match(r'^(\d+)\s*/\s*(\d+)$', text)
    if match:
        try:
            num = float(match.group(1))
            denom = float(match.group(2))
            if denom > 0:
                return num / denom
        except (ValueError, ZeroDivisionError):
            pass
    return None


def clean_province_name(province: str) -> str:
    """Clean province name from OCR artifacts like newlines."""
    if not province:
        return ''
    # Remove newlines and extra spaces
    province = re.sub(r'[\n\r\s]+', '', province)
    # Remove common OCR artifacts
    province = province.strip()
    return province


def fix_location_ocr_errors(text: str) -> str:
    """Fix common OCR errors in location names."""
    if not text:
        return ''

    # Common OCR corrections for district/sub_district names
    ocr_corrections = {
        'ไทยน้อย': 'ไทรน้อย',
        'โคกโกเฒ่า': 'โคกโคเฒ่า',
        'ปากเกร็ด (ตลาดขวัญ)': 'ปากเกร็ด',
        'ต้นธงชัย': 'ต้นธงชัย',  # This is correct
        'ธงชัย': 'ต้นธงชัย',  # Missing prefix
    }

    for wrong, correct in ocr_corrections.items():
        if wrong in text:
            text = text.replace(wrong, correct)

    return text


def fix_district_name(district: str, province: str) -> str:
    """
    Fix district name for 'อำเภอเมือง' cases.
    'เมือง' should become 'เมือง{province}' (e.g., 'เมืองสุพรรณบุรี').
    """
    if not district:
        return ''

    district = district.strip()

    # Apply OCR corrections
    district = fix_location_ocr_errors(district)

    # If district is just 'เมือง' and we have province, expand it
    if district == 'เมือง' and province:
        # Clean province name first
        clean_prov = clean_province_name(province)
        # Remove 'จังหวัด' prefix if present
        clean_prov = re.sub(r'^จังหวัด', '', clean_prov)
        # Look up the proper district name
        lookup_result = lookup_district('เมือง' + clean_prov, clean_prov)
        if lookup_result:
            return lookup_result.get('district', 'เมือง' + clean_prov)
        return 'เมือง' + clean_prov

    return district


def parse_location(text: str) -> Dict[str, str]:
    """
    Parse location text which can be in various formats.
    Returns dict with sub_district, district, province.
    """
    result = {'sub_district': '', 'district': '', 'province': ''}

    if not text:
        return result

    text = clean_text(text)
    text = convert_thai_numerals(text)

    # Remove leading numbers like "1." but not "ต.1"
    text = re.sub(r'^(\d+\.)\s*', '', text)

    # Handle Bangkok variants
    if 'กรุงเทพ' in text or 'กทม' in text:
        result['province'] = 'กรุงเทพมหานคร'

    # Thai word pattern - require at least 2 characters
    THAI_WORD = r'[\u0E00-\u0E7Fa-zA-Z0-9]{2,}'

    # Extract province: จ. or จังหวัด
    prov_patterns = [
        r'จังหวัด\s*(' + THAI_WORD + ')',
        r'จว\.?\s*(' + THAI_WORD + ')',
        r'/จ\.(' + THAI_WORD + ')',
        r'\sจ\.(' + THAI_WORD + ')',
        r'(?<=[ก-ฮ])จ\.(' + THAI_WORD + ')',
        r'^จ\.(' + THAI_WORD + ')',
    ]
    for pattern in prov_patterns:
        match = re.search(pattern, text)
        if match:
            val = match.group(1).strip()
            if len(val) >= 2:  # Ensure at least 2 chars
                result['province'] = clean_province_name(val)
                break

    # Extract district: อ. or อำเภอ or เขต
    dist_patterns = [
        r'อำเภอ\s*(' + THAI_WORD + ')',
        r'เขต\s*(' + THAI_WORD + ')',
        r'/อ\.([^/จ]+?)(?=จ\.|/|$)',
        r'\sอ\.([^/จ]+?)(?=จ\.|/|$)',
        r'^อ\.([^/จ]+?)(?=จ\.|/|$)',
    ]
    for pattern in dist_patterns:
        match = re.search(pattern, text)
        if match:
            val = match.group(1).strip()
            if val and len(val) >= 2:  # Ensure at least 2 chars
                val = re.sub(r'\s*กทม\.?$', '', val)
                result['district'] = val
                break

    # Extract sub-district: ต. or ตำบล or แขวง
    sub_patterns = [
        r'ตำบล\s*(' + THAI_WORD + ')',
        r'แขวง\s*(' + THAI_WORD + ')',
        r'/ต\.(' + THAI_WORD + ')',
        r'\sต\.(' + THAI_WORD + ')',
        r'^ต\.(' + THAI_WORD + ')',
    ]
    for pattern in sub_patterns:
        match = re.search(pattern, text)
        if match:
            val = match.group(1).strip()
            if val and len(val) >= 2:  # Ensure at least 2 chars
                result['sub_district'] = val
                break

    # Fix district name for 'เมือง' case
    if result['district'] and result['province']:
        result['district'] = fix_district_name(result['district'], result['province'])

    return result


def is_land_type(text: str) -> bool:
    """Check if text is a land document type keyword."""
    return any(kw in text for kw in LAND_TYPE_KEYWORDS)


def is_location_text(text: str) -> bool:
    """Check if text contains location information."""
    return any(kw in text for kw in LOCATION_KEYWORDS)


def is_doc_number(text: str) -> bool:
    """
    Check if text looks like a document number (3-6 digits).
    Document numbers are typically 3-6 digits WITHOUT commas or decimal points.
    Prices have commas (e.g., 10,080,000.00) and should NOT match.
    """
    text = convert_thai_numerals(text.strip())

    # If text contains commas, it's likely a price not a doc number
    if ',' in text:
        return False

    # If text contains decimal with more than 0 after, likely a price
    if '.' in text:
        parts = text.split('.')
        if len(parts) == 2 and len(parts[1]) >= 2:  # e.g., ".00" suffix
            return False

    # Remove only dots that aren't decimal separators
    cleaned = text.replace('.', '')

    # Doc numbers are typically 3-6 digits
    return cleaned.isdigit() and 3 <= len(cleaned) <= 6


def is_row_number(text: str) -> bool:
    """Check if text is a row number (1-2 digits, optionally followed by a period)."""
    text = convert_thai_numerals(text.strip())
    # Match 1-2 digits optionally followed by a period
    return re.match(r'^[0-9]{1,2}\.?$', text) is not None


def is_area_value(text: str) -> bool:
    """Check if text looks like an area value."""
    text = convert_thai_numerals(text.strip())
    # Area values are typically small numbers or fractions
    if re.match(r'^[\d]+$', text):
        val = int(text)
        return val < 1000  # Reasonable area value
    if re.match(r'^\d+/\d+$', text):  # Fraction
        return True
    return False


def find_land_pages(pages: List[Dict]) -> List[Tuple[int, Dict]]:
    """Find pages with land asset data."""
    result = []
    for i, page in enumerate(pages):
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])
        if 'รายละเอียดประกอบรายการที่ดิน' in page_text:
            result.append((i, page))
    return result


def group_lines_by_y(lines: List[Dict], tolerance: float = 0.2) -> List[List[Dict]]:
    """Group lines that are on the same visual row based on Y coordinate."""
    if not lines:
        return []

    # Sort by y first
    sorted_lines = sorted(lines, key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[1])

    rows = []
    current_row = [sorted_lines[0]]
    current_y = get_polygon_center(sorted_lines[0].get('polygon', [0]*8))[1]

    for line in sorted_lines[1:]:
        _, cy = get_polygon_center(line.get('polygon', [0]*8))
        if abs(cy - current_y) <= tolerance:
            current_row.append(line)
        else:
            # Sort current row by x before adding
            current_row.sort(key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[0])
            rows.append(current_row)
            current_row = [line]
            current_y = cy

    if current_row:
        current_row.sort(key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[0])
        rows.append(current_row)

    return rows


def detect_column_positions(rows: List[List[Dict]]) -> Dict[str, Tuple[float, float]]:
    """
    Detect column positions dynamically from the data.
    Returns dict with column name -> (min_x, max_x) ranges.
    """
    # Collect x positions for different content types
    doc_num_x = []
    location_x = []
    area_x = []

    for row in rows[:20]:  # Sample first 20 data rows
        for line in row:
            content = line.get('content', '').strip()
            cx, _ = get_polygon_center(line.get('polygon', [0]*8))

            if is_doc_number(content):
                doc_num_x.append(cx)
            elif is_location_text(content):
                location_x.append(cx)
            elif is_area_value(content) and cx > 3.0:  # Area values are on right side
                area_x.append(cx)

    # Calculate ranges with some padding
    columns = {}

    if doc_num_x:
        min_x, max_x = min(doc_num_x), max(doc_num_x)
        columns['doc_number'] = (min_x - 0.3, max_x + 0.3)
    else:
        columns['doc_number'] = (1.0, 2.5)  # Default

    if location_x:
        min_x, max_x = min(location_x), max(location_x)
        columns['location'] = (min_x - 0.2, max_x + 0.3)
    else:
        columns['location'] = (1.8, 3.5)  # Default

    # Area columns are fixed relative positions (ไร่, งาน, ตร.ว.)
    columns['rai'] = (3.4, 3.85)
    columns['ngan'] = (3.85, 4.2)
    columns['sq_wa'] = (4.2, 4.7)

    return columns


def extract_land_info(pages: List[Tuple[int, Dict]], nacc_id: str, submitter_id: str,
                       latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """Extract land asset info from land pages using content-based detection."""
    land_infos = []
    asset_id = start_asset_id

    for _, page in pages:
        lines = page.get('lines', [])

        # Group lines by visual rows
        rows = group_lines_by_y(lines, tolerance=0.2)

        # Detect column positions dynamically
        columns = detect_column_positions(rows)

        current_item = None
        location_texts = []

        for row_lines in rows:
            # Get row position
            row_y = sum(get_polygon_center(l.get('polygon', [0]*8))[1] for l in row_lines) / len(row_lines)

            # Skip header rows (typically y < 3.0)
            if row_y < 3.0:
                continue

            # Collect all content from this row
            row_content = [l.get('content', '').strip() for l in row_lines]
            row_text = ' '.join(row_content)

            # Check for new row start - either by row number or land type keyword
            new_row_detected = False

            # Method 1: First element is a row number (1-2 digits)
            first_line = row_lines[0] if row_lines else None
            if first_line:
                first_content = first_line.get('content', '').strip()
                first_x, _ = get_polygon_center(first_line.get('polygon', [0]*8))

                # Row number at leftmost position
                if first_x < 1.0 and is_row_number(first_content):
                    new_row_detected = True

            # Method 2: Row contains land type keyword
            if not new_row_detected and any(is_land_type(c) for c in row_content):
                # Check if this is a continuation or new item
                if current_item is None or abs(row_y - current_item.get('y_pos', 0)) > 1.0:
                    new_row_detected = True

            if new_row_detected:
                # Save previous item
                if current_item:
                    combined_loc = ' '.join(location_texts)
                    loc_info = parse_location(combined_loc)
                    if loc_info['sub_district']:
                        current_item['sub_distirict'] = loc_info['sub_district']
                    if loc_info['district']:
                        current_item['distirict'] = loc_info['district']
                    if loc_info['province']:
                        current_item['province'] = loc_info['province']

                    # Use Thai location lookup to fill missing location data
                    location = fill_missing_location(
                        current_item.get('sub_distirict', ''),
                        current_item.get('distirict', ''),
                        current_item.get('province', ''),
                        ''  # No post_code for land
                    )
                    sub_district = fix_location_ocr_errors(location.get('sub_district', ''))
                    current_item['sub_distirict'] = sub_district
                    current_item['province'] = clean_province_name(location.get('province', ''))
                    # Fix district name for 'เมือง' -> 'เมือง{province}'
                    district = location.get('district', '')
                    current_item['distirict'] = fix_district_name(district, current_item['province'])

                    # Allow records even without doc_number (some documents don't have them)
                    # Require at least one of: doc_number, rai, ngan, or sq_wa
                    if current_item.get('land_doc_number') or \
                       current_item.get('rai') or current_item.get('ngan') or current_item.get('sq_wa'):
                        land_infos.append(current_item)
                        asset_id += 1

                current_item = {
                    'asset_id': asset_id,
                    'submitter_id': submitter_id,
                    'nacc_id': nacc_id,
                    'land_doc_number': '',
                    'rai': 0,
                    'ngan': 0,
                    'sq_wa': 0,
                    'sub_distirict': '',
                    'distirict': '',
                    'province': '',
                    'y_pos': row_y,
                    'latest_submitted_date': latest_submitted_date
                }
                location_texts = []

            if not current_item:
                continue

            # Allow rows within reasonable y distance
            if abs(row_y - current_item['y_pos']) > 1.0:
                continue

            # Process each line in the row
            for line in row_lines:
                content = line.get('content', '').strip()
                if not content:
                    continue

                polygon = line.get('polygon', [0]*8)
                cx, _ = get_polygon_center(polygon)
                content_converted = convert_thai_numerals(content)

                # Doc number detection - use content pattern AND position
                # Doc numbers are at x < 2.5 and have no commas (prices are at x > 6)
                if is_doc_number(content) and cx < 2.5:
                    doc_num = content_converted.replace('.', '')
                    if not current_item['land_doc_number']:  # Only set if not already set
                        current_item['land_doc_number'] = doc_num

                # Location detection - use keywords
                if is_location_text(content) or (columns['location'][0] <= cx <= columns['location'][1]
                    and re.search(r'[ก-ฮ]{2,}', content)):
                    location_texts.append(content)

                # Area values - detect by position and content pattern
                # Rai column
                if columns['rai'][0] <= cx <= columns['rai'][1]:
                    val = parse_area_value(content)
                    if val is not None and val < 1000:
                        current_item['rai'] = int(val)

                # Ngan column
                if columns['ngan'][0] <= cx <= columns['ngan'][1]:
                    val = parse_area_value(content)
                    if val is not None and val < 10:
                        current_item['ngan'] = int(val)

                # Sq Wa column
                if columns['sq_wa'][0] <= cx <= columns['sq_wa'][1]:
                    frac_val = parse_fraction(content)
                    if frac_val is not None:
                        current_item['sq_wa'] = current_item.get('sq_wa', 0) + frac_val
                    else:
                        val = parse_area_value(content)
                        if val is not None and val < 500:
                            if current_item.get('sq_wa', 0) == 0:
                                current_item['sq_wa'] = val

        # Save last item
        if current_item:
            combined_loc = ' '.join(location_texts)
            loc_info = parse_location(combined_loc)
            if loc_info['sub_district']:
                current_item['sub_distirict'] = loc_info['sub_district']
            if loc_info['district']:
                current_item['distirict'] = loc_info['district']
            if loc_info['province']:
                current_item['province'] = loc_info['province']

            # Use Thai location lookup to fill missing location data
            location = fill_missing_location(
                current_item.get('sub_distirict', ''),
                current_item.get('distirict', ''),
                current_item.get('province', ''),
                ''  # No post_code for land
            )
            sub_district = fix_location_ocr_errors(location.get('sub_district', ''))
            current_item['sub_distirict'] = sub_district
            current_item['province'] = clean_province_name(location.get('province', ''))
            # Fix district name for 'เมือง' -> 'เมือง{province}'
            district = location.get('district', '')
            current_item['distirict'] = fix_district_name(district, current_item['province'])

            # Allow records even without doc_number (some documents don't have them)
            # Require at least one of: doc_number, rai, ngan, or sq_wa
            if current_item.get('land_doc_number') or \
               current_item.get('rai') or current_item.get('ngan') or current_item.get('sq_wa'):
                land_infos.append(current_item)

    return land_infos


def format_sq_wa(value):
    """Format sq_wa value: remove .0 for whole numbers, keep decimals for fractions."""
    if value is None:
        return 0
    if isinstance(value, float):
        if value == int(value):
            return int(value)
        return value
    return value


def load_text_each_page_index(input_dir: str, doc_name: str) -> Optional[Dict]:
    """Load the text_each_page index for a document."""
    # Normalize doc_name - remove .json extension if present
    doc_name_clean = doc_name.replace('.json', '')
    index_path = os.path.join(input_dir, 'text_each_page', f'{doc_name_clean}.json')

    if os.path.exists(index_path):
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return None


def load_page_file(input_dir: str, page_file: str) -> Optional[Dict]:
    """Load a specific page file from text_each_page."""
    page_path = os.path.join(input_dir, 'text_each_page', page_file)
    if os.path.exists(page_path):
        try:
            with open(page_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return None


def extract_land_from_page_content(page_data: Dict, nacc_id: str, submitter_id: str,
                                    latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """
    Extract land info from text_each_page using content-based parsing.
    Uses the 'content' field which contains the full page text.
    """
    land_infos = []
    asset_id = start_asset_id

    content = page_data.get('content', '')
    if not content:
        return []

    # Convert Thai numerals
    content = convert_thai_numerals(content)

    # Split into lines
    lines = content.split('\n')

    # Pattern to match land rows:
    # Format: [row_num]. [doc_type] [doc_number] [location] [rai] [ngan] [sq_wa] [date] ...
    # Example: "1. โฉนด 11799 ในเมือง/เมืองหนองคาย - - 69.7 10/1/56 ..."

    # Pattern for land document line - starts with number and has doc type
    land_row_pattern = re.compile(
        r'^[C\s]*(\d{1,2})[\.\s]+' +  # Row number (may have 'C' prefix from OCR)
        r'(โฉนด|น\.?\s*ส\.?\s*3\s*ก?\.?|น\.?\s*ส\.?\s*4|ส\.?\s*ป\.?\s*ก\.?|ห้องชุด|กรรมสิทธิ์)\s*' +  # Doc type
        r'(\d+)\s+' +  # Doc number
        r'(.+)$',  # Rest of line (location, area, etc.)
        re.IGNORECASE
    )

    # Simpler pattern - just number + doc_type + number
    simple_pattern = re.compile(
        r'^[C\s]*(\d{1,2})[\.\s]+' +
        r'(โฉนด|น\.?\s*ส\.?\s*3\s*ก?\.?|น\.?\s*ส\.?\s*4|ส\.?\s*ป\.?\s*ก\.?)\s*' +
        r'(\d{3,})'
    )

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip header/footer lines
        if any(skip in line for skip in ['ลับ', 'หน้า', 'ลงชื่อ', 'หมายเหตุ', 'ผย.', 'คส.', 'รายละเอียด', 'ประเภท', 'เนื้อที่']):
            continue

        # Try to match land row pattern
        match = land_row_pattern.match(line)
        if not match:
            match = simple_pattern.match(line)

        if match:
            row_num = match.group(1)
            doc_type = match.group(2)
            doc_number = match.group(3)

            # Get the rest of the line after doc number
            rest_of_line = line[match.end(3):].strip() if len(match.groups()) > 3 else ''

            # Parse location from rest of line
            location_text = ''
            rai, ngan, sq_wa = 0, 0, 0

            # Look for location pattern (Thai text with / separator)
            loc_match = re.search(r'([ก-ฮ][ก-ฮa-zA-Z\s\./]+[ก-ฮ])', rest_of_line)
            if loc_match:
                location_text = loc_match.group(1)

            # Parse area values - look for patterns like "2 3 41" or "- - 69.7"
            # Format is: rai ngan sq_wa
            area_match = re.search(r'(\d+|-)\s+(\d+|-)\s+([\d\.]+)', rest_of_line)
            if area_match:
                rai_str = area_match.group(1)
                ngan_str = area_match.group(2)
                sq_wa_str = area_match.group(3)

                if rai_str != '-':
                    try:
                        rai = int(rai_str)
                    except ValueError:
                        pass
                if ngan_str != '-':
                    try:
                        ngan = int(ngan_str)
                    except ValueError:
                        pass
                if sq_wa_str != '-':
                    try:
                        sq_wa = float(sq_wa_str)
                    except ValueError:
                        pass

            # Parse location
            loc_info = parse_location(location_text)

            # Create land info record
            land_item = {
                'asset_id': asset_id,
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'land_doc_number': doc_number,
                'rai': rai,
                'ngan': ngan,
                'sq_wa': sq_wa,
                'sub_distirict': loc_info.get('sub_district', ''),
                'distirict': loc_info.get('district', ''),
                'province': loc_info.get('province', ''),
                'latest_submitted_date': latest_submitted_date
            }

            # Use Thai location lookup to fill missing data
            location = fill_missing_location(
                land_item['sub_distirict'],
                land_item['distirict'],
                land_item['province'],
                ''
            )
            sub_district = fix_location_ocr_errors(location.get('sub_district', ''))
            land_item['sub_distirict'] = sub_district
            land_item['province'] = clean_province_name(location.get('province', ''))
            district = location.get('district', '')
            land_item['distirict'] = fix_district_name(district, land_item['province'])

            land_infos.append(land_item)
            asset_id += 1

    return land_infos


def extract_land_from_page_metadata(input_dir: str, doc_name: str, page_numbers: List[int],
                                     nacc_id: str, submitter_id: str, latest_submitted_date: str,
                                     start_asset_id: int) -> List[Dict]:
    """
    Extract land info using page_metadata to find exact land pages.
    Uses content-based parsing from text_each_page files.
    """
    land_infos = []
    asset_id = start_asset_id

    # Load the text_each_page index
    doc_index = load_text_each_page_index(input_dir, doc_name)
    if not doc_index:
        return []

    pages = doc_index.get('pages', [])

    for page_num in page_numbers:
        # Find the page in the index
        page_info = None
        for p in pages:
            if p.get('page_number') == page_num:
                page_info = p
                break

        if not page_info:
            continue

        # Load the actual page file
        page_file = page_info.get('page_file', '')
        if not page_file:
            continue

        page_data = load_page_file(input_dir, page_file)
        if not page_data:
            continue

        # Extract land info from this page using content-based parsing
        page_land = extract_land_from_page_content(
            page_data, nacc_id, submitter_id, latest_submitted_date, asset_id
        )
        land_infos.extend(page_land)
        asset_id += len(page_land)

    return land_infos


def load_assets_for_matching(output_dir: str) -> Dict[str, List[Dict]]:
    """
    Load asset.csv and create a lookup by submitter_id for matching.
    Returns dict: submitter_id -> list of land assets (asset_type_id 1-9, 36)
    """
    assets_by_submitter = {}
    asset_path = os.path.join(output_dir, 'asset.csv')

    if not os.path.exists(asset_path):
        return {}

    # Land asset type IDs: 1-9 (โฉนด, ส.ป.ก, etc.) and 36 (ห้องชุด on land)
    land_type_ids = set(range(1, 10)) | {36}

    with open(asset_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            asset_type_id = int(row.get('asset_type_id', 0))
            if asset_type_id in land_type_ids:
                submitter_id = row.get('submitter_id', '')
                if submitter_id not in assets_by_submitter:
                    assets_by_submitter[submitter_id] = []
                assets_by_submitter[submitter_id].append({
                    'asset_id': row.get('asset_id', ''),
                    'nacc_id': row.get('nacc_id', ''),
                    'index': int(row.get('index', 0)),
                    'asset_type_id': asset_type_id
                })

    # Sort by index for each submitter
    for submitter_id in assets_by_submitter:
        assets_by_submitter[submitter_id].sort(key=lambda x: x['index'])

    return assets_by_submitter


def run_step_7(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """Run step 7 to extract land asset information."""
    loader = data_loader or PipelineDataLoader(input_dir)

    all_land_info = []

    # Load assets from step_6 for matching by submitter_id
    assets_by_submitter = load_assets_for_matching(output_dir)

    # Load page_metadata index
    page_metadata = loader.page_metadata

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        nacc_id = nacc_detail.get('nacc_id', '')
        submitter_id = nacc_detail.get('submitter_id', '')

        disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
        latest_submitted_date = format_disclosure_date(disclosure_date)

        # Get doc_location_url for page_metadata lookup
        doc_info = context.get('doc_info', {})
        doc_location_url = doc_info.get('doc_location_url', '')

        # Get pages from json_content
        pages = json_content.get('pages', [])

        # Get land assets for this submitter from step_6
        submitter_assets = assets_by_submitter.get(submitter_id, [])

        # Use first available asset_id as starting point, or default
        start_asset_id = int(submitter_assets[0]['asset_id']) if submitter_assets else 2616

        # Try to use page_metadata with text_each_page first (more flexible)
        land_info = []
        if doc_location_url and page_metadata:
            step7_pages = loader.get_step_pages(doc_location_url, 'step_7')
            if step7_pages:
                # Try text_each_page content-based extraction first (no polygon dependency)
                for page_num in step7_pages:
                    page_text = loader.load_page_text(doc_location_url, page_num)
                    if page_text:
                        page_land = extract_land_from_page_content(
                            page_text, nacc_id, submitter_id,
                            latest_submitted_date, start_asset_id + len(land_info)
                        )
                        land_info.extend(page_land)

        # Fall back to polygon-based extraction if text_each_page didn't yield results
        if not land_info:
            land_pages = []
            if doc_location_url and page_metadata:
                step7_pages = loader.get_step_pages(doc_location_url, 'step_7')
                if step7_pages:
                    for page_num in step7_pages:
                        page_idx = page_num - 1
                        if 0 <= page_idx < len(pages):
                            land_pages.append((page_idx, pages[page_idx]))

            if not land_pages:
                land_pages = find_land_pages(pages)

            land_info = extract_land_info(land_pages, nacc_id, submitter_id,
                                           latest_submitted_date, start_asset_id)

        # Match extracted land info with assets from step_6 by index
        for i, info in enumerate(land_info):
            if i < len(submitter_assets):
                # Use asset_id from step_6 matched by submitter_id + index
                info['asset_id'] = submitter_assets[i]['asset_id']

        all_land_info.extend(land_info)

    # Write output
    land_fields = ['asset_id', 'submitter_id', 'nacc_id', 'land_doc_number', 'rai', 'ngan',
                   'sq_wa', 'sub_distirict', 'distirict', 'province', 'latest_submitted_date']

    writer = CSVWriter(output_dir, 'asset_land_info.csv', land_fields)
    # Format sq_wa values before writing
    filtered_rows = []
    for info in all_land_info:
        row = {k: v for k, v in info.items() if k in land_fields}
        if 'sq_wa' in row:
            row['sq_wa'] = format_sq_wa(row['sq_wa'])
        filtered_rows.append(row)
    count = writer.write_rows(filtered_rows)

    print(f"Extracted {count} land infos to {writer.output_path}")


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_7(input_dir, output_dir)
