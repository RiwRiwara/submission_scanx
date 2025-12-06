"""
Step 8: Extract building asset detailed information from JSON extract files

This step extracts:
- asset_building_info.csv - Building asset details (doc number, location)

Uses page_metadata and text_each_page for flexible content-based extraction.
Falls back to polygon-based detection for compatibility.

Key patterns detected:
- Page marker: รายละเอียดประกอบรายการโรงเรือนและสิ่งปลูกสร้าง
- Building types: บ้านพักอาศัย, อาคาร, ตึก, ห้องชุด, คอนโด, โรงเรือน
- Doc number from: ปลูกสร้างบน เอกสารสิทธิ์ (เลขที่)
- Location patterns: ถนน, ซอย, ต./อ./จ., เขต/แขวง
"""

import os
import re
import csv
from typing import List, Dict, Tuple, Optional

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import get_polygon_center, clean_text, format_disclosure_date
from utils.data_loader import PipelineDataLoader, CSVWriter
from utils.thai_location_lookup import fill_missing_location


# Thai numeral conversion
THAI_DIGITS = {'๐': '0', '๑': '1', '๒': '2', '๓': '3', '๔': '4',
               '๕': '5', '๖': '6', '๗': '7', '๘': '8', '๙': '9'}


def convert_thai_numerals_global(text: str) -> str:
    """Convert Thai numerals to Arabic numerals"""
    if not text:
        return text
    for thai, arabic in THAI_DIGITS.items():
        text = text.replace(thai, arabic)
    return text


# Building type keywords
BUILDING_TYPE_KEYWORDS = ['บ้านพักอาศัย', 'บ้าน', 'อาคาร', 'ตึก', 'ห้องชุด', 'คอนโด',
                          'โรงเรือน', 'สิ่งปลูกสร้าง', 'อพาร์ทเม้นท์', 'ทาวน์เฮาส์',
                          'ชั้น', 'หลัง', 'โกดัง', 'สำนักงาน']

# Location keywords
LOCATION_KEYWORDS = ['ถนน', 'ซอย', 'ต.', 'อ.', 'จ.', 'ตำบล', 'อำเภอ', 'จังหวัด',
                     'เขต', 'แขวง', 'กรุงเทพ', 'กทม', 'หมู่', 'บ้านเลขที่']


def convert_thai_numerals(text: str) -> str:
    """Convert Thai numerals to Arabic numerals"""
    if not text:
        return text
    for thai, arabic in THAI_DIGITS.items():
        text = text.replace(thai, arabic)
    return text


def is_building_type(text: str) -> bool:
    """Check if text contains building type keyword."""
    return any(kw in text for kw in BUILDING_TYPE_KEYWORDS)


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
    """Check if text is a row number (1-2 digits)."""
    text = convert_thai_numerals(text.strip())
    return re.match(r'^[0-9]{1,2}$', text) is not None


def parse_location(text: str) -> Dict[str, str]:
    """Parse location text and return sub_district, district, province."""
    result = {'sub_district': '', 'district': '', 'province': ''}

    if not text:
        return result

    text = clean_text(text)
    text = convert_thai_numerals(text)

    # Handle Bangkok variants
    if 'กรุงเทพ' in text or 'กทม' in text:
        result['province'] = 'กรุงเทพมหานคร'

    # Thai word pattern - require at least 2 characters
    THAI_WORD = r'[\u0E00-\u0E7Fa-zA-Z0-9]{2,}'

    # Extract province
    prov_patterns = [
        r'จังหวัด\s*(' + THAI_WORD + ')',
        r'จว?\.?\s*(' + THAI_WORD + ')',
        r'/จ\.(' + THAI_WORD + ')',
        r'\sจ\.(' + THAI_WORD + ')',
    ]
    for pattern in prov_patterns:
        match = re.search(pattern, text)
        if match:
            val = match.group(1).strip()
            if len(val) >= 2:  # Ensure at least 2 chars
                result['province'] = val
                break

    # Extract district
    dist_patterns = [
        r'อำเภอ\s*(' + THAI_WORD + ')',
        r'เขต\s*(' + THAI_WORD + ')',
        r'/อ\.(' + THAI_WORD + ')',
        r'\sอ\.(' + THAI_WORD + ')',
    ]
    for pattern in dist_patterns:
        match = re.search(pattern, text)
        if match:
            val = match.group(1).strip()
            if val and len(val) >= 2:  # Ensure at least 2 chars
                val = re.sub(r'\s*กทม\.?$', '', val)
                result['district'] = val
                break

    # Extract sub-district
    sub_patterns = [
        r'ตำบล\s*(' + THAI_WORD + ')',
        r'แขวง\s*(' + THAI_WORD + ')',
        r'/ต\.(' + THAI_WORD + ')',
        r'\sต\.(' + THAI_WORD + ')',
    ]
    for pattern in sub_patterns:
        match = re.search(pattern, text)
        if match:
            val = match.group(1).strip()
            if val and len(val) >= 2:  # Ensure at least 2 chars
                result['sub_district'] = val
                break

    return result


def find_building_pages_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Tuple[int, Dict]]:
    """
    Find pages with building asset data using page_metadata first, then fallback to content detection.

    Args:
        pages: List of page dicts from json_content
        loader: Optional PipelineDataLoader instance for page_metadata lookup
        doc_location_url: Document location URL for page_metadata lookup

    Returns:
        List of (page_idx, page_dict) tuples for building pages
    """
    result = []

    # Try to use page_metadata if loader is available
    if loader and doc_location_url:
        step8_pages = loader.get_step_pages(doc_location_url, 'step_8')
        if step8_pages:
            for page_num in step8_pages:
                # Convert 1-indexed page_num to 0-indexed page_idx
                page_idx = page_num - 1
                if 0 <= page_idx < len(pages):
                    page_data = pages[page_idx]
                    if (page_idx, page_data) not in result:
                        result.append((page_idx, page_data))

    # Fallback: use content-based detection if no pages found via metadata
    if not result:
        result = find_building_pages(pages)

    return result


def find_building_pages(pages: List[Dict]) -> List[Tuple[int, Dict]]:
    """Find pages with building asset data using content markers."""
    result = []
    for i, page in enumerate(pages):
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])
        if 'รายละเอียดประกอบรายการโรงเรือนและสิ่งปลูกสร้าง' in page_text:
            result.append((i, page))
    return result


def extract_building_from_page_content(page_data: Dict, nacc_id: str, submitter_id: str,
                                        latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """
    Extract building info from text_each_page using content-based parsing.
    Uses the 'content' field which contains the full page text.
    More flexible than polygon-based extraction.
    """
    building_infos = []
    asset_id = start_asset_id

    content = page_data.get('content', '')
    if not content:
        return []

    # Convert Thai numerals
    content = convert_thai_numerals_global(content)

    # Split into lines
    lines = content.split('\n')

    # Pattern to match building rows:
    # Format: [row_num]. [building_type] [description] [doc_number] [location] ...
    # Example: "1. บ้านพักอาศัย 2 ชั้น 15112 ต.บางไทร/อ.บางไทร จ.อยุธยา ..."

    # Pattern for building line - starts with number and has building type
    building_row_pattern = re.compile(
        r'^[C\s]*(\d{1,2})[\.\s]+' +  # Row number (may have 'C' prefix from OCR)
        r'(บ้านพักอาศัย|บ้าน|อาคาร|ตึก|ห้องชุด|คอนโด|โรงเรือน|ทาวน์เฮาส์|อพาร์ทเม้นท์)\s*' +  # Building type
        r'(.+)$',  # Rest of line
        re.IGNORECASE
    )

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip header/footer lines
        if any(skip in line for skip in ['ลับ', 'หน้า', 'ลงชื่อ', 'หมายเหตุ', 'ผย.', 'คส.',
                                          'รายละเอียดประกอบ', 'ประเภท', 'ที่ตั้ง']):
            continue

        # Try to match building row pattern
        match = building_row_pattern.match(line)

        if match:
            row_num = match.group(1)
            building_type = match.group(2)
            rest_of_line = match.group(3).strip()

            # Extract doc number - look for 4-6 digit numbers
            doc_numbers = []
            doc_match = re.findall(r'\b(\d{4,6})\b', rest_of_line)
            for num in doc_match:
                # Skip years and invalid numbers
                if num.startswith('256') or num.startswith('255'):
                    continue
                if num in ('00000', '0000', '40000'):
                    continue
                # Skip if looks like a price (part of comma-separated number)
                if num not in doc_numbers:
                    doc_numbers.append(num)

            # Parse location from rest of line
            loc_info = parse_location(rest_of_line)

            # Create building info record
            building_item = {
                'asset_id': asset_id,
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'building_doc_number': ', '.join(doc_numbers[:2]) if doc_numbers else '',  # Max 2 doc numbers
                'sub_district': loc_info.get('sub_district', ''),
                'district': loc_info.get('district', ''),
                'province': loc_info.get('province', ''),
                'latest_submitted_date': latest_submitted_date
            }

            # Use Thai location lookup to fill missing data
            location = fill_missing_location(
                building_item['sub_district'],
                building_item['district'],
                building_item['province'],
                ''
            )
            building_item['sub_district'] = location.get('sub_district', '')
            building_item['district'] = location.get('district', '')
            building_item['province'] = location.get('province', '')

            # Only add if we have doc number
            if building_item['building_doc_number']:
                building_infos.append(building_item)
                asset_id += 1

    return building_infos


def group_lines_by_y(lines: List[Dict], tolerance: float = 0.2) -> List[List[Dict]]:
    """Group lines that are on the same visual row based on Y coordinate."""
    if not lines:
        return []

    sorted_lines = sorted(lines, key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[1])

    rows = []
    current_row = [sorted_lines[0]]
    current_y = get_polygon_center(sorted_lines[0].get('polygon', [0]*8))[1]

    for line in sorted_lines[1:]:
        _, cy = get_polygon_center(line.get('polygon', [0]*8))
        if abs(cy - current_y) <= tolerance:
            current_row.append(line)
        else:
            current_row.sort(key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[0])
            rows.append(current_row)
            current_row = [line]
            current_y = cy

    if current_row:
        current_row.sort(key=lambda x: get_polygon_center(x.get('polygon', [0]*8))[0])
        rows.append(current_row)

    return rows


def extract_building_info(pages: List[Tuple[int, Dict]], nacc_id: str, submitter_id: str,
                          latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """Extract building asset info from building pages using content-based detection."""
    building_infos = []
    asset_id = start_asset_id

    for _, page in pages:
        lines = page.get('lines', [])

        # Group lines by visual rows
        rows = group_lines_by_y(lines, tolerance=0.25)

        current_item = None
        doc_numbers = []
        location_texts = []

        for row_lines in rows:
            # Get row position
            row_y = sum(get_polygon_center(l.get('polygon', [0]*8))[1] for l in row_lines) / len(row_lines)

            # Skip header rows
            if row_y < 3.0:
                continue

            # Collect all content from this row
            row_content = [l.get('content', '').strip() for l in row_lines]
            row_text = ' '.join(row_content)

            # Check for new building item
            new_row_detected = False

            # Method 1: Row starts with a row number
            first_line = row_lines[0] if row_lines else None
            if first_line:
                first_content = first_line.get('content', '').strip()
                first_x, _ = get_polygon_center(first_line.get('polygon', [0]*8))

                if first_x < 1.2 and is_row_number(first_content):
                    new_row_detected = True

            # Method 2: Row contains building type keyword at start
            if not new_row_detected:
                for i, content in enumerate(row_content[:3]):  # Check first 3 items
                    if is_building_type(content):
                        # Check if this is a new item or continuation
                        if current_item is None or abs(row_y - current_item.get('y_pos', 0)) > 1.5:
                            new_row_detected = True
                            break

            if new_row_detected:
                # Save previous item
                if current_item:
                    if doc_numbers:
                        current_item['building_doc_number'] = ', '.join(doc_numbers)

                    # Parse accumulated location
                    combined_loc = ' '.join(location_texts)
                    loc_info = parse_location(combined_loc)
                    if loc_info['sub_district']:
                        current_item['sub_district'] = loc_info['sub_district']
                    if loc_info['district']:
                        current_item['district'] = loc_info['district']
                    if loc_info['province']:
                        current_item['province'] = loc_info['province']

                    # Use Thai location lookup to fill missing location data
                    location = fill_missing_location(
                        current_item.get('sub_district', ''),
                        current_item.get('district', ''),
                        current_item.get('province', ''),
                        ''  # No post_code for building
                    )
                    current_item['sub_district'] = location.get('sub_district', '')
                    current_item['district'] = location.get('district', '')
                    current_item['province'] = location.get('province', '')

                    if current_item['building_doc_number']:
                        building_infos.append(current_item)
                        asset_id += 1

                current_item = {
                    'asset_id': asset_id,
                    'submitter_id': submitter_id,
                    'nacc_id': nacc_id,
                    'building_doc_number': '',
                    'sub_district': '',
                    'district': '',
                    'province': '',
                    'y_pos': row_y,
                    'latest_submitted_date': latest_submitted_date
                }
                doc_numbers = []
                location_texts = []

            if not current_item:
                continue

            # Allow content within reasonable y distance
            if abs(row_y - current_item['y_pos']) > 2.0:
                continue

            # Process each line in the row
            for line in row_lines:
                content = line.get('content', '').strip()
                if not content:
                    continue

                polygon = line.get('polygon', [0]*8)
                cx, _ = get_polygon_center(polygon)
                content_converted = convert_thai_numerals(content)

                # Doc number detection - building doc numbers are at x between 3.2-4.5
                # (this is the "เอกสารสิทธิ์ (เลขที่)" column)
                # Also check content for comma-separated doc numbers like "15112,9233"
                if 3.2 <= cx <= 4.5:
                    # Handle comma-separated doc numbers
                    if ',' in content and re.match(r'^[\d,\s]+$', content_converted):
                        for part in content_converted.split(','):
                            part = part.strip()
                            if part and 3 <= len(part) <= 6:
                                # Skip invalid numbers
                                if part in ('00000', '0000', '40000'):
                                    continue
                                # Skip years
                                if part.startswith('256') or part.startswith('255'):
                                    continue
                                if part not in doc_numbers:
                                    doc_numbers.append(part)
                    elif is_doc_number(content):
                        doc_num = content_converted.replace('.', '')
                        # Skip years and invalid numbers
                        if doc_num.startswith('256') or doc_num.startswith('255'):
                            continue
                        if doc_num in ('00000', '0000', '40000'):
                            continue
                        if doc_num not in doc_numbers:
                            doc_numbers.append(doc_num)

                # Location detection
                if is_location_text(content):
                    location_texts.append(content)
                # Also capture Thai text that might be location (streets, areas)
                elif re.search(r'[ก-ฮ]{3,}', content) and not is_building_type(content):
                    # Skip header-like content
                    if 'มูลค่า' not in content and 'บาท' not in content:
                        location_texts.append(content)

        # Save last item
        if current_item:
            if doc_numbers:
                current_item['building_doc_number'] = ', '.join(doc_numbers)

            combined_loc = ' '.join(location_texts)
            loc_info = parse_location(combined_loc)
            if loc_info['sub_district']:
                current_item['sub_district'] = loc_info['sub_district']
            if loc_info['district']:
                current_item['district'] = loc_info['district']
            if loc_info['province']:
                current_item['province'] = loc_info['province']

            # Use Thai location lookup to fill missing location data
            location = fill_missing_location(
                current_item.get('sub_district', ''),
                current_item.get('district', ''),
                current_item.get('province', ''),
                ''  # No post_code for building
            )
            current_item['sub_district'] = location.get('sub_district', '')
            current_item['district'] = location.get('district', '')
            current_item['province'] = location.get('province', '')

            if current_item['building_doc_number']:
                building_infos.append(current_item)

    return building_infos


def load_building_assets_for_matching(output_dir: str) -> Dict[str, List[Dict]]:
    """
    Load asset.csv and create a lookup by submitter_id for matching building assets.
    Returns dict: submitter_id -> list of building assets (asset_type_id 10-17, 37)
    """
    assets_by_submitter = {}
    asset_path = os.path.join(output_dir, 'asset.csv')

    if not os.path.exists(asset_path):
        return {}

    # Building asset type IDs: 10-17 (บ้าน, อาคาร, etc.) and 37 (other buildings)
    building_type_ids = set(range(10, 18)) | {37}

    with open(asset_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            asset_type_id = int(row.get('asset_type_id', 0))
            if asset_type_id in building_type_ids:
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


def run_step_8(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """Run step 8 to extract building asset information."""
    loader = data_loader or PipelineDataLoader(input_dir)

    all_building_info = []

    # Load assets from step_6 for matching by submitter_id
    assets_by_submitter = load_building_assets_for_matching(output_dir)

    # Load page_metadata index
    page_metadata = loader.page_metadata

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        nacc_id = nacc_detail.get('nacc_id', '')
        submitter_id = nacc_detail.get('submitter_id', '')
        latest_submitted_date = format_disclosure_date(nacc_detail.get('disclosure_announcement_date', ''))
        doc_info = context.get('doc_info', {})
        doc_location_url = doc_info.get('doc_location_url', '')

        pages = json_content.get('pages', [])

        # Get building assets for this submitter from step_6
        submitter_assets = assets_by_submitter.get(submitter_id, [])

        # Use first available asset_id as starting point, or default
        start_asset_id = int(submitter_assets[0]['asset_id']) if submitter_assets else 2623

        # Try to use page_metadata with text_each_page first (more flexible, no polygon dependency)
        building_info = []
        if doc_location_url and page_metadata:
            step8_pages = loader.get_step_pages(doc_location_url, 'step_8')
            if step8_pages:
                # Try text_each_page content-based extraction first
                for page_num in step8_pages:
                    page_text = loader.load_page_text(doc_location_url, page_num)
                    if page_text:
                        page_buildings = extract_building_from_page_content(
                            page_text, nacc_id, submitter_id,
                            latest_submitted_date, start_asset_id + len(building_info)
                        )
                        building_info.extend(page_buildings)

        # Fall back to polygon-based extraction if text_each_page didn't yield results
        if not building_info:
            building_pages = find_building_pages_with_metadata(pages, loader, doc_location_url)
            building_info = extract_building_info(building_pages, nacc_id, submitter_id,
                                                   latest_submitted_date, start_asset_id)

        # Match extracted building info with assets from step_6 by index
        for i, info in enumerate(building_info):
            if i < len(submitter_assets):
                # Use asset_id from step_6 matched by submitter_id + index
                info['asset_id'] = submitter_assets[i]['asset_id']

        all_building_info.extend(building_info)

    building_fields = ['asset_id', 'submitter_id', 'nacc_id', 'building_doc_number',
                       'sub_district', 'district', 'province', 'latest_submitted_date']
    writer = CSVWriter(output_dir, 'asset_building_info.csv', building_fields)
    filtered_rows = [{k: v for k, v in info.items() if k in building_fields} for info in all_building_info]
    count = writer.write_rows(filtered_rows)
    print(f"Extracted {count} building infos to {writer.output_path}")


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_8(input_dir, output_dir)
