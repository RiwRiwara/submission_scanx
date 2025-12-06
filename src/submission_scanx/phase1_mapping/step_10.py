"""
Step 10: Extract other asset detailed information from JSON extract files

This step extracts:
- asset_other_asset_info.csv - Other asset details (count, unit)

Uses page_metadata and text_each_page for flexible content-based extraction.
Falls back to polygon-based detection for compatibility.

Other asset page structure (x positions) - used for polygon fallback:
- Row number: ~0.5-0.9
- Asset description: ~1.0-3.5
- Count: ~3.7-4.1
- Unit: ~3.7-4.5 (same column as count, units are text)
- Date: ~4.3-5.0
- Valuation: ~5.8-6.5
- Owner checkmarks: ~6.8-8.0
"""

import os
import re
from typing import List, Dict, Tuple

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import get_polygon_center, clean_text, format_disclosure_date
from utils.data_loader import PipelineDataLoader, CSVWriter


# Thai numeral conversion
THAI_DIGITS = {'๐': '0', '๑': '1', '๒': '2', '๓': '3', '๔': '4',
               '๕': '5', '๖': '6', '๗': '7', '๘': '8', '๙': '9'}


def convert_thai_numerals(text: str) -> str:
    """Convert Thai numerals to Arabic numerals"""
    if not text:
        return text
    for thai, arabic in THAI_DIGITS.items():
        text = text.replace(thai, arabic)
    return text


def find_other_asset_pages(pages: List[Dict]) -> List[Tuple[int, Dict]]:
    """Find pages with other asset data using content-based detection"""
    result = []
    for i, page in enumerate(pages):
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])
        # Match both main page and continuation pages (ต่อ)
        if 'รายละเอียดประกอบรายการทรัพย์สินอื่น' in page_text:
            result.append((i, page))
    return result


def find_other_asset_pages_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Tuple[int, Dict]]:
    """
    Find pages with other asset data using page_metadata first, then fallback to content detection.

    Args:
        pages: List of page dictionaries from JSON
        loader: PipelineDataLoader instance for page_metadata lookup
        doc_location_url: Document location URL for metadata lookup

    Returns:
        List of (page_index, page_dict) tuples
    """
    result = []

    # Try page_metadata first
    if loader and doc_location_url:
        step10_pages = loader.get_step_pages(doc_location_url, 'step_10')
        if step10_pages:
            for page_num in step10_pages:
                page_idx = page_num - 1  # Convert 1-indexed to 0-indexed
                if 0 <= page_idx < len(pages):
                    result.append((page_idx, pages[page_idx]))

    # Fallback to content-based detection if metadata not available or empty
    if not result:
        result = find_other_asset_pages(pages)

    return result


def extract_other_asset_from_page_content(page_data: Dict, nacc_id: str, submitter_id: str,
                                           latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """
    Extract other asset info from text_each_page using content-based parsing.
    Uses the 'content' field which contains the full page text.
    More flexible than polygon-based extraction.
    """
    other_infos = []
    asset_id = start_asset_id

    content = page_data.get('content', '')
    if not content:
        return []

    # Convert Thai numerals
    content = convert_thai_numerals(content)

    # Split into lines
    lines = content.split('\n')

    # Pattern to match other asset rows:
    # Format: [row_num]. [asset_description] [count] [unit] [date] [valuation] ...
    # Example: "1. นาฬิกา ROLEX 2 เรือน 25/12/50 500,000 ..."

    # Pattern for other asset line - starts with row number
    asset_row_pattern = re.compile(
        r'^[C\s]*(\d{1,3})[\.\s]+' +  # Row number (may have 'C' prefix from OCR)
        r'(.+)$',  # Rest of line (description, count, unit, etc.)
        re.IGNORECASE
    )

    # Pattern for count and unit: number followed by Thai unit word
    count_unit_pattern = re.compile(r'(\d{1,3})\s*([ก-๙]{1,10})?')

    # Common Thai unit words
    unit_keywords = ['เรือน', 'ชิ้น', 'อัน', 'วง', 'เส้น', 'ใบ', 'คัน', 'ตัว',
                     'บาท', 'องค์', 'แหวน', 'สร้อย', 'กำไล', 'พระ']

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip header/footer lines
        if any(skip in line for skip in ['ลับ', 'หน้า', 'ลงชื่อ', 'หมายเหตุ', 'ผย.', 'คส.',
                                          'รายละเอียดประกอบ', 'ประเภท', 'จำนวน', 'หน่วย', 'มูลค่า']):
            continue

        # Try to match asset row pattern
        match = asset_row_pattern.match(line)

        if match:
            row_num = match.group(1)
            rest_of_line = match.group(2).strip()

            # Skip very short lines (likely just row numbers)
            if len(rest_of_line) < 3:
                continue

            # Skip if looks like a year (2500+)
            if re.match(r'^25\d{2}$', rest_of_line):
                continue

            # Try to extract count and unit
            count_val = 0
            unit_val = ''

            # Look for count+unit patterns in the line
            # Common patterns: "2 เรือน", "1 วง", "5 ชิ้น"
            count_matches = count_unit_pattern.findall(rest_of_line)
            for cnt, unt in count_matches:
                try:
                    num = int(cnt)
                    # Valid count range (1-99)
                    if 1 <= num <= 99:
                        count_val = num
                        if unt and len(unt) <= 10:
                            # Check if it's a valid unit
                            for keyword in unit_keywords:
                                if keyword in unt:
                                    unit_val = unt
                                    break
                            if not unit_val and re.match(r'^[ก-๙]+$', unt):
                                unit_val = unt
                        break
                except ValueError:
                    continue

            # If no count found, try alternative patterns
            if count_val == 0:
                # Look for standalone numbers followed by Thai text
                simple_match = re.search(r'(\d{1,2})\s+([ก-๙]+)', rest_of_line)
                if simple_match:
                    try:
                        num = int(simple_match.group(1))
                        if 1 <= num <= 99:
                            count_val = num
                            unit_val = simple_match.group(2)[:10]
                    except ValueError:
                        pass

            # Create other asset info record
            if count_val > 0:
                asset_item = {
                    'asset_id': asset_id,
                    'submitter_id': submitter_id,
                    'nacc_id': nacc_id,
                    'count': count_val,
                    'unit': clean_text(unit_val) if unit_val else '',
                    'latest_submitted_date': latest_submitted_date
                }
                other_infos.append(asset_item)
                asset_id += 1

    return other_infos


def extract_other_asset_info(pages: List[Tuple[int, Dict]], nacc_id: str, submitter_id: str,
                              latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """Extract other asset info from other asset pages.

    Strategy:
    1. First pass: Find all row numbers and their y positions
    2. Second pass: Associate each content line with its closest row number
    3. Extract count and unit from the count/unit column (x ~3.5-4.6)
    """
    other_infos = []
    asset_id = start_asset_id

    for _, page in pages:
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])

        # Determine header height based on page type
        is_continuation = '(ต่อ)' in page_text
        header_y = 1.3 if is_continuation else 2.5  # Lowered from 2.7 to catch content

        # First pass: Find all row numbers with their y positions
        row_numbers = []
        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers/footers
            if cy < header_y or cy > 10.5:
                continue

            # Row numbers are in leftmost column
            if cx < 1.5 and re.match(r'^\d{1,3}$', content):
                row_num = int(content)
                if 1 <= row_num <= 500:  # Valid row number range
                    row_numbers.append({'num': row_num, 'y': cy})

        # Sort row numbers by y position
        row_numbers = sorted(row_numbers, key=lambda x: x['y'])

        if not row_numbers:
            continue

        # Create items for each row number
        row_items = {}
        for r in row_numbers:
            row_items[r['num']] = {
                'asset_id': asset_id,
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'count': 0,
                'unit': '',
                'y_pos': r['y'],
                'row_num': r['num'],
                'latest_submitted_date': latest_submitted_date
            }
            asset_id += 1

        # Second pass: Associate content with closest row
        for line in lines:
            content = line.get('content', '').strip()
            if not content:
                continue

            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers/footers
            if cy < header_y or cy > 10.5:
                continue

            # Skip row numbers themselves
            if cx < 1.5 and re.match(r'^\d{1,3}$', content):
                continue

            # Find closest row number (within tolerance)
            closest_row = None
            min_dist = 0.8  # y tolerance
            for r in row_numbers:
                dist = abs(cy - r['y'])
                if dist < min_dist:
                    min_dist = dist
                    closest_row = r['num']

            if closest_row is None:
                continue

            item = row_items.get(closest_row)
            if not item:
                continue

            # Count and unit column (x ~3.5-4.3) - tightened to avoid date column
            if 3.5 <= cx <= 4.3:
                # Skip patterns that are clearly not count/unit
                if 'ไม่พบ' in content or 'รายละเอียด' in content:
                    continue
                # Skip if this looks like a year (2500+)
                if re.match(r'^25\d{2}$', content):
                    continue

                # Parse patterns like "7 บาท", "9 องค์", "1", etc.
                # First try to extract number from pattern like "7 บาท กับ"
                count_match = re.match(r'^(\d{1,3})\s*([ก-๙]+)?', content)
                if count_match:
                    count_val = int(count_match.group(1))
                    # Valid count range (1-99 for most items)
                    if 1 <= count_val <= 99:
                        item['count'] = count_val
                    unit_part = count_match.group(2)
                    if unit_part and len(unit_part) <= 10:
                        item['unit'] = unit_part
                # Also check for standalone Thai unit text
                elif re.match(r'^[ก-๙]+$', content) and len(content) <= 10:
                    if not item['unit']:  # Don't override if already set
                        # Skip common non-unit words
                        if content not in ['หน่วย', 'จำนวน', 'รายการ']:
                            item['unit'] = clean_text(content)

        # Collect valid items
        for row_num in sorted(row_items.keys()):
            item = row_items[row_num]
            if item['count'] > 0:
                other_infos.append(item)

    return other_infos


def run_step_10(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 10 to extract other asset information.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    loader = data_loader or PipelineDataLoader(input_dir)

    all_other_info = []
    other_asset_id = 2637

    # Load page_metadata index
    page_metadata = loader.page_metadata

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        doc_info = context.get('doc_info', {})
        nacc_id = nacc_detail.get('nacc_id', '')
        submitter_id = nacc_detail.get('submitter_id', '')
        latest_submitted_date = format_disclosure_date(nacc_detail.get('disclosure_announcement_date', ''))
        doc_location_url = doc_info.get('doc_location_url', '')

        pages = json_content.get('pages', [])

        # Try to use page_metadata with text_each_page first (more flexible, no polygon dependency)
        other_info = []
        if doc_location_url and page_metadata:
            step10_pages = loader.get_step_pages(doc_location_url, 'step_10')
            if step10_pages:
                # Try text_each_page content-based extraction first
                for page_num in step10_pages:
                    page_text = loader.load_page_text(doc_location_url, page_num)
                    if page_text:
                        page_others = extract_other_asset_from_page_content(
                            page_text, nacc_id, submitter_id,
                            latest_submitted_date, other_asset_id + len(other_info)
                        )
                        other_info.extend(page_others)

        # Fall back to polygon-based extraction if text_each_page didn't yield results
        if not other_info:
            other_pages = find_other_asset_pages_with_metadata(pages, loader, doc_location_url)
            other_info = extract_other_asset_info(other_pages, nacc_id, submitter_id,
                                                   latest_submitted_date, other_asset_id)

        all_other_info.extend(other_info)
        other_asset_id += len(other_info)

    other_fields = ['asset_id', 'submitter_id', 'nacc_id', 'count', 'unit', 'latest_submitted_date']
    writer = CSVWriter(output_dir, 'asset_other_asset_info.csv', other_fields)
    filtered_rows = [{k: v for k, v in info.items() if k in other_fields} for info in all_other_info]
    count = writer.write_rows(filtered_rows)
    print(f"Extracted {count} other asset infos to {writer.output_path}")


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_10(input_dir, output_dir)
