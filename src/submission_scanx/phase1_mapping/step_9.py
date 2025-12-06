"""
Step 9: Extract vehicle asset detailed information from JSON extract files

This step extracts:
- asset_vehicle_info.csv - Vehicle asset details (registration, model, province)

Uses page_metadata and text_each_page for flexible content-based extraction.
Falls back to polygon-based detection for compatibility.

Vehicle page structure (x positions) - used for polygon fallback:
- Row number: ~0.5
- Vehicle type: ~1.0-1.5
- Brand/Model: ~1.6-2.5
- Registration: ~2.6-3.2
- Province: ~3.3-4.0
"""

import os
import re
from typing import List, Dict, Tuple
import csv

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


# Vehicle type keywords - to be removed from model names
VEHICLE_TYPE_KEYWORDS = ['รถยนต์', 'รถจักรยานยนต์', 'รถบรรทุก', 'รถตู้', 'รถกระบะ',
                          'เรือ', 'เครื่องบิน', 'รถพ่วง', 'รถไถ', 'ยานพาหนะ',
                          'รถเก๋ง', 'รถนั่ง', 'จักรยานยนต์', 'มอเตอร์ไซค์']

# Province names (common ones for validation)
PROVINCE_KEYWORDS = ['กรุงเทพ', 'กทม', 'นนทบุรี', 'ปทุมธานี', 'สมุทรปราการ',
                     'นครราชสีมา', 'เชียงใหม่', 'ขอนแก่น', 'ชลบุรี', 'ภูเก็ต',
                     'เชียงราย', 'นครปฐม', 'สุพรรณบุรี', 'ระยอง', 'พิษณุโลก',
                     'อุดรธานี', 'สงขลา', 'นครสวรรค์', 'พระนครศรีอยุธยา']


def clean_vehicle_model(model: str) -> str:
    """Remove vehicle type prefixes from model name."""
    if not model:
        return ''
    result = model.strip()
    # Remove vehicle type prefixes
    for keyword in VEHICLE_TYPE_KEYWORDS:
        result = re.sub(rf'^{keyword}\s*', '', result, flags=re.IGNORECASE)
        result = re.sub(rf'\s+{keyword}\s+', ' ', result, flags=re.IGNORECASE)
    # Clean up extra spaces
    result = re.sub(r'\s+', ' ', result).strip()
    # Skip if only vehicle type remains
    if result.lower() in [k.lower() for k in VEHICLE_TYPE_KEYWORDS]:
        return ''
    return result


def is_valid_province(text: str) -> bool:
    """Check if text looks like a valid province name (not a date)."""
    if not text:
        return False
    text = text.strip()
    # Check if it's a date pattern
    if re.search(r'\d{1,2}\s*[/\.\-]\s*\d{1,2}', text):
        return False
    if re.search(r'(ม\.ค\.|ก\.พ\.|มี\.ค\.|เม\.ย\.|พ\.ค\.|มิ\.ย\.|ก\.ค\.|ส\.ค\.|ก\.ย\.|ต\.ค\.|พ\.ย\.|ธ\.ค\.)', text):
        return False
    if re.search(r'(มกราคม|กุมภาพันธ์|มีนาคม|เมษายน|พฤษภาคม|มิถุนายน|กรกฎาคม|สิงหาคม|กันยายน|ตุลาคม|พฤศจิกายน|ธันวาคม)', text):
        return False
    # Check if it contains province keywords
    for prov in PROVINCE_KEYWORDS:
        if prov in text:
            return True
    return False


def find_vehicle_pages(pages: List[Dict]) -> List[Tuple[int, Dict]]:
    """Find pages with vehicle asset data using content-based detection"""
    result = []
    for i, page in enumerate(pages):
        lines = page.get('lines', [])
        page_text = ' '.join([l.get('content', '') for l in lines])
        if 'รายละเอียดประกอบรายการยานพาหนะ' in page_text:
            result.append((i, page))
    return result


def find_vehicle_pages_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Tuple[int, Dict]]:
    """
    Find pages with vehicle asset data using page_metadata first, then fallback to content detection.

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
        step9_pages = loader.get_step_pages(doc_location_url, 'step_9')
        if step9_pages:
            for page_num in step9_pages:
                page_idx = page_num - 1  # Convert 1-indexed to 0-indexed
                if 0 <= page_idx < len(pages):
                    result.append((page_idx, pages[page_idx]))

    # Fallback to content-based detection if metadata not available or empty
    if not result:
        result = find_vehicle_pages(pages)

    return result


def extract_vehicle_from_page_content(page_data: Dict, nacc_id: str, submitter_id: str,
                                       latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """
    Extract vehicle info from text_each_page using content-based parsing.
    Uses the 'content' field which contains the full page text.
    More flexible than polygon-based extraction.
    """
    vehicle_infos = []
    asset_id = start_asset_id

    content = page_data.get('content', '')
    if not content:
        return []

    # Convert Thai numerals
    content = convert_thai_numerals(content)

    # Split into lines
    lines = content.split('\n')

    # Pattern to match vehicle rows:
    # Format: [row_num]. [vehicle_type] [brand/model] [registration] [province] ...
    # Example: "1. รถยนต์ TOYOTA CAMRY กก1234 กรุงเทพมหานคร ..."

    # Pattern for vehicle line - starts with number
    vehicle_row_pattern = re.compile(
        r'^[C\s]*(\d{1,2})[\.\s]+' +  # Row number (may have 'C' prefix from OCR)
        r'(รถยนต์|รถจักรยานยนต์|รถบรรทุก|รถตู้|รถกระบะ|เรือ|เครื่องบิน|รถพ่วง|รถไถ)?\s*' +  # Optional vehicle type
        r'(.+)$',  # Rest of line (brand, model, registration, province)
        re.IGNORECASE
    )

    # Registration number pattern: Thai chars + numbers (e.g., กก1234, 1กก1234)
    registration_pattern = re.compile(r'([ก-ฮ]{1,3}\s*\d{1,4}|\d[ก-ฮ]{1,3}\s*\d{1,4})')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Skip header/footer lines
        if any(skip in line for skip in ['ลับ', 'หน้า', 'ลงชื่อ', 'หมายเหตุ', 'ผย.', 'คส.',
                                          'รายละเอียดประกอบ', 'ประเภท', 'ทะเบียน', 'ยี่ห้อ']):
            continue

        # Try to match vehicle row pattern
        match = vehicle_row_pattern.match(line)

        if match:
            row_num = match.group(1)
            vehicle_type = match.group(2) or ''
            rest_of_line = match.group(3).strip()

            # Extract registration number
            reg_match = registration_pattern.search(rest_of_line)
            registration_number = ''
            if reg_match:
                registration_number = reg_match.group(1).replace(' ', '')

            # Extract province - look for Thai province names
            province = ''
            for prov in PROVINCE_KEYWORDS:
                if prov in rest_of_line:
                    # Extract province name more precisely
                    prov_match = re.search(rf'({prov}[ก-ฮ]*)', rest_of_line)
                    if prov_match:
                        province = prov_match.group(1)
                        break

            # Extract vehicle model - words before registration
            vehicle_model = ''
            if reg_match:
                model_text = rest_of_line[:reg_match.start()].strip()
                # Clean up model text
                model_text = re.sub(r'\s+', ' ', model_text)
                if len(model_text) > 2:
                    vehicle_model = model_text

            # Create vehicle info record
            vehicle_item = {
                'asset_id': asset_id,
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'registration_number': registration_number,
                'vehicle_model': vehicle_model,
                'province': province,
                'latest_submitted_date': latest_submitted_date
            }

            # Only add if we have registration number
            if vehicle_item['registration_number']:
                vehicle_infos.append(vehicle_item)
                asset_id += 1

    return vehicle_infos


def extract_vehicle_info(pages: List[Tuple[int, Dict]], nacc_id: str, submitter_id: str,
                         latest_submitted_date: str, start_asset_id: int) -> List[Dict]:
    """Extract vehicle asset info from vehicle pages using two-pass approach.
    
    First pass: identify all row numbers and their y-positions
    Second pass: collect all content for each row
    """
    vehicle_infos = []
    asset_id = start_asset_id

    for _, page in pages:
        lines = page.get('lines', [])
        
        # Find header y position by looking for column headers
        header_y = 2.5  # Default
        footer_y = 10.5  # Default
        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            _, cy = get_polygon_center(polygon)
            if any(kw in content for kw in ['ลำดับ', 'หมายเลข', 'ทะเบียน', 'ประเภท', 'จังหวัด']):
                if 2.5 < cy < 4.0:
                    header_y = max(header_y, cy + 0.2)
            if 'หมายเหตุ' in content and cy > 8.0:
                footer_y = min(footer_y, cy - 0.2)

        # First pass: identify all row numbers with their y positions
        row_info = []
        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            if cy < header_y or cy > footer_y:
                continue

            # Look for row number - match "1", "1.", "1 ." etc.
            row_match = re.match(r'^(\d{1,2})[\.\s]*$', content)
            if cx < 1.2 and row_match:
                row_info.append({'index': int(row_match.group(1)), 'y': cy})

        # Sort rows by y position
        row_info = sorted(row_info, key=lambda x: x['y'])

        # Build row_contents: map each row index to its contents
        row_contents = {r['index']: {'y': r['y'], 'contents': []} for r in row_info}

        # Second pass: assign each content line to its closest row
        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            if cy < header_y or cy > footer_y:
                continue

            # Skip row numbers themselves
            if cx < 1.2 and re.match(r'^(\d{1,2})[\.\s]*$', content):
                continue

            # Find the closest row for this content (within y tolerance)
            min_dist = float('inf')
            closest_row = None
            for r in row_info:
                dist = abs(cy - r['y'])
                if dist < min_dist and dist < 2.0:  # y tolerance
                    min_dist = dist
                    closest_row = r['index']

            if closest_row is not None:
                row_contents[closest_row]['contents'].append({
                    'content': content,
                    'cx': cx,
                    'cy': cy
                })

        # Process each row's contents
        for row_idx, info in row_contents.items():
            current_item = {
                'asset_id': asset_id,
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'registration_number': '',
                'vehicle_model': '',
                'province': '',
                'y_pos': info['y'],
                'latest_submitted_date': latest_submitted_date
            }
            model_parts = []

            for item in info['contents']:
                content = item['content']
                cx = item['cx']

                # Vehicle type/model/brand (x ~1.0-3.2)
                if 1.0 <= cx <= 3.2:
                    model_text = clean_text(content)
                    # Skip if it's just a number or too short
                    if len(model_text) > 1 and not re.match(r'^[\d\-\.]+$', model_text):
                        # Skip column headers
                        if model_text not in ['ประเภท', 'ยี่ห้อ', 'รุ่น']:
                            model_parts.append(model_text)

                # Registration number (x ~3.0-4.2)
                if 3.0 <= cx <= 4.2:
                    reg_text = clean_text(content)
                    # Extract registration pattern from potentially merged text
                    # Patterns: กก1234, 1กก1234, กก 1234, 12-2660
                    # Also handles merged text like "ศศ909กรุงเทพมหานคร"
                    reg_match = re.match(r'^(\d?[ก-ฮ]{1,3}\s*[\d]{1,4})', reg_text)
                    if reg_match:
                        reg_clean = reg_match.group(1).replace(' ', '').replace('.', '')
                        current_item['registration_number'] = reg_clean
                    elif re.match(r'^\d{1,2}[\-]\d{4}$', reg_text):
                        current_item['registration_number'] = reg_text
                    # Fallback: if Thai chars + digits anywhere
                    elif re.search(r'[ก-ฮ]', reg_text) and re.search(r'\d', reg_text):
                        reg_clean = reg_text.replace(' ', '').replace('.', '')
                        # Try to extract just the registration part (stop at Thai province chars)
                        parts = re.split(r'(กรุงเทพ|กทม|นนทบุรี|ปทุมธานี|\d{1,2}/\d{1,2}/)', reg_clean)
                        if parts:
                            current_item['registration_number'] = parts[0]

                # Province (x ~4.0-5.2)
                if 4.0 <= cx <= 5.2:
                    prov_text = clean_text(content)
                    # Skip if it's a date or number
                    if '/' in prov_text or re.match(r'^[\d,\.]+$', prov_text):
                        continue
                    if 'กรุงเทพ' in prov_text or 'กทม' in prov_text:
                        current_item['province'] = prov_text
                    elif len(prov_text) > 2 and re.search(r'[ก-ฮ]', prov_text):
                        if not any(kw in prov_text for kw in ['วัน', 'เดือน', 'ปี', 'บาท', 'ก.ย.', 'ม.ค.']):
                            current_item['province'] = prov_text

            # Set vehicle model - clean up by removing type prefixes
            if model_parts:
                raw_model = ' '.join(model_parts)
                current_item['vehicle_model'] = clean_vehicle_model(raw_model)

            # Validate province - filter out dates
            if current_item['province'] and not is_valid_province(current_item['province']):
                current_item['province'] = ''

            # Add if we have registration number or model
            if current_item['registration_number'] or current_item['vehicle_model']:
                vehicle_infos.append(current_item)
                asset_id += 1

    return vehicle_infos


def load_vehicle_assets_for_matching(output_dir: str) -> Dict[str, List[Dict]]:
    """
    Load asset.csv and create a lookup by submitter_id for matching vehicle assets.
    Returns dict: submitter_id -> list of vehicle assets (asset_type_id 18-21, 38)
    """
    assets_by_submitter = {}
    asset_path = os.path.join(output_dir, 'asset.csv')

    if not os.path.exists(asset_path):
        return {}

    # Vehicle asset type IDs: 18-21 (รถยนต์, รถจักรยานยนต์, etc.) and 38 (other vehicles)
    vehicle_type_ids = set(range(18, 22)) | {38}

    with open(asset_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            asset_type_id = int(row.get('asset_type_id', 0))
            if asset_type_id in vehicle_type_ids:
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


def run_step_9(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 9 to extract vehicle asset information.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    loader = data_loader or PipelineDataLoader(input_dir)

    all_vehicle_info = []

    # Load assets from step_6 for matching by submitter_id
    assets_by_submitter = load_vehicle_assets_for_matching(output_dir)

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

        # Get vehicle assets for this submitter from step_6
        submitter_assets = assets_by_submitter.get(submitter_id, [])

        # Use first available asset_id as starting point, or default
        start_asset_id = int(submitter_assets[0]['asset_id']) if submitter_assets else 2630

        # Try to use page_metadata with text_each_page first (more flexible, no polygon dependency)
        vehicle_info = []
        if doc_location_url and page_metadata:
            step9_pages = loader.get_step_pages(doc_location_url, 'step_9')
            if step9_pages:
                # Try text_each_page content-based extraction first
                for page_num in step9_pages:
                    page_text = loader.load_page_text(doc_location_url, page_num)
                    if page_text:
                        page_vehicles = extract_vehicle_from_page_content(
                            page_text, nacc_id, submitter_id,
                            latest_submitted_date, start_asset_id + len(vehicle_info)
                        )
                        vehicle_info.extend(page_vehicles)

        # Fall back to polygon-based extraction if text_each_page didn't yield results
        if not vehicle_info:
            vehicle_pages = find_vehicle_pages_with_metadata(pages, loader, doc_location_url)
            vehicle_info = extract_vehicle_info(vehicle_pages, nacc_id, submitter_id,
                                                 latest_submitted_date, start_asset_id)

        # Match extracted vehicle info with assets from step_6 by index
        for i, info in enumerate(vehicle_info):
            if i < len(submitter_assets):
                # Use asset_id from step_6 matched by submitter_id + index
                info['asset_id'] = submitter_assets[i]['asset_id']

        all_vehicle_info.extend(vehicle_info)

    vehicle_fields = ['asset_id', 'submitter_id', 'nacc_id', 'registration_number',
                      'vehicle_model', 'province', 'latest_submitted_date']
    writer = CSVWriter(output_dir, 'asset_vehicle_info.csv', vehicle_fields)
    filtered_rows = [{k: v for k, v in info.items() if k in vehicle_fields} for info in all_vehicle_info]
    count = writer.write_rows(filtered_rows)
    print(f"Extracted {count} vehicle infos to {writer.output_path}")


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_9(input_dir, output_dir)
