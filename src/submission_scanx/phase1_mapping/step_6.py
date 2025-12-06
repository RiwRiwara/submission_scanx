"""
Step 6: Extract asset information from JSON extract files

This step extracts:
- asset.csv - Asset records

Asset data is found in pages ~12-32 depending on document:
- รายละเอียดประกอบรายการที่ดิน (Land) - asset_type_id: 1-9, 36
- รายละเอียดประกอบรายการโรงเรือนและสิ่งปลูกสร้าง (Building) - asset_type_id: 10-17, 37
- รายละเอียดประกอบรายการยานพาหนะ (Vehicle) - asset_type_id: 18-21, 38
- รายละเอียดประกอบรายการสิทธิและสัมปทาน (Rights) - asset_type_id: 22-27, 39
- รายละเอียดประกอบรายการทรัพย์สินอื่น (Other) - asset_type_id: 28-35

date_acquiring_type_id:
1 = มีข้อมูล (has data)
2 = ไม่ทราบวันที่แน่ชัด (unknown exact date)
3 = ปัจจุบัน (current)
4 = ไม่มีข้อมูลในเอกสาร (no data in document)

date_ending_type_id:
1 = มีข้อมูล (has data)
2 = ปัจจุบัน (current)
3 = ตลอดชีพ (lifetime)
4 = ไม่มีข้อมูลในเอกสาร (no data in document)

asset_acquisition_type_id:
6 = ไม่ได้ระบุในเอกสาร (default - not specified)
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
    THAI_MONTHS,
    get_polygon_center,
    clean_number,
    clean_text,
    parse_thai_date,
    format_disclosure_date,
)
from utils.data_loader import PipelineDataLoader, CSVWriter

# Import improved asset type detection
from .asset_types import (
    detect_asset_type,
    detect_land_type,
    detect_building_type,
    detect_vehicle_type,
    detect_rights_type,
    detect_other_type,
    LAND_PATTERNS,
    BUILDING_PATTERNS,
    VEHICLE_PATTERNS,
    RIGHTS_PATTERNS,
    OTHER_PATTERNS,
)

# Import layout detector for adaptive thresholds
from utils.layout_detector import LayoutDetector, get_detector


def find_asset_pages(pages: List[Dict]) -> Dict[str, List[Tuple[int, Dict]]]:
    """Find pages for each asset type using _page_info metadata first, then content fallback"""
    result = {
        'land': [],
        'building': [],
        'vehicle': [],
        'rights': [],
        'other': []
    }
    
    # Map page_type from _page_info to our asset categories
    page_type_map = {
        'land': 'land',
        'buildings': 'building',
        'vehicles': 'vehicle',
        'concessions': 'rights',
        'other_assets': 'other'
    }

    for i, page in enumerate(pages):
        # First check _page_info metadata (from json_processor.py)
        page_info = page.get('_page_info', {})
        page_type = page_info.get('page_type', '')
        
        if page_type in page_type_map:
            category = page_type_map[page_type]
            result[category].append((i, page))
        else:
            # Fallback: check content markers
            lines = page.get('lines', [])
            page_text = ' '.join([l.get('content', '') for l in lines])

            if 'รายละเอียดประกอบรายการที่ดิน' in page_text:
                if (i, page) not in result['land']:
                    result['land'].append((i, page))
            elif 'รายละเอียดประกอบรายการโรงเรือนและสิ่งปลูกสร้าง' in page_text:
                if (i, page) not in result['building']:
                    result['building'].append((i, page))
            elif 'รายละเอียดประกอบรายการยานพาหนะ' in page_text:
                if (i, page) not in result['vehicle']:
                    result['vehicle'].append((i, page))
            elif 'รายละเอียดประกอบรายการสิทธิและสัมปทาน' in page_text:
                if (i, page) not in result['rights']:
                    result['rights'].append((i, page))
            elif 'รายละเอียดประกอบรายการทรัพย์สินอื่น' in page_text:
                if (i, page) not in result['other']:
                    result['other'].append((i, page))

    return result


# Legacy type dictionaries kept for backward compatibility in extraction functions
# The actual detection is now done by detect_asset_type() from asset_types module
LAND_TYPES = {1: 'โฉนด', 2: 'ส.ป.ก', 3: 'ส.ป.ก 4-01', 4: 'น.ส.3', 5: 'น.ส.3ก',
              6: 'ภบท.5', 7: 'อ.ช.2', 8: 'สัญญาซื้อขาย', 9: 'น.ค.3'}


def determine_asset_type_id(text: str, category: str) -> Tuple[int, str]:
    """Determine asset_type_id based on text and category using improved regex patterns.
    This is a wrapper around detect_asset_type() from asset_types module.

    Returns (asset_type_id, asset_type_other)
    """
    return detect_asset_type(text, category)


def determine_owner(lines: List[Dict], y_pos: float, y_tolerance: float = 0.3) -> Tuple[bool, bool, bool]:
    """Determine ownership (submitter, spouse, child) based on checkmarks
    Returns (owner_by_submitter, owner_by_spouse, owner_by_child)
    """
    # Look for checkmarks (/, ✓, V, etc.) in the owner columns
    # X positions: ผย (submitter) ~7.0-7.3, คส (spouse) ~7.3-7.6, บ (child) ~7.6-8.0

    submitter = False
    spouse = False
    child = False

    for line in lines:
        content = line.get('content', '').strip()
        polygon = line.get('polygon', [0]*8)
        cx, cy = get_polygon_center(polygon)

        # Check if in same row
        if abs(cy - y_pos) > y_tolerance:
            continue

        # Check for checkmark characters
        if content in ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔']:
            if 6.9 <= cx <= 7.35:
                submitter = True
            elif 7.35 <= cx <= 7.65:
                spouse = True
            elif 7.65 <= cx <= 8.1:
                child = True

    # Default to submitter if no checkmarks found
    if not submitter and not spouse and not child:
        submitter = True

    return submitter, spouse, child


def extract_land_assets(pages: List[Tuple[int, Dict]], all_pages: List[Dict]) -> List[Dict]:
    """Extract land assets from land pages using adaptive layout detection"""
    assets = []
    detector = get_detector()

    for page_idx, page in pages:
        lines = page.get('lines', [])

        # Auto-detect layout for this page
        layout = detector.detect_page_layout(lines, 'land')

        sorted_lines = sorted(lines, key=lambda x: (get_polygon_center(x.get('polygon', [0]*8))[1],
                                                     get_polygon_center(x.get('polygon', [0]*8))[0]))

        current_asset = None

        for line in sorted_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers and footers using detected boundaries
            if cy < layout['header_y_min'] or cy > layout['footer_y_max']:
                continue

            # Look for row number using detected column boundary
            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,2}$', content):
                # Save previous asset if exists
                if current_asset and current_asset.get('valuation'):
                    assets.append(current_asset)

                row_num = int(content)
                current_asset = {
                    'index': row_num,
                    'y_pos': cy,
                    'asset_type_id': 1,  # Default to โฉนด
                    'asset_type_other': '',
                    'asset_name': 'โฉนด',
                    'date_acquiring_type_id': 1,
                    'acquiring_date': None,
                    'acquiring_month': None,
                    'acquiring_year': None,
                    'date_ending_type_id': 4,
                    'ending_date': None,
                    'ending_month': None,
                    'ending_year': None,
                    'asset_acquisition_type_id': 6,
                    'valuation': None,
                    'owner_by_submitter': True,
                    'owner_by_spouse': False,
                    'owner_by_child': False
                }

            if not current_asset:
                continue

            # Check if this line is part of current asset (within y range)
            if abs(cy - current_asset['y_pos']) > 1.0:
                continue

            # Look for land type using detected type column range
            type_x_min, type_x_max = layout['type_col_x_range']
            if type_x_min <= cx <= type_x_max:
                # Skip if content is just a number (document number like 114172, 23111)
                if re.match(r'^\d+$', content):
                    continue

                type_id, type_other = detect_land_type(content)
                if type_id:
                    current_asset['asset_type_id'] = type_id
                    current_asset['asset_type_other'] = type_other
                    # Only update asset_name if it contains land type keywords
                    if any(kw in content for kw in ['โฉนด', 'ส.ป.ก', 'สปก', 'น.ส.', 'นส.', 'ภบท', 'อ.ช.', 'สัญญา', 'น.ค.']):
                        current_asset['asset_name'] = content[:100]

            # Look for date using detected date column range
            date_x_min, date_x_max = layout['date_col_x_range']
            if date_x_min <= cx <= date_x_max:
                day, month, year = parse_thai_date(content)
                if year:
                    current_asset['acquiring_year'] = year
                    if month:
                        current_asset['acquiring_month'] = month
                    if day:
                        current_asset['acquiring_date'] = day
                    current_asset['date_acquiring_type_id'] = 1
                elif 'ไม่พบ' in content:
                    current_asset['date_acquiring_type_id'] = 2

            # Look for valuation using detected value column range
            value_x_min, value_x_max = layout['value_col_x_range']
            if value_x_min <= cx <= value_x_max:
                val = clean_number(content)
                if val and val >= 1000:
                    current_asset['valuation'] = val

            # Check ownership marks using detected owner column range
            owner_x_min, owner_x_max = layout['owner_col_x_range']
            if owner_x_min <= cx <= owner_x_max:
                if content in ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔']:
                    # Divide owner column into 3 parts
                    owner_width = owner_x_max - owner_x_min
                    submitter_max = owner_x_min + owner_width * 0.33
                    spouse_max = owner_x_min + owner_width * 0.67

                    if cx <= submitter_max:
                        current_asset['owner_by_submitter'] = True
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = False
                    elif cx <= spouse_max:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = True
                        current_asset['owner_by_child'] = False
                    else:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = True

        # Save last asset
        if current_asset and current_asset.get('valuation'):
            assets.append(current_asset)

    return assets


def extract_building_assets(pages: List[Tuple[int, Dict]], all_pages: List[Dict]) -> List[Dict]:
    """Extract building assets from building pages using adaptive layout detection"""
    assets = []
    detector = get_detector()

    for page_idx, page in pages:
        lines = page.get('lines', [])

        # Auto-detect layout for this page
        layout = detector.detect_page_layout(lines, 'building')

        sorted_lines = sorted(lines, key=lambda x: (get_polygon_center(x.get('polygon', [0]*8))[1],
                                                     get_polygon_center(x.get('polygon', [0]*8))[0]))

        current_asset = None

        for line in sorted_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers and footers using detected boundaries
            if cy < layout['header_y_min'] or cy > layout['footer_y_max']:
                continue

            # Look for row number using detected column boundary
            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,2}$', content):
                if current_asset and current_asset.get('valuation'):
                    assets.append(current_asset)

                row_num = int(content)
                current_asset = {
                    'index': row_num,
                    'y_pos': cy,
                    'asset_type_id': 10,  # Default to บ้าน
                    'asset_type_other': '',
                    'asset_name': '',
                    'date_acquiring_type_id': 1,
                    'acquiring_date': None,
                    'acquiring_month': None,
                    'acquiring_year': None,
                    'date_ending_type_id': 4,
                    'ending_date': None,
                    'ending_month': None,
                    'ending_year': None,
                    'asset_acquisition_type_id': 6,
                    'valuation': None,
                    'owner_by_submitter': True,
                    'owner_by_spouse': False,
                    'owner_by_child': False
                }

            if not current_asset:
                continue

            if abs(cy - current_asset['y_pos']) > 1.5:
                continue

            # Look for building type using detected type column range
            type_x_min, type_x_max = layout['type_col_x_range']
            if type_x_min <= cx <= type_x_max:
                # Skip if content is just a number
                if re.match(r'^\d+$', content):
                    continue
                type_id, type_other = detect_building_type(content)
                if type_id:
                    current_asset['asset_type_id'] = type_id
                    current_asset['asset_type_other'] = type_other
                    if not current_asset['asset_name']:
                        current_asset['asset_name'] = content[:100]

            # Look for date using detected date column range
            date_x_min, date_x_max = layout['date_col_x_range']
            if date_x_min <= cx <= date_x_max:
                day, month, year = parse_thai_date(content)
                if year:
                    current_asset['acquiring_year'] = year
                    if month:
                        current_asset['acquiring_month'] = month
                    if day:
                        current_asset['acquiring_date'] = day
                    current_asset['date_acquiring_type_id'] = 1

            # Look for valuation using detected value column range
            value_x_min, value_x_max = layout['value_col_x_range']
            if value_x_min <= cx <= value_x_max:
                val = clean_number(content)
                if val and val >= 1000:
                    current_asset['valuation'] = val

            # Check ownership using detected owner column range
            owner_x_min, owner_x_max = layout['owner_col_x_range']
            if owner_x_min <= cx <= owner_x_max:
                if content in ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔']:
                    owner_width = owner_x_max - owner_x_min
                    submitter_max = owner_x_min + owner_width * 0.33
                    spouse_max = owner_x_min + owner_width * 0.67

                    if cx <= submitter_max:
                        current_asset['owner_by_submitter'] = True
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = False
                    elif cx <= spouse_max:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = True
                        current_asset['owner_by_child'] = False
                    else:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = True

        if current_asset and current_asset.get('valuation'):
            assets.append(current_asset)

    return assets


def extract_vehicle_assets(pages: List[Tuple[int, Dict]], all_pages: List[Dict]) -> List[Dict]:
    """Extract vehicle assets from vehicle pages using adaptive layout detection"""
    assets = []
    detector = get_detector()

    for page_idx, page in pages:
        lines = page.get('lines', [])

        # Auto-detect layout for this page
        layout = detector.detect_page_layout(lines, 'vehicle')

        sorted_lines = sorted(lines, key=lambda x: (get_polygon_center(x.get('polygon', [0]*8))[1],
                                                     get_polygon_center(x.get('polygon', [0]*8))[0]))

        current_asset = None

        for line in sorted_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers and footers using detected boundaries
            if cy < layout['header_y_min'] or cy > layout['footer_y_max']:
                continue

            # Look for row number using detected column boundary
            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,2}$', content):
                if current_asset and current_asset.get('valuation'):
                    assets.append(current_asset)

                row_num = int(content)
                current_asset = {
                    'index': row_num,
                    'y_pos': cy,
                    'asset_type_id': 18,  # Default to รถยนต์
                    'asset_type_other': '',
                    'asset_name': 'รถยนต์',
                    'date_acquiring_type_id': 1,
                    'acquiring_date': None,
                    'acquiring_month': None,
                    'acquiring_year': None,
                    'date_ending_type_id': 4,
                    'ending_date': None,
                    'ending_month': None,
                    'ending_year': None,
                    'asset_acquisition_type_id': 6,
                    'valuation': None,
                    'owner_by_submitter': True,
                    'owner_by_spouse': False,
                    'owner_by_child': False
                }

            if not current_asset:
                continue

            if abs(cy - current_asset['y_pos']) > 1.5:
                continue

            # Look for vehicle type using detected type column range
            type_x_min, type_x_max = layout['type_col_x_range']
            if type_x_min <= cx <= type_x_max:
                # Skip if content is just a number
                if re.match(r'^\d+$', content):
                    continue
                type_id, type_other = detect_vehicle_type(content)
                if type_id:
                    current_asset['asset_type_id'] = type_id
                    current_asset['asset_type_other'] = type_other
                    current_asset['asset_name'] = content[:100]

            # Look for date using detected date column range
            date_x_min, date_x_max = layout['date_col_x_range']
            if date_x_min <= cx <= date_x_max:
                day, month, year = parse_thai_date(content)
                if year:
                    current_asset['acquiring_year'] = year
                    if month:
                        current_asset['acquiring_month'] = month
                    if day:
                        current_asset['acquiring_date'] = day
                    current_asset['date_acquiring_type_id'] = 1

            # Look for valuation using detected value column range
            value_x_min, value_x_max = layout['value_col_x_range']
            if value_x_min <= cx <= value_x_max:
                val = clean_number(content)
                if val and val >= 100:
                    current_asset['valuation'] = val

            # Check ownership using detected owner column range
            owner_x_min, owner_x_max = layout['owner_col_x_range']
            if owner_x_min <= cx <= owner_x_max:
                if content in ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔']:
                    owner_width = owner_x_max - owner_x_min
                    submitter_max = owner_x_min + owner_width * 0.33
                    spouse_max = owner_x_min + owner_width * 0.67

                    if cx <= submitter_max:
                        current_asset['owner_by_submitter'] = True
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = False
                    elif cx <= spouse_max:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = True
                        current_asset['owner_by_child'] = False
                    else:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = True

        if current_asset and current_asset.get('valuation'):
            assets.append(current_asset)

    return assets


def extract_rights_assets(pages: List[Tuple[int, Dict]], all_pages: List[Dict]) -> List[Dict]:
    """Extract rights/concession assets from rights pages using adaptive layout detection"""
    assets = []
    detector = get_detector()

    for page_idx, page in pages:
        lines = page.get('lines', [])

        # Auto-detect layout for this page
        layout = detector.detect_page_layout(lines, 'rights')

        sorted_lines = sorted(lines, key=lambda x: (get_polygon_center(x.get('polygon', [0]*8))[1],
                                                     get_polygon_center(x.get('polygon', [0]*8))[0]))

        current_asset = None

        for line in sorted_lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers and footers using detected boundaries
            if cy < layout['header_y_min'] or cy > layout['footer_y_max']:
                continue

            # Look for row number using detected column boundary
            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,2}$', content):
                if current_asset and current_asset.get('valuation'):
                    assets.append(current_asset)

                row_num = int(content)
                current_asset = {
                    'index': row_num,
                    'y_pos': cy,
                    'asset_type_id': 22,  # Default to กรมธรรม์
                    'asset_type_other': '',
                    'asset_name': '',
                    'date_acquiring_type_id': 1,
                    'acquiring_date': None,
                    'acquiring_month': None,
                    'acquiring_year': None,
                    'date_ending_type_id': 1,  # Rights usually have ending date
                    'ending_date': None,
                    'ending_month': None,
                    'ending_year': None,
                    'asset_acquisition_type_id': 6,
                    'valuation': None,
                    'owner_by_submitter': True,
                    'owner_by_spouse': False,
                    'owner_by_child': False
                }

            if not current_asset:
                continue

            if abs(cy - current_asset['y_pos']) > 1.2:
                continue

            # Look for rights type and name using detected type column range
            type_x_min, type_x_max = layout['type_col_x_range']
            if type_x_min <= cx <= type_x_max:
                # Skip if content is just a number
                if re.match(r'^\d+$', content):
                    continue
                # Determine type using improved pattern matching
                type_id, type_other = detect_rights_type(content)
                if type_id:
                    current_asset['asset_type_id'] = type_id
                    current_asset['asset_type_other'] = type_other

                # Build asset name
                if not current_asset['asset_name']:
                    current_asset['asset_name'] = clean_text(content)
                else:
                    current_asset['asset_name'] += ' ' + clean_text(content)

            # Look for acquiring date using detected date column range
            date_x_min, date_x_max = layout['date_col_x_range']
            if date_x_min <= cx <= date_x_max:
                day, month, year = parse_thai_date(content)
                if year:
                    current_asset['acquiring_year'] = year
                    if month:
                        current_asset['acquiring_month'] = month
                    if day:
                        current_asset['acquiring_date'] = day
                    current_asset['date_acquiring_type_id'] = 1

            # Look for ending date (between date and value columns)
            # For rights, ending date is typically after acquiring date
            if date_x_max < cx < layout['value_col_x_range'][0]:
                day, month, year = parse_thai_date(content)
                if year:
                    current_asset['ending_year'] = year
                    if month:
                        current_asset['ending_month'] = month
                    if day:
                        current_asset['ending_date'] = day
                    current_asset['date_ending_type_id'] = 1

            # Look for valuation using detected value column range
            value_x_min, value_x_max = layout['value_col_x_range']
            if value_x_min <= cx <= value_x_max:
                val = clean_number(content)
                if val is not None:
                    current_asset['valuation'] = val

            # Check ownership using detected owner column range
            owner_x_min, owner_x_max = layout['owner_col_x_range']
            if owner_x_min <= cx <= owner_x_max:
                if content in ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔']:
                    owner_width = owner_x_max - owner_x_min
                    submitter_max = owner_x_min + owner_width * 0.33
                    spouse_max = owner_x_min + owner_width * 0.67

                    if cx <= submitter_max:
                        current_asset['owner_by_submitter'] = True
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = False
                    elif cx <= spouse_max:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = True
                        current_asset['owner_by_child'] = False
                    else:
                        current_asset['owner_by_submitter'] = False
                        current_asset['owner_by_spouse'] = False
                        current_asset['owner_by_child'] = True

        if current_asset and current_asset.get('valuation') is not None:
            # Clean up asset name
            current_asset['asset_name'] = current_asset['asset_name'][:200].strip()
            assets.append(current_asset)

    return assets


def extract_other_assets(pages: List[Tuple[int, Dict]], all_pages: List[Dict]) -> List[Dict]:
    """Extract other assets from other asset pages using adaptive layout detection"""
    assets = []
    detector = get_detector()

    for page_idx, page in pages:
        lines = page.get('lines', [])

        # Auto-detect layout for this page
        layout = detector.detect_page_layout(lines, 'other')

        # First pass: identify all row numbers with their y positions
        row_info = []
        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,3}$', content):
                row_info.append({'index': int(content), 'y': cy})
        row_info = sorted(row_info, key=lambda x: x['y'])

        # Assign each content line to its closest row
        # This ensures content just before a row number still belongs to that row
        row_contents = {r['index']: {'y': r['y'], 'contents': []} for r in row_info}

        for line in lines:
            content = line.get('content', '').strip()
            polygon = line.get('polygon', [0]*8)
            cx, cy = get_polygon_center(polygon)

            # Skip headers and footers
            if cy < layout['header_y_min'] or cy > layout['footer_y_max']:
                continue

            # Skip row numbers themselves
            if cx < layout['row_num_x_max'] and re.match(r'^\d{1,3}$', content):
                continue

            # Find the closest row for this content
            min_dist = float('inf')
            closest_row = None
            for r in row_info:
                dist = abs(cy - r['y'])
                if dist < min_dist and dist < 0.5:  # Must be within 0.5 tolerance
                    min_dist = dist
                    closest_row = r['index']

            if closest_row is not None:
                row_contents[closest_row]['contents'].append({
                    'content': content,
                    'cx': cx,
                    'cy': cy
                })

        # Now process each row's contents
        for row_idx, info in row_contents.items():
            current_asset = {
                'index': row_idx,
                'y_pos': info['y'],
                'asset_type_id': 35,
                'asset_type_other': '',
                'asset_name': '',
                'date_acquiring_type_id': 2,
                'acquiring_date': None,
                'acquiring_month': None,
                'acquiring_year': None,
                'date_ending_type_id': 4,
                'ending_date': None,
                'ending_month': None,
                'ending_year': None,
                'asset_acquisition_type_id': 6,
                'valuation': None,
                'owner_by_submitter': True,
                'owner_by_spouse': False,
                'owner_by_child': False
            }

            # Sort contents by y then x
            sorted_contents = sorted(info['contents'], key=lambda x: (x['cy'], x['cx']))

            for item in sorted_contents:
                content = item['content']
                cx = item['cx']
                cy = item['cy']

                # Look for item name using detected type column range
                type_x_min, type_x_max = layout['type_col_x_range']
                if type_x_min <= cx <= type_x_max:
                    # Skip headers
                    if content in ['รายการ', 'จำนวน', 'หน่วย', 'ที่', 'ลำดับ']:
                        continue
                    # Skip if content is just a number
                    if re.match(r'^\d+$', content):
                        continue

                    # Build asset name first
                    if not current_asset['asset_name']:
                        current_asset['asset_name'] = clean_text(content)
                    else:
                        current_asset['asset_name'] += ' ' + clean_text(content)

                    # Determine type from content using improved pattern matching
                    type_id, type_other = detect_other_type(content)
                    if type_id != 35:  # Only update if not default
                        current_asset['asset_type_id'] = type_id
                        current_asset['asset_type_other'] = type_other

                # Look for date/year using detected date column range
                date_x_min, date_x_max = layout['date_col_x_range']
                if date_x_min <= cx <= date_x_max:
                    if 'ไม่พบ' in content:
                        current_asset['date_acquiring_type_id'] = 2
                    else:
                        day, month, year = parse_thai_date(content)
                        if year:
                            current_asset['acquiring_year'] = year
                            if month:
                                current_asset['acquiring_month'] = month
                            if day:
                                current_asset['acquiring_date'] = day
                            current_asset['date_acquiring_type_id'] = 1

                # Look for valuation using detected value column range
                value_x_min, value_x_max = layout['value_col_x_range']
                if value_x_min <= cx <= value_x_max:
                    val = clean_number(content)
                    if val and val >= 100:
                        current_asset['valuation'] = val

                # Check ownership using detected owner column range
                owner_x_min, owner_x_max = layout['owner_col_x_range']
                if owner_x_min <= cx <= owner_x_max:
                    if content in ['/', '✓', 'V', 'v', '1', 'I', 'l', '|', '✔', '-']:
                        if content == '-':
                            continue
                        owner_width = owner_x_max - owner_x_min
                        submitter_max = owner_x_min + owner_width * 0.33
                        spouse_max = owner_x_min + owner_width * 0.67

                        if cx <= submitter_max:
                            current_asset['owner_by_submitter'] = True
                            current_asset['owner_by_spouse'] = False
                            current_asset['owner_by_child'] = False
                        elif cx <= spouse_max:
                            current_asset['owner_by_submitter'] = False
                            current_asset['owner_by_spouse'] = True
                            current_asset['owner_by_child'] = False
                        else:
                            current_asset['owner_by_submitter'] = False
                            current_asset['owner_by_spouse'] = False
                            current_asset['owner_by_child'] = True

            # After processing all contents for this row, save if valid
            if current_asset.get('valuation'):
                current_asset['asset_name'] = current_asset['asset_name'][:200].strip()
                # Final type determination based on full asset name
                if current_asset['asset_type_id'] == 35:
                    type_id, type_other = detect_other_type(current_asset['asset_name'])
                    current_asset['asset_type_id'] = type_id
                    current_asset['asset_type_other'] = type_other
                assets.append(current_asset)

    # Post-process: re-check all assets with type 35 based on full name
    for asset in assets:
        if asset['asset_type_id'] == 35 and asset['asset_name']:
            type_id, type_other = detect_other_type(asset['asset_name'])
            asset['asset_type_id'] = type_id
            asset['asset_type_other'] = type_other

    # Filter out invalid assets (empty names or just numbers)
    valid_assets = []
    for asset in assets:
        name = asset.get('asset_name', '').strip()
        # Skip if name is empty, just numbers, or just "1" from quantity column
        if not name or re.match(r'^[\d\s]+$', name) or len(name) < 3:
            continue
        valid_assets.append(asset)

    return valid_assets


def find_asset_pages_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> Dict[str, List[Tuple[int, Dict]]]:
    """
    Find pages for each asset type using page_metadata first, then fallback to content detection.

    Args:
        pages: List of page dicts from json_content
        loader: Optional PipelineDataLoader instance for page_metadata lookup
        doc_location_url: Document location URL for page_metadata lookup

    Returns:
        Dict mapping asset category to list of (page_idx, page_dict) tuples
    """
    result = {
        'land': [],
        'building': [],
        'vehicle': [],
        'rights': [],
        'other': []
    }

    # Step name to asset category mapping
    step_to_category = {
        'step_7': 'land',       # Land details
        'step_8': 'building',   # Building details
        'step_9': 'vehicle',    # Vehicle details
        'step_10': 'other',     # Other assets
    }

    # Try to use page_metadata if loader is available
    if loader and doc_location_url:
        for step_name, category in step_to_category.items():
            step_pages = loader.get_step_pages(doc_location_url, step_name)
            if step_pages:
                for page_num in step_pages:
                    # Convert 1-indexed page_num to 0-indexed page_idx
                    page_idx = page_num - 1
                    if 0 <= page_idx < len(pages):
                        page_data = pages[page_idx]
                        if (page_idx, page_data) not in result[category]:
                            result[category].append((page_idx, page_data))

    # Fallback: use the original find_asset_pages logic for any missing categories
    for category in result:
        if not result[category]:
            # Use content-based detection as fallback
            fallback_pages = find_asset_pages(pages)
            result[category] = fallback_pages.get(category, [])

    return result


def extract_asset_data(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict,
    start_asset_id: int,
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Dict]:
    """Extract all assets from JSON content"""

    assets = []

    nacc_id = nacc_detail.get('nacc_id')
    # Use submitter_id from nacc_detail (primary key for matching)
    submitter_id = nacc_detail.get('submitter_id', '')

    # Parse disclosure date
    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    pages = json_content.get('pages', [])
    if not pages:
        return assets

    # Find asset pages - use page_metadata if available, otherwise fallback to content detection
    asset_pages = find_asset_pages_with_metadata(pages, loader, doc_location_url)

    # Extract from each category
    land_assets = extract_land_assets(asset_pages['land'], pages)
    building_assets = extract_building_assets(asset_pages['building'], pages)
    vehicle_assets = extract_vehicle_assets(asset_pages['vehicle'], pages)
    rights_assets = extract_rights_assets(asset_pages['rights'], pages)
    other_assets = extract_other_assets(asset_pages['other'], pages)

    asset_id = start_asset_id

    # Combine all assets with proper indexing
    all_extracted = []

    # Group by type and reindex
    land_idx = 1
    for asset in land_assets:
        asset['index'] = land_idx
        land_idx += 1
        all_extracted.append(asset)

    building_idx = 1
    for asset in building_assets:
        asset['index'] = building_idx
        building_idx += 1
        all_extracted.append(asset)

    vehicle_idx = 1
    for asset in vehicle_assets:
        asset['index'] = vehicle_idx
        vehicle_idx += 1
        all_extracted.append(asset)

    rights_idx = 1
    for asset in rights_assets:
        asset['index'] = rights_idx
        rights_idx += 1
        all_extracted.append(asset)

    other_idx = 1
    for asset in other_assets:
        asset['index'] = other_idx
        other_idx += 1
        all_extracted.append(asset)

    for asset in all_extracted:
        assets.append({
            'asset_id': asset_id,
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'index': asset['index'],
            'asset_type_id': asset['asset_type_id'],
            'asset_type_other': asset.get('asset_type_other', ''),
            'asset_name': asset.get('asset_name', ''),
            'date_acquiring_type_id': asset.get('date_acquiring_type_id', 4),
            'acquiring_date': asset.get('acquiring_date'),
            'acquiring_month': asset.get('acquiring_month'),
            'acquiring_year': asset.get('acquiring_year'),
            'date_ending_type_id': asset.get('date_ending_type_id', 4),
            'ending_date': asset.get('ending_date'),
            'ending_month': asset.get('ending_month'),
            'ending_year': asset.get('ending_year'),
            'asset_acquisition_type_id': asset.get('asset_acquisition_type_id', 6),
            'valuation': asset.get('valuation'),
            'owner_by_submitter': asset.get('owner_by_submitter', True),
            'owner_by_spouse': asset.get('owner_by_spouse', False),
            'owner_by_child': asset.get('owner_by_child', False),
            'latest_submitted_date': latest_submitted_date
        })
        asset_id += 1

    return assets


def run_step_6(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 6 to extract asset data.
    
    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    # Use shared data loader if provided, otherwise create new one
    loader = data_loader or PipelineDataLoader(input_dir)

    all_assets = []
    asset_id = 2616  # Starting asset_id based on test data

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']
        doc_info = context['doc_info']
        doc_location_url = doc_info.get('doc_location_url', '')

        # Pass loader and doc_location_url for page_metadata integration
        assets = extract_asset_data(
            json_content, nacc_detail, submitter_info, asset_id,
            loader=loader, doc_location_url=doc_location_url
        )
        all_assets.extend(assets)
        asset_id += len(assets)

    # Write output using CSVWriter
    asset_fields = [
        'asset_id', 'submitter_id', 'nacc_id', 'index', 'asset_type_id', 'asset_type_other',
        'asset_name', 'date_acquiring_type_id', 'acquiring_date', 'acquiring_month',
        'acquiring_year', 'date_ending_type_id', 'ending_date', 'ending_month', 'ending_year',
        'asset_acquisition_type_id', 'valuation', 'owner_by_submitter', 'owner_by_spouse',
        'owner_by_child', 'latest_submitted_date'
    ]

    # Convert booleans to TRUE/FALSE strings
    processed_assets = []
    for asset in all_assets:
        row = asset.copy()
        row['owner_by_submitter'] = 'TRUE' if row['owner_by_submitter'] else 'FALSE'
        row['owner_by_spouse'] = 'TRUE' if row['owner_by_spouse'] else 'FALSE'
        row['owner_by_child'] = 'TRUE' if row['owner_by_child'] else 'FALSE'
        processed_assets.append(row)

    writer = CSVWriter(output_dir, 'asset.csv', asset_fields)
    count = writer.write_rows(processed_assets)

    print(f"Extracted {count} assets to {writer.output_path}")

    return all_assets


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_6(input_dir, output_dir)
