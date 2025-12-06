"""
Step 1: Extract submitter_position.csv from JSON extract files

This step extracts position information for submitters from OCR JSON files.
Uses page metadata to target specific pages and coordinate-based extraction
for parsing position data.

Position Period Types (from enum_type/position_period_type.csv):
- 1: ตำแหน่งปัจจุบัน ณ วันที่มีหน้าที่ยื่นบัญชีฯ (ทุกตำแหน่ง)
- 2: ตำแหน่งงานปัจจุบัน
- 3: ตำแหน่งงานย้อนหลัง 5 ปี

Position Category Types (from enum_type/position_category_type.csv):
- 4: สมาชิกสภาผู้แทนราษฎร

Date Acquiring Types (from enum_type/date_acquiring_type.csv):
- 1: มีข้อมูล
- 4: ไม่มีข้อมูลในเอกสาร

Date Ending Types (from enum_type/date_ending_type.csv):
- 1: มีข้อมูล
- 4: ไม่มีข้อมูลในเอกสาร

Page Metadata (from page_metadata/index.json):
- step_1 pages contain personal_info with work history
- Uses text_each_page for structured line data with coordinates

Column Layout (X coordinates in inches, page width ~8.26 inches):
- Date column: X ~0.6-2.0
- Position column: X ~2.0-4.5
- Workplace column: X ~4.5-7.5
"""

import os
import re
from typing import List, Dict, Any, Optional, Tuple

# Import shared utilities from utils package
import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import (
    clean_text,
    clean_ocr_text,
    parse_thai_date,
    parse_date_range,
    detect_position_category,
    format_disclosure_date,
    get_line_y,
    group_lines_by_row,
    is_date_range,
    is_header_line,
    is_skip_line,
)
from utils.data_loader import PipelineDataLoader, CSVWriter


# Column boundaries for coordinate-based extraction (in inches)
# Based on actual NACC form layout, varies slightly between pages:
# Page 3: Date ~1.0-2.5, Position ~2.5-4.5, Workplace ~4.5-7.0
# Page 4: Date ~0.6-2.2, Position ~2.2-4.2, Workplace ~4.2-6.6
# Use content-based detection as primary, coordinates as secondary
COL_DATE_MAX_PAGE3 = 2.5
COL_DATE_MAX_PAGE4 = 2.2
COL_POSITION_MAX = 4.3
COL_WORKPLACE_MAX = 7.0


def get_line_column(line: Dict, page_type: str = 'default') -> str:
    """Determine which column a line belongs to based on X coordinate and content"""
    polygon = line.get('polygon', [])
    content = line.get('content', '').strip()

    if len(polygon) < 2:
        return 'unknown'

    x1 = polygon[0]  # Left X coordinate

    # Content-based detection first (more reliable)
    # Date patterns
    if re.search(r'^\d{4}\s*[-–]', content) or re.search(r'^\d{1,2}\s*[ก-๙\.]+\s*\d{4}\s*[-–]', content):
        return 'date'

    # Adjust column boundary based on page type
    col_date_max = COL_DATE_MAX_PAGE4 if page_type == 'continuation' else COL_DATE_MAX_PAGE3

    if x1 < col_date_max:
        return 'date'
    elif x1 < COL_POSITION_MAX:
        return 'position'
    elif x1 < COL_WORKPLACE_MAX:
        return 'workplace'
    else:
        return 'notes'


def classify_line_by_content(content: str, x: float) -> str:
    """Classify line by content analysis, more reliable than just coordinates"""
    content = content.strip()

    # Date patterns - highest priority
    if is_date_range(content):
        return 'date'

    # Skip patterns
    if is_header_line(content) or is_skip_line(content):
        return 'skip'

    # Primary workplace indicators (definitely workplace)
    primary_workplace = ['บริษัท', 'พรรค', 'กระทรวง', 'กรม', 'สำนักงาน']
    # Secondary workplace indicators (could be position context)
    secondary_workplace = ['สมาคม', 'มูลนิธิ', 'คณะกรรมาธิการ', 'วุฒิสภา', 'สภาผู้แทนราษฎร']

    # Position indicators
    position_indicators = ['กรรมการ', 'ประธาน', 'รอง', 'ผู้จัดการ', 'นักวิชาการ',
                          'ที่ปรึกษา', 'ผู้อำนวยการ', 'สมาชิก', 'เลขาธิการ',
                          'ผู้ว่า', 'หัวหน้า', 'รัฐมนตรี', 'แบบบัญชีรายชื่อ']

    has_primary_workplace = any(ind in content for ind in primary_workplace)
    has_secondary_workplace = any(ind in content for ind in secondary_workplace)
    has_position = any(ind in content for ind in position_indicators)

    # Primary workplace is definitely workplace
    if has_primary_workplace:
        # Unless position indicator comes first
        if has_position:
            pos_idx = min((content.find(ind) for ind in position_indicators if ind in content), default=999)
            wp_idx = min((content.find(ind) for ind in primary_workplace if ind in content), default=999)
            if pos_idx < wp_idx:
                return 'position'
        return 'workplace'

    # Position takes priority over secondary workplace
    if has_position:
        return 'position'

    # Secondary workplace only if no position indicators and X is in workplace column
    if has_secondary_workplace and x > 4.0:
        return 'workplace'

    # Use X coordinate as fallback
    # Column 1 (Date): X < 2.2
    # Column 2 (Position): X 2.2 - 4.2
    # Column 3 (Workplace): X > 4.2
    if x < 2.2:
        return 'date'
    elif x < 4.2:
        return 'position'
    else:
        return 'workplace'


def extract_positions_from_page(page: Dict, is_continuation: bool = False) -> List[Dict]:
    """Extract position data from a single page using coordinates and content analysis"""
    lines = page.get('lines', [])
    if not lines:
        return []

    positions = []

    # Find work history section - look for "ประวัติการทำงานย้อนหลัง"
    history_start_y = None
    history_end_y = None

    for line in lines:
        content = line.get('content', '')
        y = get_line_y(line)

        if 'ประวัติการทำงานย้อนหลัง' in content:
            history_start_y = y
        elif history_start_y and ('ลงชื่อ' in content or 'ผู้ยื่นบัญชี' in content):
            if y > history_start_y:
                history_end_y = y
                break

    if history_start_y is None:
        return []

    if history_end_y is None:
        history_end_y = 11.0  # Default to bottom of page

    # Filter and classify lines within work history section
    history_lines = []
    for line in lines:
        content = line.get('content', '').strip()
        y = get_line_y(line)
        x = line.get('polygon', [0])[0] if line.get('polygon') else 0

        if y < history_start_y or y > history_end_y:
            continue
        if len(content) <= 2:
            continue

        line_type = classify_line_by_content(content, x)
        if line_type == 'skip':
            continue

        history_lines.append({
            'content': content,
            'x': x,
            'y': y,
            'type': line_type
        })

    # Sort by Y coordinate first, then X
    history_lines.sort(key=lambda l: (l['y'], l['x']))

    # Group lines by row (similar Y coordinate)
    # Use the FIRST line's Y as anchor, not average, to prevent drift
    rows = []
    current_row = []
    anchor_y = None

    for line in history_lines:
        if anchor_y is None or abs(line['y'] - anchor_y) <= 0.20:
            current_row.append(line)
            if anchor_y is None:
                anchor_y = line['y']
        else:
            if current_row:
                rows.append(current_row)
            current_row = [line]
            anchor_y = line['y']

    if current_row:
        rows.append(current_row)

    # Process rows to build position entries
    current_entry = None

    for row in rows:
        # Sort row by X coordinate
        row.sort(key=lambda l: l['x'])

        row_date = None
        row_positions = []
        row_workplaces = []

        for line in row:
            if line['type'] == 'date':
                row_date = line['content']
            elif line['type'] == 'position':
                row_positions.append(line['content'])
            elif line['type'] == 'workplace':
                row_workplaces.append(line['content'])

        # If we found a new date, save previous entry and start new one
        if row_date:
            if current_entry and (current_entry['positions'] or current_entry['workplaces']):
                positions.append({
                    'date_range': current_entry['date'],
                    'position': ' '.join(current_entry['positions']),
                    'workplace': ' '.join(current_entry['workplaces'])
                })
            current_entry = {
                'date': row_date,
                'positions': row_positions,
                'workplaces': row_workplaces
            }
        elif current_entry:
            # Continue adding to current entry
            current_entry['positions'].extend(row_positions)
            current_entry['workplaces'].extend(row_workplaces)

    # Don't forget the last entry
    if current_entry and (current_entry['positions'] or current_entry['workplaces']):
        positions.append({
            'date_range': current_entry['date'],
            'position': ' '.join(current_entry['positions']),
            'workplace': ' '.join(current_entry['workplaces'])
        })

    return positions


def extract_positions_coordinate_based(json_content: Dict) -> Tuple[List[Dict], List[Dict]]:
    """
    Extract current and past positions using coordinate-based parsing.
    Returns (current_positions, past_positions)
    """
    pages = json_content.get('pages', [])
    if not pages:
        return [], []

    current_positions = []
    past_positions = []

    # Priority pages for work history: page 3-4 first, then page 20 for continuation
    priority_pages = [3, 4]
    continuation_pages = [20, 21, 5]

    # Find pages with work history
    pages_dict = {p.get('page_number'): p for p in pages}

    # Check priority pages first
    for page_num in priority_pages:
        if page_num in pages_dict:
            page = pages_dict[page_num]
            lines = page.get('lines', [])

            # Check if this page has work history
            has_history = any('ประวัติการทำงานย้อนหลัง' in l.get('content', '') for l in lines)
            if has_history:
                positions = extract_positions_from_page(page, is_continuation=False)
                past_positions.extend(positions)

    # Check continuation pages
    for page_num in continuation_pages:
        if page_num in pages_dict:
            page = pages_dict[page_num]
            lines = page.get('lines', [])

            # Check if this is a continuation page "(ต่อ)"
            has_continuation = any('(ต่อ)' in l.get('content', '') for l in lines)
            if has_continuation:
                positions = extract_positions_from_page(page, is_continuation=True)
                past_positions.extend(positions)

    # Also check page 3 for current positions
    if 3 in pages_dict:
        page = pages_dict[3]
        lines = page.get('lines', [])

        # Look for current position section
        current_section_y = None
        history_section_y = None

        for line in lines:
            content = line.get('content', '')
            y = get_line_y(line)

            if 'ตำแหน่งปัจจุบันในหน่วยงาน' in content:
                current_section_y = y
            elif 'ประวัติการทำงานย้อนหลัง' in content and not '(ต่อ)' in content:
                history_section_y = y
                break

        if current_section_y and history_section_y:
            # Extract current positions between these sections
            current_lines = []
            for line in lines:
                y = get_line_y(line)
                if current_section_y < y < history_section_y:
                    content = line.get('content', '')
                    if not is_header_line(content) and not is_skip_line(content):
                        current_lines.append(line)

            # Group and parse current positions
            rows = group_lines_by_row(current_lines)
            for row in rows:
                row_sorted = sorted(row, key=lambda l: l.get('polygon', [0])[0])
                position_text = None
                workplace_text = None
                date_text = None

                for line in row_sorted:
                    content = line.get('content', '').strip()
                    col = get_line_column(line)

                    if col == 'position':
                        position_text = content
                    elif col == 'workplace':
                        workplace_text = content
                    elif col == 'date' or col == 'notes':
                        if re.search(r'\d{1,2}\s*[ก-๙\.]+\s*\d{4}', content):
                            date_text = content

                if position_text:
                    current_positions.append({
                        'position': position_text,
                        'workplace': workplace_text or '',
                        'date': date_text or ''
                    })

    return current_positions, past_positions


def extract_past_positions_regex(content: str) -> List[Dict]:
    """Extract past positions using regex"""
    # Find all past positions sections (including continuations with ต่อ)
    past_section_pattern = r'ประวัติการทำงานย้อนหลัง\s*5?\s*ปี(.*?)(?=ข้อมูลรายได้|$)'
    matches = re.findall(past_section_pattern, content, re.DOTALL)

    if not matches:
        return []

    # Combine all sections
    section_text = '\n'.join(matches)
    lines = section_text.split('\n')

    # Skip patterns - more precise (avoid matching position keywords)
    skip_patterns = [
        '- ลับ -', 'ลงชื่อ', 'ผู้ยื่นบัญชี', 'ระยะเวลาดำรงตำแหน่ง',
        'หน่วยงาน / ที่ตั้ง', 'หมายเหตุ', 'ประวัติการทำงาน',
        'หน้า ', '1.00.00', 'เลขประจำตัวประชาชน', 'วันเดือนปี เกิด',
        'ชื่อและชื่อสกุล', 'ชื่อเดิม', 'สถานภาพการสมรส',
        'คู่สมรส', 'พี่น้อง', 'ที่อยู่ที่ติดต่อ', 'โทรศัพท์',
        'ตำแหน่งปัจจุบัน', 'บิดา :', 'มารดา :'
    ]

    position_keywords = [
        'กรรมการ', 'สมาชิก', 'ประธาน', 'รอง', 'ผู้จัดการ',
        'นักวิชาการ', 'ที่ปรึกษา', 'ผู้อำนวยการ'
    ]

    workplace_keywords = ['บริษัท', 'พรรค', 'สมาคม', 'สำนักงาน', 'คณะกรรมาธิการ', 'มูลนิธิ']

    clean_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(p in line for p in skip_patterns):
            continue
        if len(line) <= 2 or re.match(r'^\d+$', line):
            continue
        # Skip if line is just "ตำแหน่ง" header
        if line == 'ตำแหน่ง':
            continue
        clean_lines.append(line)

    past_positions = []
    # Date range patterns - handle various formats including "2555- 5 ก.พ.2562"
    date_range_pattern = r'(\d{1,2}\s*[ก-๙\.]+\.?\s*\d{4}|\d{4})\s*[-–]\s*(\d{1,2}\s*[ก-๙\.]+\.?\s*\d{4}|\d{4})'

    i = 0
    while i < len(clean_lines):
        line = clean_lines[i]

        if re.search(date_range_pattern, line):
            date_range = line
            position = None
            workplace = None
            additional_positions = []  # For cases with multiple positions under same date

            j = i + 1
            # Look ahead for position and workplace
            while j < len(clean_lines) and j < i + 8:
                next_line = clean_lines[j]

                # If we hit another date range, stop looking
                if re.search(date_range_pattern, next_line):
                    break

                has_pos_kw = any(kw in next_line for kw in position_keywords)
                has_wp_kw = any(kw in next_line for kw in workplace_keywords)

                # Find position (must have position keyword)
                if position is None:
                    # Position with workplace keyword embedded (like "รองประธานกรรมการ มูลนิธิสปริงนิวส์อาสา")
                    if has_pos_kw and has_wp_kw:
                        # Check if position keyword comes first
                        pos_match = re.search(r'(กรรมการ|สมาชิก|ประธาน|รอง|ผู้จัดการ|นักวิชาการ|ที่ปรึกษา|ผู้อำนวยการ)', next_line)
                        wp_match = re.search(r'(บริษัท|พรรค|สมาคม|สำนักงาน|คณะกรรมาธิการ|มูลนิธิ)', next_line)
                        if pos_match and wp_match and pos_match.start() < wp_match.start():
                            position = next_line
                            j += 1
                            continue
                    elif has_pos_kw and not has_wp_kw:
                        position = next_line
                        j += 1
                        continue
                else:
                    # Already have a position, check for additional position or workplace
                    # Check if this is a position (position keyword comes first)
                    is_position_line = False
                    if has_pos_kw:
                        if not has_wp_kw:
                            is_position_line = True
                        else:
                            # Both keywords present - check which comes first
                            pos_match = re.search(r'(กรรมการ|สมาชิก|ประธาน|รอง|ผู้จัดการ|นักวิชาการ|ที่ปรึกษา|ผู้อำนวยการ)', next_line)
                            wp_match = re.search(r'(บริษัท|พรรค|สมาคม|สำนักงาน|คณะกรรมาธิการ|มูลนิธิ)', next_line)
                            if pos_match and wp_match and pos_match.start() < wp_match.start():
                                is_position_line = True

                    if is_position_line:
                        # This is an additional position under the same date range
                        additional_positions.append({
                            'date_range': date_range,
                            'position': next_line,
                            'workplace': ''  # Will fill in next
                        })
                        j += 1
                        continue

                # Find workplace after position is found
                if position and has_wp_kw:
                    # Check if this workplace should go to an additional position
                    if additional_positions:
                        # Fill workplace for the last additional position
                        additional_positions[-1]['workplace'] = next_line
                    else:
                        workplace = next_line
                    j += 1
                    continue

                # If we already have position but this line doesn't match patterns
                if position and not has_pos_kw and not has_wp_kw:
                    break

                j += 1

            if position:
                past_positions.append({
                    'date_range': date_range,
                    'position': position,
                    'workplace': workplace or ''
                })
                # Add any additional positions found
                past_positions.extend(additional_positions)

            i = j
        else:
            i += 1

    return past_positions


def extract_submitter_positions(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict
) -> List[Dict]:
    """Extract submitter positions from JSON content using coordinate-based parsing"""
    positions = []
    content = json_content.get('content', '')
    has_pages = 'pages' in json_content and json_content['pages']

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    # Parse disclosure date for latest_submitted_date
    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    # 1. Extract main position at submission (position_period_type_id=1)
    main_position = nacc_detail.get('position', '')
    agency = nacc_detail.get('agency', '')
    date_by_submitted_case = nacc_detail.get('date_by_submitted_case', '')

    if main_position:
        start_day, start_month, start_year = None, None, None
        if date_by_submitted_case:
            parts = date_by_submitted_case.split('/')
            if len(parts) == 3:
                start_day = int(parts[0])
                start_month = int(parts[1])
                start_year = int(parts[2])

        # Clean position name - remove parenthetical content and OCR artifacts
        clean_position = re.sub(r'\s*\([^)]+\)\s*', '', main_position).strip()
        clean_position = clean_ocr_text(clean_position)

        positions.append({
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'position_period_type_id': 1,
            'index': 0,
            'position': clean_position,
            'position_category_type_id': detect_position_category(main_position),
            'workplace': clean_ocr_text(agency),
            'workplace_location': clean_ocr_text(agency),
            'date_acquiring_type_id': 1,
            'start_date': start_day,
            'start_month': start_month,
            'start_year': start_year,
            'date_ending_type_id': 4,
            'end_date': '',
            'end_month': '',
            'end_year': '',
            'note': 'ตำแหน่งที่ยื่นหน้าที่ยื่นบัญชีฯ',
            'latest_submitted_date': latest_submitted_date
        })

    # Try coordinate-based extraction first if pages data is available
    if has_pages:
        current_positions_coord, past_positions_coord = extract_positions_coordinate_based(json_content)

        # 2. Add current positions (position_period_type_id=2) from coordinate-based extraction
        for idx, pos in enumerate(current_positions_coord):
            date_str = pos.get('date', '')
            start_day, start_month, start_year = parse_thai_date(date_str)

            positions.append({
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'position_period_type_id': 2,
                'index': idx + 1,
                'position': clean_ocr_text(pos.get('position', '')),
                'position_category_type_id': '',
                'workplace': clean_ocr_text(pos.get('workplace', '')),
                'workplace_location': '',
                'date_acquiring_type_id': 1 if (start_day or start_month or start_year) else 4,
                'start_date': start_day,
                'start_month': start_month,
                'start_year': start_year,
                'date_ending_type_id': 4,
                'end_date': '',
                'end_month': '',
                'end_year': '',
                'note': '',
                'latest_submitted_date': latest_submitted_date
            })

        # 3. Add past positions (position_period_type_id=3) from coordinate-based extraction
        for idx, pos in enumerate(past_positions_coord):
            date_range = pos.get('date_range', '')
            start, end = parse_date_range(date_range)
            start_day, start_month, start_year = start
            end_day, end_month, end_year = end

            date_ending_type = 1 if (end_day or end_month or end_year) else 4
            date_acquiring_type = 1 if (start_day or start_month or start_year) else 4

            # Clean position and workplace text
            position_text = clean_ocr_text(pos.get('position', ''))
            workplace_text = clean_ocr_text(pos.get('workplace', ''))

            positions.append({
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'position_period_type_id': 3,
                'index': idx + 1,
                'position': position_text,
                'position_category_type_id': '',
                'workplace': workplace_text,
                'workplace_location': '',
                'date_acquiring_type_id': date_acquiring_type,
                'start_date': start_day,
                'start_month': start_month,
                'start_year': start_year,
                'date_ending_type_id': date_ending_type,
                'end_date': end_day,
                'end_month': end_month,
                'end_year': end_year,
                'note': '',
                'latest_submitted_date': latest_submitted_date
            })

    else:
        # Fallback to regex-based extraction if no pages data
        # 2. Extract current positions (position_period_type_id=2)
        current_section_pattern = r'ตำแหน่งปัจจุบันในหน่วยงาน[^\n]*\n(.*?)ประวัติการทำงานย้อนหลัง'
        current_match = re.search(current_section_pattern, content, re.DOTALL)

        if current_match:
            section_text = current_match.group(1)
            # Simple regex extraction for current positions
            pos_pattern = r'(ที่ปรึกษา|กรรมการ[^\n]*|ประธาน[^\n]*)'
            workplace_pattern = r'(บริษัท[^\n]+)'
            date_pattern = r'(\d{1,2}\s*[ก-๙\.]+\s*\d{4})'

            pos_matches = re.findall(pos_pattern, section_text)
            workplace_matches = re.findall(workplace_pattern, section_text)
            date_matches = re.findall(date_pattern, section_text)

            for idx, pos in enumerate(pos_matches):
                workplace = workplace_matches[idx] if idx < len(workplace_matches) else ''
                date_str = date_matches[idx] if idx < len(date_matches) else ''
                start_day, start_month, start_year = parse_thai_date(date_str)

                positions.append({
                    'submitter_id': submitter_id,
                    'nacc_id': nacc_id,
                    'position_period_type_id': 2,
                    'index': idx + 1,
                    'position': clean_ocr_text(pos),
                    'position_category_type_id': '',
                    'workplace': clean_ocr_text(workplace),
                    'workplace_location': '',
                    'date_acquiring_type_id': 1,
                    'start_date': start_day,
                    'start_month': start_month,
                    'start_year': start_year,
                    'date_ending_type_id': 4,
                    'end_date': '',
                    'end_month': '',
                    'end_year': '',
                    'note': '',
                    'latest_submitted_date': latest_submitted_date
                })

        # 3. Extract past positions (position_period_type_id=3)
        past_positions = extract_past_positions_regex(content)

        # Add past positions
        for idx, pos in enumerate(past_positions):
            date_range = pos.get('date_range', '')
            start, end = parse_date_range(date_range)
            start_day, start_month, start_year = start
            end_day, end_month, end_year = end

            date_ending_type = 1 if (end_day or end_month or end_year) else 4

            # Clean position and workplace text
            position_text = clean_ocr_text(pos.get('position', ''))
            workplace_text = clean_ocr_text(pos.get('workplace', ''))

            positions.append({
                'submitter_id': submitter_id,
                'nacc_id': nacc_id,
                'position_period_type_id': 3,
                'index': idx + 1,
                'position': position_text,
                'position_category_type_id': '',
                'workplace': workplace_text,
                'workplace_location': '',
                'date_acquiring_type_id': 1,
                'start_date': start_day,
                'start_month': start_month,
                'start_year': start_year,
                'date_ending_type_id': date_ending_type,
                'end_date': end_day,
                'end_month': end_month,
                'end_year': end_year,
                'note': '',
                'latest_submitted_date': latest_submitted_date
            })

    return positions


def clean_position_text(text: str) -> str:
    """
    Comprehensive position text cleaner.
    Fixes OCR errors, removes addresses, and normalizes common patterns.
    """
    if not text:
        return ''

    # Remove leading noise characters (bullets, numbers, special chars)
    text = re.sub(r'^[\s•\*\-\d\.\,\:]+', '', text)

    # Remove trailing noise
    text = re.sub(r'[\s\.\,\:]+$', '', text)

    # Remove Thai numerals at end (page numbers etc)
    text = re.sub(r'\s*[๐-๙]+\s*$', '', text)

    # Remove address patterns (these shouldn't be in position field)
    # Pattern: เลขที่, หมู่, ซอย, ถนน, แขวง, ตำบล, อำเภอ, เขต, จังหวัด followed by data
    text = re.sub(r'\s*เลขที่\s*[\d\-\/]+.*$', '', text)
    text = re.sub(r'\s*หมู่\s*\d+.*$', '', text)
    text = re.sub(r'\s*หมู่ที่\s*\d+.*$', '', text)
    text = re.sub(r'\s*ซอย\s*\S+.*$', '', text)
    text = re.sub(r'\s*ถนน\s*\S+.*$', '', text)
    text = re.sub(r'\s*ถ\.\s*\S+.*$', '', text)

    # Remove standalone location patterns
    text = re.sub(r'\s+ต\.\s*\S+\s+อ\.\s*\S+\s+จ\.\s*\S+.*$', '', text)
    text = re.sub(r'\s+ตำบล\s*\S+\s+อำเภอ\s*\S+.*$', '', text)
    text = re.sub(r'\s+แขวง\s*\S+\s+เขต\s*\S+.*$', '', text)

    # Remove postal codes
    text = re.sub(r'\s+\d{5}\s*$', '', text)
    text = re.sub(r'\s+กรุงเทพฯ?\s*\d{5}.*$', '', text)
    text = re.sub(r'\s+กทม\.?\s*\d{5}.*$', '', text)

    # Remove "จังหวัด X เขต Y" patterns at end (keep for specific position titles)
    # But clean standalone addresses like "32 ต. บ้านกอก อ. จัดรัส จ. ชัยภูมิ"
    text = re.sub(r'\s+\d+\s+ต\.\s*\S+\s+อ\.\s*\S+\s+จ\.\s*\S+.*$', '', text)

    # OCR corrections dictionary - common misreadings
    ocr_corrections = {
        # สมาชิกสภาผู้แทนราษฎร variants
        r'สมาชิกสภาพิสทนราษฎร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาผู้แทนอาหาร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาย์แทนงามัตร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภา\s*วแทนราษฎร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาชแทนราษฎร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาฬาสภาผู้แทนราษฎร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาพแทนราษฎร์': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกกีฬานักเทนราษฎร์': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมรักษ์ภาษีแทบรอบุร์': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาพ์เทาทางการ': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาต์และการกร': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมาชิกสภาพิแทนกุมภา': 'สมาชิกสภาผู้แทนราษฎร',
        r'สมเช็ก\s*สภาพแทนราษฎร': 'สมาชิกสภาผู้แทนราษฎร',
        r'ผมรักการแทนฯ': 'สมาชิกสภาผู้แทนราษฎร',
        # สมาชิกวุฒิสภา variants
        r'วุฒิบีภา': 'สมาชิกวุฒิสภา',
        r'สำเนาถูกต้อง': '',  # Remove this garbage
    }

    for pattern, replacement in ocr_corrections.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    # Clean ลงซื้อ, ลงชื่อ garbage
    text = re.sub(r'\s*ลงซื้อ.*$', '', text)
    text = re.sub(r'\s*ลงชื่อ.*$', '', text)

    # Remove "ตําแหน่ง" header
    text = re.sub(r'^ตํา?แหน่ง\s*', '', text)

    # Clean address-like company text that got merged
    # If position starts with address-like pattern, it's probably garbage
    if re.match(r'^[\d๐-๙]+[\/\-]', text):
        return ''
    if re.match(r'^กรุงเทพ', text):
        return ''

    # Normalize ส.ส. / ส.ว. variants
    text = re.sub(r'สส\.?(?![ก-๙])', 'ส.ส.', text)
    text = re.sub(r'ส\.ส\.\.', 'ส.ส.', text)  # Fix double dots
    text = re.sub(r'สว\.?(?![ก-๙])', 'ส.ว.', text)

    # Clean up เขต formatting (add space before number)
    text = re.sub(r'เขต(\d)', r'เขต \1', text)

    # Replace hyphens with spaces in position names
    text = re.sub(r'(\S)-(\S)', r'\1 \2', text)

    # Clean extra spaces
    text = re.sub(r'\s+', ' ', text).strip()

    # Final validation - if result looks like garbage, return empty
    if len(text) < 3:
        return ''
    # If mostly numbers, probably address garbage
    if re.match(r'^[\d\s\-\/\.]+$', text):
        return ''

    return text


def normalize_position_text(text: str) -> str:
    """Normalize position text to standard form."""
    return clean_position_text(text)


def extract_positions_from_page_text(page_data: Dict) -> Tuple[List[Dict], List[Dict]]:
    """
    Extract current and past positions from page text data.
    Uses structured line data with coordinates for better accuracy.

    Args:
        page_data: Page text data from text_each_page

    Returns:
        Tuple of (current_positions, past_positions)
    """
    lines = page_data.get('lines', [])

    current_positions = []
    past_positions = []

    # Find section markers
    current_section_y = None
    history_section_y = None
    history_end_y = None

    for line in lines:
        text = line.get('text', '')
        y = line.get('y', 0)

        if 'ตำแหน่งปัจจุบันในหน่วยงาน' in text:
            current_section_y = y
        elif 'ประวัติการทำงานย้อนหลัง' in text and '(ต่อ)' not in text:
            history_section_y = y
        elif 'ลงชื่อ' in text or 'ผู้ยื่นบัญชี' in text:
            if history_section_y and y > history_section_y:
                history_end_y = y
                break

    if history_end_y is None:
        history_end_y = 11.0

    # Group lines by Y coordinate (row)
    def group_by_row(line_list: List[Dict], y_tolerance: float = 0.15) -> List[List[Dict]]:
        if not line_list:
            return []
        sorted_lines = sorted(line_list, key=lambda l: (l.get('y', 0), l.get('x', 0)))
        rows = []
        current_row = [sorted_lines[0]]
        anchor_y = sorted_lines[0].get('y', 0)

        for line in sorted_lines[1:]:
            if abs(line.get('y', 0) - anchor_y) <= y_tolerance:
                current_row.append(line)
            else:
                rows.append(current_row)
                current_row = [line]
                anchor_y = line.get('y', 0)
        if current_row:
            rows.append(current_row)
        return rows

    # Extract past positions from history section
    if history_section_y:
        history_lines = [
            l for l in lines
            if history_section_y < l.get('y', 0) < history_end_y
            and len(l.get('text', '').strip()) > 2
        ]

        rows = group_by_row(history_lines)
        current_entry = None

        for row in rows:
            row = sorted(row, key=lambda l: l.get('x', 0))
            row_date = None
            row_positions = []
            row_workplaces = []

            for line in row:
                text = line.get('text', '').strip()
                x = line.get('x', 0)

                # Skip headers and patterns
                if any(skip in text for skip in ['ระยะเวลา', 'หน่วยงาน', 'หมายเหตุ', '- ลับ -']):
                    continue

                # Date pattern check
                date_match = re.search(r'(\d{4})\s*[-–]\s*(\d{4}|\d{1,2}\s*[ก-๙\.]+\s*\d{4})', text)
                if date_match or re.match(r'^พ\.ศ\.\s*\d{4}', text) or re.match(r'^\d{4}\s*[-–]', text):
                    row_date = text
                elif x < 2.5:
                    # Could be date in first column
                    if re.search(r'\d{4}', text):
                        row_date = text
                elif x < 4.5:
                    # Position column
                    row_positions.append(text)
                else:
                    # Workplace column
                    row_workplaces.append(text)

            if row_date:
                if current_entry and (current_entry['positions'] or current_entry['workplaces']):
                    past_positions.append({
                        'date_range': current_entry['date'],
                        'position': ' '.join(current_entry['positions']),
                        'workplace': ' '.join(current_entry['workplaces'])
                    })
                current_entry = {
                    'date': row_date,
                    'positions': row_positions,
                    'workplaces': row_workplaces
                }
            elif current_entry:
                current_entry['positions'].extend(row_positions)
                current_entry['workplaces'].extend(row_workplaces)

        if current_entry and (current_entry['positions'] or current_entry['workplaces']):
            past_positions.append({
                'date_range': current_entry['date'],
                'position': ' '.join(current_entry['positions']),
                'workplace': ' '.join(current_entry['workplaces'])
            })

    # Extract current positions
    if current_section_y and history_section_y:
        current_lines = [
            l for l in lines
            if current_section_y < l.get('y', 0) < history_section_y
            and len(l.get('text', '').strip()) > 2
        ]

        rows = group_by_row(current_lines)
        for row in rows:
            row = sorted(row, key=lambda l: l.get('x', 0))
            position_text = None
            workplace_text = None
            date_text = None
            location_text = None

            for line in row:
                text = line.get('text', '').strip()
                x = line.get('x', 0)

                if any(skip in text for skip in ['ตำแหน่ง', 'หน่วยงาน', '- ลับ -']):
                    continue

                # Check for position indicators
                if any(ind in text for ind in ['กรรมการ', 'ประธาน', 'รอง', 'ที่ปรึกษา', 'ผู้จัดการ', 'สมาชิก', 'รัฐมนตรี', 'นักการเมือง', 'ผู้รับใบอนุญาต']):
                    position_text = text
                elif any(ind in text for ind in ['บริษัท', 'พรรค', 'สำนักงาน', 'กระทรวง', 'สโมสร', 'วิทยาลัย', 'มูลนิธิ']):
                    workplace_text = text
                elif re.search(r'ตำบล|แขวง|อำเภอ|เขต|จังหวัด', text):
                    location_text = text
                elif re.search(r'\d{1,2}\s*[ก-๙\.]+\s*\d{4}', text):
                    date_text = text

            if position_text:
                current_positions.append({
                    'position': position_text,
                    'workplace': workplace_text or '',
                    'workplace_location': location_text or '',
                    'date': date_text or ''
                })

    return current_positions, past_positions


def extract_submitter_positions_v2(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict,
    data_loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Dict]:
    """
    Improved extraction using page metadata and text_each_page.
    Falls back to original method if page data not available.
    """
    positions = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    # Parse disclosure date
    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    # 1. Extract main position (position_period_type_id=1) from nacc_detail
    main_position = nacc_detail.get('position', '')
    agency = nacc_detail.get('agency', '')
    date_by_submitted_case = nacc_detail.get('date_by_submitted_case', '')

    if main_position:
        start_day, start_month, start_year = None, None, None
        if date_by_submitted_case:
            parts = date_by_submitted_case.split('/')
            if len(parts) == 3:
                start_day = int(parts[0])
                start_month = int(parts[1])
                start_year = int(parts[2])

        clean_position = re.sub(r'\s*\([^)]+\)\s*', '', main_position).strip()
        clean_position = clean_ocr_text(clean_position)

        positions.append({
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'position_period_type_id': 1,
            'index': 0,
            'position': clean_position,
            'position_category_type_id': detect_position_category(main_position),
            'workplace': clean_ocr_text(agency),
            'workplace_location': clean_ocr_text(agency),
            'date_acquiring_type_id': 1,
            'start_date': start_day,
            'start_month': start_month,
            'start_year': start_year,
            'date_ending_type_id': 4,
            'end_date': '',
            'end_month': '',
            'end_year': '',
            'note': 'ตำแหน่งที่ยื่นหน้าที่ยื่นบัญชีฯ',
            'latest_submitted_date': latest_submitted_date
        })

    # Try page metadata approach first
    current_positions = []
    past_positions = []

    if data_loader and doc_location_url:
        step1_pages = data_loader.load_step_pages_text(doc_location_url, 'step_1')

        for page_data in step1_pages:
            curr, past = extract_positions_from_page_text(page_data)
            current_positions.extend(curr)
            past_positions.extend(past)

    # If page metadata didn't yield results, fall back to original method
    if not past_positions and json_content:
        content = json_content.get('content', '')
        has_pages = 'pages' in json_content and json_content['pages']

        if has_pages:
            current_positions, past_positions = extract_positions_coordinate_based(json_content)
        else:
            past_positions = extract_past_positions_regex(content)

    # 2. Add current positions (position_period_type_id=2)
    for idx, pos in enumerate(current_positions):
        date_str = pos.get('date', '')
        start_day, start_month, start_year = parse_thai_date(date_str)

        position_text = normalize_position_text(clean_ocr_text(pos.get('position', '')))
        workplace_text = clean_ocr_text(pos.get('workplace', ''))

        positions.append({
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'position_period_type_id': 2,
            'index': idx + 1,
            'position': position_text,
            'position_category_type_id': '',
            'workplace': workplace_text,
            'workplace_location': clean_ocr_text(pos.get('workplace_location', '')),
            'date_acquiring_type_id': 1 if (start_day or start_month or start_year) else 4,
            'start_date': start_day,
            'start_month': start_month,
            'start_year': start_year,
            'date_ending_type_id': 4,
            'end_date': '',
            'end_month': '',
            'end_year': '',
            'note': '',
            'latest_submitted_date': latest_submitted_date
        })

    # 3. Add past positions (position_period_type_id=3)
    for idx, pos in enumerate(past_positions):
        date_range = pos.get('date_range', '')
        start, end = parse_date_range(date_range)
        start_day, start_month, start_year = start
        end_day, end_month, end_year = end

        date_ending_type = 1 if (end_day or end_month or end_year) else 4
        date_acquiring_type = 1 if (start_day or start_month or start_year) else 4

        position_text = normalize_position_text(clean_ocr_text(pos.get('position', '')))
        workplace_text = clean_ocr_text(pos.get('workplace', ''))

        positions.append({
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'position_period_type_id': 3,
            'index': idx + 1,
            'position': position_text,
            'position_category_type_id': '',
            'workplace': workplace_text,
            'workplace_location': '',
            'date_acquiring_type_id': date_acquiring_type,
            'start_date': start_day,
            'start_month': start_month,
            'start_year': start_year,
            'date_ending_type_id': date_ending_type,
            'end_date': end_day,
            'end_month': end_month,
            'end_year': end_year,
            'note': '',
            'latest_submitted_date': latest_submitted_date
        })

    return positions


def run_step_1(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 1 to extract submitter positions.

    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    # Use shared data loader if provided, otherwise create new one
    loader = data_loader or PipelineDataLoader(input_dir)

    all_positions = []

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']
        doc_location_url = context['doc_info'].get('doc_location_url', '')

        # Use improved v2 extraction with page metadata
        positions = extract_submitter_positions_v2(
            json_content, nacc_detail, submitter_info,
            data_loader=loader,
            doc_location_url=doc_location_url
        )
        all_positions.extend(positions)

    # Write output using CSVWriter
    fieldnames = [
        'submitter_id', 'nacc_id', 'position_period_type_id', 'index', 'position',
        'position_category_type_id', 'workplace', 'workplace_location',
        'date_acquiring_type_id', 'start_date', 'start_month', 'start_year',
        'date_ending_type_id', 'end_date', 'end_month', 'end_year',
        'note', 'latest_submitted_date'
    ]

    writer = CSVWriter(output_dir, 'submitter_position.csv', fieldnames)
    count = writer.write_rows(all_positions)

    print(f"Extracted {count} positions to {writer.output_path}")
    return all_positions


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_1(input_dir, output_dir)
