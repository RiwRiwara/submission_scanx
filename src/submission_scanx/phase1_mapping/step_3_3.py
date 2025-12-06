"""
Step 3.3: Extract spouse position information from JSON extract files

This step extracts spouse_position.csv containing:
- Position title (ตำแหน่ง)
- Workplace (หน่วยงาน)
- Workplace location (ที่ตั้ง)

The position data is typically found around y=4.4-6.5 on the spouse page.
"""

import os
import re
from typing import List, Dict

import sys
from pathlib import Path
_src_dir = str(Path(__file__).parent.parent.parent)
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from utils.common import (
    clean_ocr_text,
    clean_position_text,
    get_polygon_center,
    format_disclosure_date,
)
from utils.data_loader import PipelineDataLoader, CSVWriter
from .step_3_common import find_spouse_page


# =============================================================================
# REGION CONSTANTS
# =============================================================================

# Position section region
POSITION_SECTION_Y_MIN = 4.4
POSITION_SECTION_Y_MAX = 6.8

# Position vs workplace X boundary
POSITION_X_MAX = 3.0
WORKPLACE_X_MIN = 3.0


# =============================================================================
# PAGE DETECTION
# =============================================================================

def find_spouse_page_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> Dict:
    """
    Find spouse page using page_metadata first, then fallback to content detection.
    """
    # Try page_metadata first - use step_3_1 (spouse info page)
    if loader and doc_location_url:
        # step_3_1 is the spouse info page in page_metadata
        spouse_pages = loader.get_step_pages(doc_location_url, 'step_3_1')
        if spouse_pages:
            page_num = spouse_pages[0]
            page_idx = page_num - 1
            if 0 <= page_idx < len(pages):
                return pages[page_idx]

    # Fallback to content-based detection
    return find_spouse_page(pages)


# =============================================================================
# EXTRACTION FUNCTIONS
# =============================================================================

def extract_spouse_positions_from_page(page_lines: List[Dict]) -> List[Dict]:
    """Extract spouse positions from page lines."""
    positions = []

    # Find the y-position of the "ที่อยู่ที่ติดต่อได้" section
    address_section_y = None
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) >= 8 and 'ที่อยู่ที่ติดต่อได้' in content:
            _, cy = get_polygon_center(polygon)
            address_section_y = cy
            break

    # Determine end of position section
    position_section_y_max = address_section_y - 0.2 if address_section_y else POSITION_SECTION_Y_MAX

    # Skip labels and form template text
    skip_labels = [
        'ตำแหน่งปัจจุบัน', 'หน่วยงาน / ที่ตั้ง', 'หมายเหตุ',
        'ไม่ได้ประกอบอาชีพ', 'กราบขอพร', 'ที่อยู่ที่ติดต่อได้',
        'อาคาร/หมู่บ้าน', 'ตรอก/ซอย', 'อำเภอ/เขต', 'จังหวัด',
        'รหัสไปรษณีย์', 'โทรศัพท์', 'อีเมล์', 'เลขที่', 'หมู่ที่',
        'ตำบล/แขวง', 'แขวง',
        # Form template text that should not be positions
        'ชื่อและชื่อสกุล', 'โปรดระบุคำนำหน้านาม', 'ให้ชัดเจนว่าเป็น',
        'ชื่อเดิม', 'นามสกุลเดิม', 'ชื่อสกุลเดิม',
        'สถานภาพ', 'สมรส', 'โสด', 'หย่า', 'คู่สมรสเสียชีวิต',
        'อยู่กินกันฉันสามีภริยา', 'คณะกรรมการ ป.ป.ช.', 'เมื่อวันที่',
        'วันเดือนปี เกิด', 'อายุ', 'สัญชาติ', 'เลขประจำตัวประชาชน'
    ]

    # Collect position and workplace lines
    position_lines = []
    workplace_lines = []

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])

        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Skip if outside position section
        if not (POSITION_SECTION_Y_MIN <= cy <= position_section_y_max):
            continue

        # Skip headers and labels
        if any(label in content for label in skip_labels):
            continue

        # Skip if just "ตำแหน่ง" header
        if content.strip() == 'ตำแหน่ง':
            continue

        # Skip very short or numeric-only content
        if len(content.strip()) < 4:
            continue
        if re.match(r'^[\d\s./]+$', content.strip()):
            continue

        # Position is on the left (x < 3.0), workplace on the right
        if cx < POSITION_X_MAX:
            position_lines.append({
                'content': content,
                'y': cy,
                'x': cx
            })
        else:
            workplace_lines.append({
                'content': content,
                'y': cy,
                'x': cx
            })

    # Group position lines by y-proximity
    position_groups = []
    current_group = []

    sorted_positions = sorted(position_lines, key=lambda x: x['y'])
    for pos in sorted_positions:
        if not current_group:
            current_group.append(pos)
        elif abs(pos['y'] - current_group[-1]['y']) < 0.4:
            current_group.append(pos)
        else:
            if current_group:
                position_groups.append(current_group)
            current_group = [pos]

    if current_group:
        position_groups.append(current_group)

    # Group workplace lines
    company_patterns = ['บริษัท', 'สำนักงาน', 'กรม', 'กระทรวง', 'มูลนิธิ', 'สมาคม']

    workplace_groups = []
    current_wp_group = []

    sorted_workplaces = sorted(workplace_lines, key=lambda x: x['y'])
    for wp in sorted_workplaces:
        content = wp['content']
        is_new_company = any(content.startswith(p) for p in company_patterns)

        if not current_wp_group:
            current_wp_group.append(wp)
        elif is_new_company and len(current_wp_group) > 0:
            workplace_groups.append(current_wp_group)
            current_wp_group = [wp]
        elif abs(wp['y'] - current_wp_group[-1]['y']) < 0.5:
            current_wp_group.append(wp)
        else:
            if current_wp_group:
                workplace_groups.append(current_wp_group)
            current_wp_group = [wp]

    if current_wp_group:
        workplace_groups.append(current_wp_group)

    # Match position groups with workplace groups
    for pos_group in position_groups:
        pos_y_min = min(p['y'] for p in pos_group)
        pos_y_max = max(p['y'] for p in pos_group)
        pos_y_center = (pos_y_min + pos_y_max) / 2

        # Combine position text and clean OCR artifacts
        position_text = ' '.join([p['content'] for p in sorted(pos_group, key=lambda x: x['y'])])
        position_text = clean_ocr_text(position_text)
        position_text = clean_position_text(position_text)

        # Find best matching workplace group
        best_wp_group = None
        best_distance = float('inf')

        for wp_group in workplace_groups:
            wp_y_min = min(w['y'] for w in wp_group)
            wp_y_max = max(w['y'] for w in wp_group)
            wp_y_center = (wp_y_min + wp_y_max) / 2

            if wp_y_min <= pos_y_max + 0.5 and wp_y_max >= pos_y_min - 0.2:
                distance = abs(wp_y_center - pos_y_center)
                if distance < best_distance:
                    best_distance = distance
                    best_wp_group = wp_group

        workplace_text = ''
        workplace_location = ''

        if best_wp_group:
            wp_lines = sorted(best_wp_group, key=lambda x: x['y'])

            if wp_lines:
                workplace_text = clean_ocr_text(wp_lines[0]['content'])

            if len(wp_lines) > 1:
                address_parts = [w['content'] for w in wp_lines[1:]]
                workplace_location = clean_ocr_text(' '.join(address_parts))

            workplace_groups.remove(best_wp_group)

        # Skip if position text contains form template content
        form_template_patterns = [
            'ชื่อและชื่อสกุล', 'โปรดระบุคำนำหน้านาม', 'ชื่อเดิม',
            'สถานภาพ', 'คู่สมรสเสียชีวิต', 'อยู่กินกันฉันสามีภริยา',
            'คณะกรรมการ ป.ป.ช.', 'เมื่อวันที่', 'หย่า'
        ]
        is_form_template = any(p in position_text for p in form_template_patterns)

        if position_text and not is_form_template:
            positions.append({
                'position': position_text,
                'workplace': workplace_text,
                'workplace_location': workplace_location
            })

    return positions


# =============================================================================
# MAIN EXTRACTION
# =============================================================================

def extract_spouse_position(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict,
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Dict]:
    """Extract spouse positions from JSON content."""

    spouse_positions = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    pages = json_content.get('pages', [])
    page = find_spouse_page_with_metadata(pages, loader, doc_location_url)

    if page is None:
        return spouse_positions

    page_lines = page.get('lines', [])
    positions = extract_spouse_positions_from_page(page_lines)

    for idx, pos in enumerate(positions):
        spouse_positions.append({
            'spouse_id': None,  # Will be assigned by orchestrator
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'position_period_type_id': 2,  # Current position
            'index': idx + 1,
            'position': pos.get('position', ''),
            'workplace': pos.get('workplace', ''),
            'workplace_location': pos.get('workplace_location', ''),
            'note': '',
            'latest_submitted_date': latest_submitted_date
        })

    return spouse_positions


def run_step_3_3(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 3.3 to extract spouse positions.
    """
    loader = data_loader or PipelineDataLoader(input_dir)

    all_spouse_positions = []
    next_spouse_id = 1

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']
        doc_info = context.get('doc_info', {})
        doc_location_url = doc_info.get('doc_location_url', '')

        spouse_positions = extract_spouse_position(
            json_content, nacc_detail, submitter_info,
            loader=loader, doc_location_url=doc_location_url
        )

        # Assign spouse_id based on nacc_id match
        # Note: In orchestrated mode, spouse_id comes from spouse_info
        for position in spouse_positions:
            position['spouse_id'] = next_spouse_id

        if spouse_positions:
            next_spouse_id += 1

        all_spouse_positions.extend(spouse_positions)

    # Write output
    fields = [
        'spouse_id', 'submitter_id', 'nacc_id', 'position_period_type_id', 'index',
        'position', 'workplace', 'workplace_location', 'note', 'latest_submitted_date'
    ]
    writer = CSVWriter(output_dir, 'spouse_position.csv', fields)
    count = writer.write_rows(all_spouse_positions)
    print(f"Extracted {count} spouse positions to {writer.output_path}")

    return all_spouse_positions


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')
    run_step_3_3(input_dir, output_dir)
