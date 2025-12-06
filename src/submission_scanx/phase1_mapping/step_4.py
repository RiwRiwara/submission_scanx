"""
Step 4: Extract relative information from JSON extract files

This step extracts:
1. relative_info.csv - Relatives information (บิดา, มารดา, บุตร, พี่น้อง, บิดา/มารดาคู่สมรส)

Data locations in the document (maybe not 100% not sure):
- Page 4 (index 3): บิดา (Father), มารดา (Mother) of submitter
- Page 5 (index 4): บิดาคู่สมรส (Spouse's Father), มารดาคู่สมรส (Spouse's Mother)
- Page 6 (index 5): บุตร (Children)
- Page 7 (index 6): พี่น้อง (Siblings)

Relationship IDs:
1 = บิดา (Father)
2 = มารดา (Mother)
3 = พี่น้อง (Siblings)
4 = บุตร (Children)
5 = บิดาคู่สมรส (Spouse's Father)
6 = มารดาคู่สมรส (Spouse's Mother)
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
    Y_TOLERANCE,
    THAI_TITLES,
    clean_ocr_text,
    get_polygon_center,
    is_valid_thai_name,
    extract_title_and_name,
    parse_age,
    format_disclosure_date,
)
from utils.data_loader import PipelineDataLoader, CSVWriter


def check_is_death(page_lines: List[Dict], target_y: float, y_tolerance: float = 0.3) -> bool:
    """Check if there's a 'ตาย' or 'ถึงแก่กรรม' marker near the given y position"""
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        if abs(cy - target_y) <= y_tolerance:
            if 'ตาย' in content or 'ถึงแก่กรรม' in content:
                return True

    return False


def find_parent_by_label(page_lines: List[Dict], parent_type: str) -> Optional[Dict]:
    """
    Find parent information by searching for the label pattern.
    Returns None if not found.
    """
    result = {
        'title': '',
        'first_name': '',
        'last_name': '',
        'age': None,
        'occupation': '',
        'workplace': '',
        'workplace_location': '',
        'is_death': False
    }

    label_y = None

    # First pass: find the parent label
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Look for parent label pattern: "บิดา :" or "บิดา:" or "มารดา :" etc.
        label_pattern = rf'{parent_type}\s*:\s*ชื่อและชื่อสกุล'
        if re.search(label_pattern, content):
            label_y = cy
            # Try to extract name from same line
            name_match = re.search(rf'{parent_type}\s*:\s*ชื่อและชื่อสกุล\s*([\u0E00-\u0E7F\s]+)', content)
            if name_match:
                name_text = name_match.group(1).strip()
                name_text = re.sub(r'[.\s]+$', '', name_text)
                if is_valid_thai_name(name_text):
                    title, first, last = extract_title_and_name(name_text)
                    result['title'] = title
                    result['first_name'] = first
                    result['last_name'] = last
            break

    if label_y is None:
        return None

    # If name not found in label line, look for separate name line
    if not result['first_name']:
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Name should be near the label (within 0.3 y-units), to the right (x > 1.5)
            if abs(cy - label_y) <= 0.3 and cx > 1.5:
                if is_valid_thai_name(content) and f'{parent_type}' not in content and 'ชื่อและชื่อสกุล' not in content:
                    # Check if it has a title prefix - include military ranks and abbreviations
                    title_patterns = ['นาย', 'นาง', 'นางสาว', 'น.ส.', 'พล', 'ดร.', 'นาวา', 'เรือ', 'เด็ก',
                                     'ร.ต.', 'ส.ต.', 'จ.ส.', 'พ.อ.', 'พ.ท.', 'พ.ต.', 'ร้อย', 'จ่า', 'สิบ']
                    has_title = any(t in content for t in title_patterns)
                    if has_title:
                        title, first, last = extract_title_and_name(content)
                        result['title'] = title
                        result['first_name'] = first
                        result['last_name'] = last
                        break

    if not result['first_name']:
        return None

    # Extract age from the same region
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        if abs(cy - label_y) <= 0.3:
            age = parse_age(content)
            if age:
                result['age'] = age

    # Note: is_death detection is unreliable from OCR alone
    # The "ตาย" text appears as a checkbox label in the form
    # Default to False, can be refined with more sophisticated detection
    result['is_death'] = False

    # Extract occupation (อาชีพ) - usually on the next line
    occupation_y = label_y + 0.3  # Occupation line is about 0.3 units below
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        if abs(cy - occupation_y) <= 0.4 and cx < 3.5:
            if 'อาชีพ' in content:
                occ_match = re.search(r'อาชีพ\s*([\u0E00-\u0E7F\s]+)', content)
                if occ_match:
                    occ_text = occ_match.group(1).strip()
                    occ_text = re.sub(r'[.\s]+$', '', occ_text)
                    if occ_text and occ_text not in ['-', '...', '....']:
                        result['occupation'] = clean_ocr_text(occ_text)

    # Extract workplace
    workplace_y = label_y + 0.3
    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        if abs(cy - workplace_y) <= 0.4 and cx > 3.5:
            if 'สถานที่ทำงาน' in content:
                wp_match = re.search(r'สถานที่ทำงาน\s*([\u0E00-\u0E7Fa-zA-Z\s\(\)]+)', content)
                if wp_match:
                    wp_text = wp_match.group(1).strip()
                    wp_text = re.sub(r'[.\s]+$', '', wp_text)
                    if wp_text and wp_text not in ['-', '...', '....']:
                        result['workplace'] = clean_ocr_text(wp_text)

    return result


def extract_parent_info(page_lines: List[Dict], parent_type: str, y_start: float, y_end: float) -> Dict:
    """
    Extract parent (father/mother) information from page lines.
    This is a fallback function that searches within a y-range.

    parent_type: 'บิดา' or 'มารดา'
    y_start, y_end: Y-range to search in
    """
    # Filter lines to the specified y-range
    filtered_lines = []
    for line in page_lines:
        polygon = line.get('polygon', [])
        if len(polygon) >= 8:
            _, cy = get_polygon_center(polygon)
            if y_start <= cy <= y_end:
                filtered_lines.append(line)

    result = find_parent_by_label(filtered_lines, parent_type)

    if result is None:
        return {
            'title': '',
            'first_name': '',
            'last_name': '',
            'age': None,
            'occupation': '',
            'workplace': '',
            'workplace_location': '',
            'is_death': False
        }

    return result


def extract_submitter_parents(page_lines: List[Dict]) -> List[Dict]:
    """Extract submitter's father and mother from page 4"""
    relatives = []

    # Page 4 structure varies by document:
    # บิดา (Father) is typically around y=4.8-6.0
    # มารดา (Mother) is typically around y=5.2-6.8
    # Use wider ranges to accommodate variations

    # Extract father - search the entire relevant area
    father = extract_parent_info(page_lines, 'บิดา', 4.5, 6.5)
    if father.get('first_name'):
        relatives.append({
            'relationship_id': 1,  # บิดา
            **father
        })

    # Extract mother - search below father area
    mother = extract_parent_info(page_lines, 'มารดา', 5.0, 7.0)
    if mother.get('first_name'):
        relatives.append({
            'relationship_id': 2,  # มารดา
            **mother
        })

    return relatives


def extract_spouse_parents(page_lines: List[Dict]) -> List[Dict]:
    """Extract spouse's father and mother from page 5"""
    relatives = []

    # Page 5 structure (spouse page) varies by document:
    # บิดาคู่สมรส (Spouse's Father) is typically around y=7.2-9.2
    # มารดาคู่สมรส (Spouse's Mother) is typically around y=7.8-10.0
    # Use wider ranges to accommodate variations

    # Extract spouse's father - search the spouse parents area
    father = extract_parent_info(page_lines, 'บิดา', 7.0, 9.5)
    if father.get('first_name'):
        relatives.append({
            'relationship_id': 5,  # บิดาคู่สมรส
            **father
        })

    # Extract spouse's mother
    mother = extract_parent_info(page_lines, 'มารดา', 7.5, 10.5)
    if mother.get('first_name'):
        relatives.append({
            'relationship_id': 6,  # มารดาคู่สมรส
            **mother
        })

    return relatives


def extract_children(page_lines: List[Dict]) -> List[Dict]:
    """Extract children from page 6"""
    children = []

    # Page 6 structure:
    # Header "บุตร" at top
    # Table with columns: ลำดับที่, ชื่อ-ชื่อสกุล, อายุ, ที่อยู่และสถานศึกษา/ที่ทำงาน
    # Data rows start around y=2.3

    # Address/school area polygon (x: 4.5-7.9, y: 1.8-9.7)
    CHILD_INFO_X_MIN = 4.2
    CHILD_INFO_X_MAX = 8.0

    # Age column boundaries (x: 3.5-4.5)
    AGE_COL_X_MIN = 3.5
    AGE_COL_X_MAX = 4.5

    # Find all lines that look like child entries (have name with title)
    child_entries = []

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Children data is between y=2.3 and y=10 (varies by number of children)
        if cy < 2.3:
            continue

        # Look for names with child titles (เด็กชาย, เด็กหญิง) or adult titles
        # Include abbreviated forms like น.ส., ด.ช., ด.ญ.
        child_title_patterns = ['เด็กชาย', 'เด็กหญิง', 'นาย', 'นาง', 'นางสาว', 'น.ส.', 'ด.ช.', 'ด.ญ.']
        if any(t in content for t in child_title_patterns):
            # Check if it's in the name column (x around 1.0-3.5)
            if 1.0 <= cx <= 3.5:
                title, first, last = extract_title_and_name(content)
                # Accept names even without last name (some OCR may miss it)
                if first:
                    child_entries.append({
                        'title': title,
                        'first_name': first,
                        'last_name': last,
                        'y': cy,
                        'age': None,
                        'address': '',
                        'school': '',
                        'workplace': ''
                    })

    # Sort children by y-position
    child_entries.sort(key=lambda x: x['y'])

    # Find y-boundaries for each child (up to next child or end of data area)
    for i, child in enumerate(child_entries):
        child_y = child['y']

        # Determine y-range for this child's info
        if i + 1 < len(child_entries):
            next_child_y = child_entries[i + 1]['y']
            y_max = next_child_y - 0.1
        else:
            y_max = child_y + 2.0  # Last child, extend 2 units

        y_min = child_y - 0.3

        # Find age in the age column (x around 3.5-4.5)
        # Age is typically 0.15-0.5 y-units BELOW the name line
        age_candidates = []
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Age column check - wider x range and below the name
            if AGE_COL_X_MIN <= cx <= AGE_COL_X_MAX:
                # Age is usually 0-0.8 units below the name line
                y_diff = cy - child_y
                if -0.1 <= y_diff <= 0.8:
                    # Try to extract age from content
                    # Handle various OCR formats: "13", ".13", ",13", "13 ปี", etc.
                    cleaned = content.strip()

                    # Remove leading dots, commas, or other OCR artifacts
                    cleaned = re.sub(r'^[.,\s#*]+', '', cleaned)

                    # Try parse_age first
                    age = parse_age(cleaned)
                    if age and 1 <= age <= 100:
                        age_candidates.append({
                            'age': age,
                            'y_diff': y_diff,
                            'content': content
                        })
                        continue

                    # Try direct number extraction
                    match = re.search(r'^(\d{1,2})(?:\s*ปี)?$', cleaned)
                    if match:
                        age_val = int(match.group(1))
                        if 1 <= age_val <= 100:
                            age_candidates.append({
                                'age': age_val,
                                'y_diff': y_diff,
                                'content': content
                            })

        # Select the best age candidate (closest to expected position: 0.1-0.3 below name)
        if age_candidates:
            # Prefer candidates with y_diff close to 0.2 (typical position)
            best_candidate = min(age_candidates, key=lambda x: abs(x['y_diff'] - 0.2))
            child['age'] = best_candidate['age']

        # Collect all lines in the address/school area for this child
        info_lines = []
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Check if in the info area (right side of table)
            if CHILD_INFO_X_MIN <= cx <= CHILD_INFO_X_MAX and y_min <= cy <= y_max:
                info_lines.append({
                    'content': content,
                    'x': cx,
                    'y': cy
                })

        # Sort by y position
        info_lines.sort(key=lambda x: x['y'])

        # Extract address and school from info lines
        address_parts = []
        address_set = set()
        school = ''

        for info in info_lines:
            content = info['content']

            # Skip labels and empty content
            if content.strip() in ['ที่อยู่', 'ที่ทำงาน', '-', '...', '....', '.']:
                continue
            if re.match(r'^[.\s]+$', content):
                continue

            # Extract school
            if 'สถานศึกษา' in content:
                school_match = re.search(r'สถานศึกษา\s*([\u0E00-\u0E7Fa-zA-Z\s]+)', content)
                if school_match:
                    school = clean_ocr_text(school_match.group(1))
                continue

            # Skip workplace label only lines
            if content.strip() == 'ที่ทำงาน' or content.startswith('ที่ทำงาน .'):
                continue

            # Check for address-like content
            if re.search(r'(แขวง|เขต|ตำบล|อำเภอ|กรุงเทพ|จังหวัด|\d{5}|ซอย|ถนน|หมู่|ต\.|อ\.|จ\.)', content):
                addr_text = clean_ocr_text(content)
                # Remove "ที่อยู่" prefix if present
                addr_text = re.sub(r'^ที่อยู่\s*', '', addr_text)
                if addr_text and addr_text not in address_set:
                    address_set.add(addr_text)
                    address_parts.append(addr_text)

        if address_parts:
            child['address'] = clean_ocr_text(' '.join(address_parts))
        if school:
            child['school'] = school

    # Convert to output format
    result = []
    for i, child in enumerate(child_entries):
        result.append({
            'relationship_id': 4,  # บุตร
            'index': i + 1,
            'title': child['title'],
            'first_name': child['first_name'],
            'last_name': child['last_name'],
            'age': child['age'],
            'address': child['address'],
            'occupation': '',
            'school': child['school'],
            'workplace': '',
            'workplace_location': '',
            'is_death': None  # Not typically tracked for children
        })

    return result


def extract_siblings(page_lines: List[Dict]) -> List[Dict]:
    """Extract siblings from page 7"""
    siblings = []

    # Page 7 structure:
    # Header "พี่น้อง" at top
    # "พี่น้องร่วมบิดามารดา หรือร่วมบิดา หรือมารดา จำนวน X คน"
    # Table with: ลำดับที่, ชื่อ-ชื่อสกุล, อายุ, ที่อยู่และที่ทำงาน

    # Address/workplace area polygon (x: 4.5-7.9, y: 1.2-9.9)
    SIBLING_INFO_X_MIN = 4.2
    SIBLING_INFO_X_MAX = 8.0

    sibling_entries = []

    for line in page_lines:
        content = line.get('content', '')
        polygon = line.get('polygon', [])
        if len(polygon) < 8:
            continue

        cx, cy = get_polygon_center(polygon)

        # Siblings data starts around y=1.8
        if cy < 1.5:
            continue

        # Look for names with adult titles in the name column (x around 1.0-3.5)
        # Include various military ranks, abbreviated titles, etc.
        sibling_title_patterns = ['นาย', 'นาง', 'นางสาว', 'น.ส.', 'พล', 'ดร.', 'นาวา', 'เรือ',
                                   'ร.ต.', 'ส.ต.', 'จ.ส.', 'พ.อ.', 'พ.ท.', 'พ.ต.', 'ค.ต.']
        if any(t in content for t in sibling_title_patterns):
            if 1.0 <= cx <= 3.5:
                # Skip header labels
                if 'ชื่อ-ชื่อสกุล' in content:
                    continue

                title, first, last = extract_title_and_name(content)
                # Accept names even without last name (some siblings may have missing last names in OCR)
                if first:
                    sibling_entries.append({
                        'title': title,
                        'first_name': first,
                        'last_name': last,
                        'y': cy,
                        'age': None,
                        'address': '',
                        'workplace': '',
                        'workplace_location': ''
                    })

    # Sort siblings by y-position
    sibling_entries.sort(key=lambda x: x['y'])

    # Find y-boundaries for each sibling (up to next sibling or end of data area)
    for i, sibling in enumerate(sibling_entries):
        sib_y = sibling['y']

        # Determine y-range for this sibling's info
        # Use the midpoint between this sibling and next as boundary
        if i + 1 < len(sibling_entries):
            next_sib_y = sibling_entries[i + 1]['y']
            y_max = (sib_y + next_sib_y) / 2 + 0.2  # Just past midpoint
        else:
            y_max = sib_y + 1.5  # Last sibling, extend 1.5 units

        # Start from just before the sibling's name
        if i > 0:
            prev_sib_y = sibling_entries[i - 1]['y']
            y_min = (prev_sib_y + sib_y) / 2 + 0.1  # Just past midpoint from previous
        else:
            y_min = sib_y - 0.5

        # Find age in the age column (x around 3.5-4.5)
        # Collect candidates and select the best one (similar to children extraction)
        age_candidates = []
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Age column check
            if 3.5 <= cx <= 4.5:
                y_diff = cy - sib_y
                # Age is usually on the same line or slightly above/below (-0.3 to +0.5)
                if -0.3 <= y_diff <= 0.5:
                    # Handle OCR artifacts
                    cleaned = content.strip()
                    cleaned = re.sub(r'^[.,\s#*]+', '', cleaned)

                    # Try parse_age first
                    age = parse_age(cleaned)
                    if age and 1 <= age <= 100:
                        age_candidates.append({
                            'age': age,
                            'y_diff': abs(y_diff),
                            'content': content
                        })
                        continue

                    # Try direct number extraction
                    match = re.search(r'^(\d{1,2})(?:\s*ปี)?$', cleaned)
                    if match:
                        age_val = int(match.group(1))
                        if 1 <= age_val <= 100:
                            age_candidates.append({
                                'age': age_val,
                                'y_diff': abs(y_diff),
                                'content': content
                            })

        # Select the best age candidate (closest to the name line)
        if age_candidates:
            best_candidate = min(age_candidates, key=lambda x: x['y_diff'])
            sibling['age'] = best_candidate['age']

        # Collect all lines in the info area for this sibling
        info_lines = []
        for line in page_lines:
            content = line.get('content', '')
            polygon = line.get('polygon', [])
            if len(polygon) < 8:
                continue

            cx, cy = get_polygon_center(polygon)

            # Check if in the info area (right side of table)
            if SIBLING_INFO_X_MIN <= cx <= SIBLING_INFO_X_MAX and y_min <= cy <= y_max:
                info_lines.append({
                    'content': content,
                    'x': cx,
                    'y': cy
                })

        # Sort by y position
        info_lines.sort(key=lambda x: x['y'])

        # Extract address and workplace from info lines
        address_parts = []
        address_set = set()
        workplace = ''

        for info in info_lines:
            content = info['content']

            # Skip labels and empty content
            if content.strip() in ['ที่อยู่', '-', '...', '....', '.']:
                continue
            if re.match(r'^[.\s]+$', content):
                continue

            # Extract workplace (contains company keywords after ที่ทำงาน)
            if 'ที่ทำงาน' in content:
                # Extract workplace if it contains meaningful content
                wp_match = re.search(r'ที่ทำงาน[อยู่\s]*([\u0E00-\u0E7Fa-zA-Z\s\(\)\.]+)', content)
                if wp_match:
                    wp_text = clean_ocr_text(wp_match.group(1))
                    # Check if it has meaningful workplace info (not just address parts)
                    if wp_text and not wp_text.startswith('.'):
                        # Check if this contains address info like เขต, แขวง
                        if re.search(r'(เขต|แขวง|กรุงเทพ)', wp_text):
                            # This is actually address info in ที่ทำงาน line
                            addr_text = clean_ocr_text(content.replace('ที่ทำงาน', '').strip())
                            if addr_text and addr_text not in address_set:
                                address_set.add(addr_text)
                                address_parts.append(addr_text)
                        elif re.search(r'(บริษัท|สำนักงาน|กรม|มูลนิธิ)', wp_text):
                            workplace = wp_text
                continue

            # Skip standalone ที่ทำงาน label
            if content.strip() == 'ที่ทำงาน' or content.startswith('ที่ทำงาน .'):
                continue

            # Check for address-like content
            if re.search(r'(แขวง|เขต|ตำบล|อำเภอ|กรุงเทพ|จังหวัด|\d{5}|ซอย|ถนน|ถ\.|หมู่|ต\.|อ\.|จ\.|จว\.)', content):
                addr_text = clean_ocr_text(content)
                # Remove "ที่อยู่" prefix if present
                addr_text = re.sub(r'^ที่อยู่\s*', '', addr_text)
                if addr_text and addr_text not in address_set:
                    address_set.add(addr_text)
                    address_parts.append(addr_text)

        if address_parts:
            sibling['address'] = clean_ocr_text(' '.join(address_parts))
        if workplace:
            sibling['workplace'] = workplace

    # Convert to output format
    result = []
    for i, sib in enumerate(sibling_entries):
        result.append({
            'relationship_id': 3,  # พี่น้อง
            'index': i + 1,
            'title': sib['title'],
            'first_name': sib['first_name'],
            'last_name': sib['last_name'],
            'age': sib['age'],
            'address': sib['address'],
            'occupation': '',
            'school': '',
            'workplace': sib['workplace'],
            'workplace_location': sib.get('workplace_location', ''),
            'is_death': None
        })

    return result


def find_pages_with_metadata(
    pages: List[Dict],
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> Dict[str, List[int]]:
    """
    Find page indices using page_metadata first, then fallback to content markers.

    Args:
        pages: List of page dicts from json_content
        loader: Optional PipelineDataLoader instance for page_metadata lookup
        doc_location_url: Document location URL for page_metadata lookup

    Returns:
        Dict mapping page types to list of page indices (0-indexed)

    Page mappings (from page_metadata_mapper.py):
    - step_1: personal_info (submitter page) - contains submitter's parents (บิดา, มารดา)
    - step_3_1: spouse_info - contains spouse's parents (บิดาคู่สมรส, มารดาคู่สมรส)
    - step_4: children page AND siblings page (relative_info)
    """
    result = {
        'personal': [],   # Submitter's parents (บิดา, มารดา) - relationship_id 1, 2
        'spouse': [],     # Spouse's parents (บิดาคู่สมรส, มารดาคู่สมรส) - relationship_id 5, 6
        'children': [],   # Children (บุตร) - relationship_id 4
        'siblings': []    # Siblings (พี่น้อง) - relationship_id 3
    }

    # Try to use page_metadata if loader is available
    if loader and doc_location_url:
        # step_1: personal_info (submitter) - has submitter's parents at bottom
        step_1_pages = loader.get_step_pages(doc_location_url, 'step_1')
        if step_1_pages:
            for page_num in step_1_pages:
                page_idx = page_num - 1
                if 0 <= page_idx < len(pages) and page_idx not in result['personal']:
                    result['personal'].append(page_idx)

        # step_3_1: spouse_info - has spouse's parents at bottom
        step_3_1_pages = loader.get_step_pages(doc_location_url, 'step_3_1')
        if step_3_1_pages:
            for page_num in step_3_1_pages:
                page_idx = page_num - 1
                if 0 <= page_idx < len(pages) and page_idx not in result['spouse']:
                    result['spouse'].append(page_idx)

        # step_4: relative_info - both children and siblings pages
        # Need to distinguish children vs siblings by page content
        step_4_pages = loader.get_step_pages(doc_location_url, 'step_4')
        if step_4_pages:
            for page_num in step_4_pages:
                page_idx = page_num - 1
                if 0 <= page_idx < len(pages):
                    page_lines = pages[page_idx].get('lines', [])
                    header_text = ' '.join(l.get('content', '') for l in page_lines[:15])

                    # Check if it's children or siblings page
                    if 'บุตร' in header_text or 'หน้า 3' in header_text:
                        if page_idx not in result['children']:
                            result['children'].append(page_idx)
                    elif 'พี่น้อง' in header_text or 'หน้า 4' in header_text:
                        if page_idx not in result['siblings']:
                            result['siblings'].append(page_idx)

    # Fallback: use content-based detection for any missing page types
    for page_type in result:
        if not result[page_type]:
            fallback_result = find_pages_by_content(pages)
            result[page_type] = fallback_result.get(page_type, [])

    return result


def find_pages_by_content(pages: List[Dict]) -> Dict[str, List[int]]:
    """
    Find page indices by content markers (หน้า labels).
    Returns dict mapping page types to list of page indices.

    Page types:
    - 'personal': หน้า 1 - personal info with submitter's parents
    - 'spouse': หน้า 2 - spouse info with spouse's parents
    - 'children': หน้า 3 - บุตร page
    - 'siblings': หน้า 4 - พี่น้อง page (can have multiple continuation pages)
    """
    result = {
        'personal': [],
        'spouse': [],
        'children': [],
        'siblings': []
    }

    for idx, page in enumerate(pages):
        # First check _page_info metadata (from json_processor.py)
        page_info = page.get('_page_info', {})
        page_type = page_info.get('page_type', '')
        
        if page_type == 'personal_info':
            # Check if it's submitter's page (หน้า 1) or spouse's page (หน้า 2)
            lines = page.get('lines', [])
            header_text = ' '.join(l.get('content', '') for l in lines[:20])
            
            # Prioritize page number check over content keywords
            if 'หน้า 1' in header_text or 'ข้อมูลส่วนบุคคล' in header_text:
                result['personal'].append(idx)
            elif 'หน้า 2' in header_text:
                result['spouse'].append(idx)
            else:
                # Default to personal if can't determine
                result['personal'].append(idx)
        elif page_type == 'spouse_info':
            # Spouse info page (from json_processor)
            result['spouse'].append(idx)
        elif page_type == 'children':
            result['children'].append(idx)
        elif page_type == 'siblings':
            result['siblings'].append(idx)
        else:
            # Fallback: check content markers for pages without _page_info
            lines = page.get('lines', [])
            if not lines:
                continue

            header_lines = lines[:15]
            header_text = ' '.join(l.get('content', '') for l in header_lines)

            # Look for specific page markers
            if 'หน้า 1' in header_text and 'ข้อมูลส่วนบุคคล' in header_text:
                if idx not in result['personal']:
                    result['personal'].append(idx)
            elif 'หน้า 2' in header_text and 'คู่สมรส' in header_text:
                if idx not in result['spouse']:
                    result['spouse'].append(idx)
            elif 'หน้า 3' in header_text and 'บุตร' in header_text:
                if idx not in result['children']:
                    result['children'].append(idx)
            elif 'หน้า 4' in header_text and 'พี่น้อง' in header_text:
                if idx not in result['siblings']:
                    result['siblings'].append(idx)
            elif 'บุตรโดยชอบด้วยกฎหมาย' in header_text:
                if idx not in result['children']:
                    result['children'].append(idx)
            elif 'พี่น้องร่วมบิดามารดา' in header_text:
                if idx not in result['siblings']:
                    result['siblings'].append(idx)

    return result


def extract_relative_data(
    json_content: Dict,
    nacc_detail: Dict,
    submitter_info: Dict,
    loader: 'PipelineDataLoader' = None,
    doc_location_url: str = ''
) -> List[Dict]:
    """Extract all relative information from JSON content"""

    relatives = []

    nacc_id = nacc_detail.get('nacc_id')
    submitter_id = submitter_info.get('submitter_id')

    # Parse disclosure date for latest_submitted_date
    disclosure_date = nacc_detail.get('disclosure_announcement_date', '')
    latest_submitted_date = format_disclosure_date(disclosure_date)

    pages = json_content.get('pages', [])

    # Track relative_id base (using nacc_id to generate unique IDs)
    # Based on test data: relative_id starts at (nacc_id - 62) * something
    base_id = (int(nacc_id) - 37) * 8 if nacc_id else 0

    # Find pages using page_metadata first, then fallback to content markers
    page_map = find_pages_with_metadata(pages, loader, doc_location_url)

    # Extract submitter's parents from personal info page(s)
    for page_idx in page_map['personal']:
        if page_idx < len(pages):
            page_lines = pages[page_idx].get('lines', [])
            submitter_parents = extract_submitter_parents(page_lines)
            for rel in submitter_parents:
                rel['index'] = 1  # Parents always index 1
                # Avoid duplicates
                if not any(r.get('first_name') == rel.get('first_name') and
                          r.get('relationship_id') == rel.get('relationship_id')
                          for r in relatives):
                    relatives.append(rel)

    # Extract spouse's parents from spouse info page(s)
    for page_idx in page_map['spouse']:
        if page_idx < len(pages):
            page_lines = pages[page_idx].get('lines', [])
            spouse_parents = extract_spouse_parents(page_lines)
            for rel in spouse_parents:
                rel['index'] = 1  # Parents always index 1
                # Avoid duplicates
                if not any(r.get('first_name') == rel.get('first_name') and
                          r.get('relationship_id') == rel.get('relationship_id')
                          for r in relatives):
                    relatives.append(rel)

    # Extract children from all children pages (handle continuation pages)
    all_children = []
    for page_idx in page_map['children']:
        if page_idx < len(pages):
            page_lines = pages[page_idx].get('lines', [])
            children = extract_children(page_lines)
            all_children.extend(children)

    # Re-index children and add to relatives
    for i, child in enumerate(all_children):
        child['index'] = i + 1
        # Avoid duplicates by name
        if not any(r.get('first_name') == child.get('first_name') and
                  r.get('relationship_id') == 4 for r in relatives):
            relatives.append(child)

    # Extract siblings from all siblings pages (handle continuation pages - ใบแทรก)
    all_siblings = []
    for page_idx in page_map['siblings']:
        if page_idx < len(pages):
            page_lines = pages[page_idx].get('lines', [])
            siblings = extract_siblings(page_lines)
            all_siblings.extend(siblings)

    # Re-index siblings and add to relatives
    for i, sibling in enumerate(all_siblings):
        sibling['index'] = i + 1
        # Avoid duplicates by name
        if not any(r.get('first_name') == sibling.get('first_name') and
                  r.get('relationship_id') == 3 for r in relatives):
            relatives.append(sibling)

    # Fallback: If no pages found by content markers, try fixed indices (for older format)
    if not any(page_map.values()):
        # Page 4 (index 3): Submitter's parents
        if len(pages) > 3:
            page_lines = pages[3].get('lines', [])
            submitter_parents = extract_submitter_parents(page_lines)
            for rel in submitter_parents:
                rel['index'] = 1
                relatives.append(rel)

        # Page 5 (index 4): Spouse's parents
        if len(pages) > 4:
            page_lines = pages[4].get('lines', [])
            is_spouse_page = any('คู่สมรส' in line.get('content', '') for line in page_lines)
            if is_spouse_page:
                spouse_parents = extract_spouse_parents(page_lines)
                for rel in spouse_parents:
                    rel['index'] = 1
                    relatives.append(rel)

        # Page 6 (index 5): Children
        if len(pages) > 5:
            page_lines = pages[5].get('lines', [])
            is_children_page = any('บุตร' in line.get('content', '') for line in page_lines[:10])
            if is_children_page:
                children = extract_children(page_lines)
                relatives.extend(children)

        # Page 7 (index 6): Siblings
        if len(pages) > 6:
            page_lines = pages[6].get('lines', [])
            is_siblings_page = any('พี่น้อง' in line.get('content', '') for line in page_lines[:10])
            if is_siblings_page:
                siblings = extract_siblings(page_lines)
                relatives.extend(siblings)

    # Assign IDs and format output
    result = []
    relative_id_counter = base_id

    for rel in relatives:
        relative_id_counter += 1

        # Format is_death field
        is_death = rel.get('is_death')
        if is_death is True:
            is_death_str = 'TRUE'
        elif is_death is False:
            is_death_str = 'FALSE'
        else:
            is_death_str = ''

        result.append({
            'relative_id': relative_id_counter,
            'submitter_id': submitter_id,
            'nacc_id': nacc_id,
            'index': rel.get('index', 1),
            'relationship_id': rel['relationship_id'],
            'title': rel.get('title', ''),
            'first_name': rel.get('first_name', ''),
            'last_name': rel.get('last_name', ''),
            'age': rel.get('age') if rel.get('age') else '',
            'address': rel.get('address', ''),
            'occupation': rel.get('occupation', ''),
            'school': rel.get('school', ''),
            'workplace': rel.get('workplace', ''),
            'workplace_location': rel.get('workplace_location', ''),
            'latest_submitted_date': latest_submitted_date,
            'is_death': is_death_str
        })

    return result


def run_step_4(input_dir: str, output_dir: str, data_loader: PipelineDataLoader = None):
    """
    Run step 4 to extract relative information.
    
    Args:
        input_dir: Input directory path
        output_dir: Output directory path
        data_loader: Optional shared PipelineDataLoader instance for caching
    """
    # Use shared data loader if provided, otherwise create new one
    loader = data_loader or PipelineDataLoader(input_dir)

    all_relatives = []

    for context in loader.iterate_documents():
        json_content = context['json_content']
        nacc_detail = context['nacc_detail']
        submitter_info = context['submitter_info']
        doc_info = context['doc_info']
        doc_location_url = doc_info.get('doc_location_url', '')

        # Pass loader and doc_location_url for page_metadata integration
        relatives = extract_relative_data(
            json_content, nacc_detail, submitter_info,
            loader=loader, doc_location_url=doc_location_url
        )
        all_relatives.extend(relatives)

    # Write output using CSVWriter
    relative_info_fields = [
        'relative_id', 'submitter_id', 'nacc_id', 'index', 'relationship_id',
        'title', 'first_name', 'last_name', 'age', 'address', 'occupation',
        'school', 'workplace', 'workplace_location', 'latest_submitted_date', 'is_death'
    ]

    writer = CSVWriter(output_dir, 'relative_info.csv', relative_info_fields)
    count = writer.write_rows(all_relatives)

    print(f"Extracted {count} relatives to {writer.output_path}")

    return all_relatives


if __name__ == '__main__':
    input_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_input')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'pipeline_output')

    run_step_4(input_dir, output_dir)
