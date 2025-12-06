"""
Phase 1: JSON Processing and Page Matching

This module handles:
1. Sub-phase 1a: JSON Processing - Identify page types from raw OCR JSON
2. Sub-phase 1b: Page Matching - Match pages to template using layout similarity
3. Sub-phase 1c: Text Extraction - Extract text content for each page
4. Sub-phase 1d: Metadata Mapping - Map pages to extraction steps

Adapted from pipeline_oc json_processor.py and page_matcher.py
"""

import json
import re
import argparse
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

from .page_similarity import page_layout_similarity


# ============================================================================
# Page Type Configuration
# ============================================================================

# Page types that can have extra/continuation pages
EXTRA_PAGE_TYPES = {
    'children', 'siblings', 'deposits', 'investments', 'land',
    'buildings', 'vehicles', 'concessions', 'other_assets',
    'bank_loans', 'written_debts'
}

# Standard template page numbers (37 pages)
TEMPLATE_PAGE_MAP = {
    4: 'personal_info',      # หน้า 1 - ข้อมูลส่วนบุคคล
    5: 'spouse_info',        # หน้า 2 - คู่สมรส
    6: 'children',           # หน้า 3 - บุตร
    7: 'siblings',           # หน้า 4 - พี่น้อง
    8: 'income_expense',     # หน้า 5 - รายได้ต่อปี
    9: 'income_expense',     # หน้า 5 cont - รายจ่ายต่อปี
    10: 'tax_info',          # หน้า 6 - ข้อมูลการเสียภาษี
    11: 'assets_summary',    # หน้า 7 - ข้อมูลรายการทรัพย์สิน
    12: 'attachments',       # หน้า 8 - คำรับรอง
    13: 'cash',              # หน้า 9 - เงินสด
    14: 'cash',              # หน้า 9 cont
    15: 'deposits',          # หน้า 10 - เงินฝาก
    16: 'deposits',          # หน้า 10 cont
    17: 'investments',       # หน้า 11 - เงินลงทุน
    18: 'loans_given',       # หน้า 12 - เงินให้กู้ยืม
    19: 'loans_given',       # หน้า 12 cont
    20: 'loans_given',       # หน้า 12 cont
    21: 'land',              # หน้า 13 - ที่ดิน
    22: 'land',              # หน้า 13 cont
    23: 'buildings',         # หน้า 14 - โรงเรือน
    24: 'buildings',         # หน้า 14 cont
    25: 'vehicles',          # หน้า 15 - ยานพาหนะ
    26: 'vehicles',          # หน้า 15 cont
    27: 'concessions',       # หน้า 16 - สิทธิและสัมปทาน
    28: 'concessions',       # หน้า 16 cont
    29: 'other_assets',      # หน้า 17 - ทรัพย์สินอื่น
    30: 'other_assets',      # หน้า 17 cont
    31: 'overdraft',         # หน้า 18 - เงินเบิกเกินบัญชี
    32: 'overdraft',         # หน้า 18 cont
    33: 'bank_loans',        # หน้า 19 - เงินกู้จากธนาคาร
    34: 'bank_loans',        # หน้า 19 cont
    35: 'written_debts',     # หน้า 20 - หนี้สินที่มีหลักฐาน
    36: 'documents_list',    # รายละเอียดของเอกสารประกอบ
    37: 'documents_list',    # cont
}

# Page type identification patterns - ordered by priority
PAGE_PATTERNS_ORDERED = [
    # Asset detail pages - most specific patterns first
    ('cash', [r'รายละเอียดประกอบรายการเงินสด'], True),
    ('deposits', [r'รายละเอียดประกอบรายการเงินฝาก'], True),
    ('investments', [r'รายละเอียดประกอบรายการเงินลงทุน'], True),
    ('loans_given', [r'รายละเอียดประกอบรายการเงินให้กู้ยืม'], True),
    ('land', [r'รายละเอียดประกอบรายการที่ดิน'], True),
    ('buildings', [r'รายละเอียดประกอบรายการโรงเรือน'], True),
    ('vehicles', [r'รายละเอียดประกอบรายการยานพาหนะ'], True),
    ('concessions', [r'รายละเอียดประกอบรายการสิทธิและสัมปทาน'], True),
    ('other_assets', [r'รายละเอียดประกอบรายการทรัพย์สินอื่น'], True),
    ('overdraft', [r'รายละเอียดประกอบรายการเงินเบิกเกินบัญชี'], True),
    ('bank_loans', [r'รายละเอียดประกอบรายการเงินกู้จากธนาคาร'], True),
    ('written_debts', [r'รายละเอียดประกอบรายการหนี้สินที่มีหลักฐาน', r'รายละเอียดประกอบรายการหนี้สินอื่น'], True),
    ('documents_list', [r'รายละเอียดของเอกสารประกอบ', r'รายการเอกสาร'], True),

    # Summary and info pages
    ('assets_summary', [r'ข้อมูลรายการทรัพย์สินและหนี้สิน', r'ขอแสดงรายการทรัพย์สิน'], True),
    ('attachments', [r'คำรับรอง', r'แนบเอกสารประกอบ'], True),
    ('tax_info', [r'ข้อมูลการเสียภาษี', r'ภาษีเงินได้บุคคลธรรมดา'], True),
    ('income_expense', [r'ข้อมูลรายได้ต่อปี', r'รายจ่ายต่อปี.*โดยประมาณ'], True),

    # Family pages
    ('children', [r'บุตรโดยชอบด้วยกฎหมาย', r'หน้า\s*3.*บุตร', r'^.*บุตร.*ลำดับที่'], True),
    ('siblings', [r'พี่น้องร่วมบิดามารดา', r'หน้า\s*4.*พี่น้อง'], True),

    # Personal pages
    ('personal_info', [r'หน้า\s*1.*ข้อมูลส่วนบุคคล', r'ข้อมูลส่วนบุคคล.*เลขประจำตัวประชาชน'], True),
    ('spouse_info', [r'หน้า\s*2.*คู่สมรส', r'หน้า\s*2.*เลขประจำตัวประชาชน'], True),

    # Work history
    ('work_history', [r'ประวัติการทำงาน', r'หน่วยงาน.*ที่ตั้ง'], False),

    # Cover pages
    ('cover', [r'บัญชีทรัพย์สินและหนี้สิน', r'กรณีที่ยื่น'], False),
    ('summary', [r'สำเนา.*พ้นจากตำแหน่ง', r'สำนักตรวจสอบทรัพย์สิน'], False),
]

CONTINUATION_PATTERNS = [
    r'\(ต่อ\)',
    r'ต่อ\)',
    r'ต่อ$',
]


# ============================================================================
# Phase 1a: JSON Processing (Page Type Identification)
# ============================================================================

def get_page_text(page: Dict) -> str:
    """Extract all text content from a page."""
    lines = page.get('lines', [])
    texts = [line.get('content', '') for line in lines]
    return ' '.join(texts)


def get_header_text(page: Dict, num_lines: int = 10) -> str:
    """Get text from the first N lines of a page (header area)."""
    lines = page.get('lines', [])[:num_lines]
    texts = [line.get('content', '') for line in lines]
    return ' '.join(texts)


def identify_page_type(
    page: Dict,
    page_index: int,
    prev_page_type: Optional[str] = None
) -> Tuple[str, bool, bool]:
    """
    Identify the type of a page based on its content.

    Args:
        page: Page dict with 'lines'
        page_index: 0-based index of the page
        prev_page_type: Type of the previous page

    Returns:
        Tuple of (page_type, is_continuation, is_extra_page)
    """
    header_text = get_header_text(page, 15)

    # Check for continuation page markers
    is_continuation = False
    for pattern in CONTINUATION_PATTERNS:
        if re.search(pattern, header_text):
            is_continuation = True
            break

    # If continuation with previous type, inherit it
    if is_continuation and prev_page_type and prev_page_type not in ('unknown', 'cover', 'summary'):
        is_extra = prev_page_type in EXTRA_PAGE_TYPES
        return prev_page_type, True, is_extra

    # Check patterns in priority order
    for page_type, patterns, is_unique in PAGE_PATTERNS_ORDERED:
        for pattern in patterns:
            if re.search(pattern, header_text):
                is_extra = page_type in EXTRA_PAGE_TYPES and is_continuation
                return page_type, is_continuation, is_extra

    # If continuation but no pattern matched, use previous type
    if is_continuation and prev_page_type:
        is_extra = prev_page_type in EXTRA_PAGE_TYPES
        return prev_page_type, True, is_extra

    return 'unknown', False, False


def extract_page_number_marker(page: Dict) -> Optional[str]:
    """Extract the 'หน้า X' marker from the page if present."""
    header_text = get_header_text(page, 5)
    match = re.search(r'หน้า\s*(\d+)', header_text)
    if match:
        return match.group(0)
    return None


def process_json_file_phase1a(input_path: Path, output_path: Path) -> Dict:
    """
    Phase 1a: Process a single JSON file to identify page types.

    Args:
        input_path: Path to raw OCR JSON file
        output_path: Path to save processed JSON

    Returns:
        Dict with processing statistics
    """
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    pages = data.get('pages', [])

    if not pages:
        return {'error': 'No pages found', 'pages': 0}

    # Process each page
    processed_pages = []
    prev_page_type = None
    page_type_counts = {}
    extra_pages_count = 0

    for idx, page in enumerate(pages):
        page_type, is_continuation, is_extra = identify_page_type(page, idx, prev_page_type)
        page_marker = extract_page_number_marker(page)

        processed_page = {
            'page_number': page.get('page_number', idx + 1),
            'original_page_index': idx,
            'width': page.get('width', 8.2639),
            'height': page.get('height', 11.6944),
            'unit': page.get('unit', 'inch'),
            'lines': page.get('lines', []),
            '_page_info': {
                'page_type': page_type,
                'is_continuation': is_continuation,
                'is_extra_page': is_extra,
                'page_marker': page_marker,
            }
        }

        processed_pages.append(processed_page)
        prev_page_type = page_type

        type_key = f"{page_type}_cont" if is_continuation else page_type
        page_type_counts[type_key] = page_type_counts.get(type_key, 0) + 1
        if is_extra:
            extra_pages_count += 1

    has_extra_pages = extra_pages_count > 0 or len(processed_pages) > 37

    output_data = {
        'file_name': data.get('file_name', input_path.name),
        'content': data.get('content', ''),
        'pages': processed_pages,
        '_processing_info': {
            'source_file': str(input_path),
            'total_pages': len(processed_pages),
            'template_standard_pages': 37,
            'has_extra_pages': has_extra_pages,
            'extra_pages_count': extra_pages_count,
            'page_type_counts': page_type_counts,
        }
    }

    # Preserve metadata if present
    if '_metadata' in data:
        output_data['_metadata'] = data['_metadata']

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    return {
        'success': True,
        'pages': len(processed_pages),
        'page_types': page_type_counts,
    }


# ============================================================================
# Phase 1b: Page Matching
# ============================================================================

def extract_polygons_from_page(page: Dict) -> List:
    """Extract all polygons from a page's lines."""
    polygons = []
    for line in page.get('lines', []):
        if 'polygon' in line:
            polygons.append(line['polygon'])
    return polygons


def normalize_polygons(polygons: List, width: float, height: float) -> List:
    """Normalize polygon coordinates to 0-1 range."""
    normalized = []
    for poly in polygons:
        norm_poly = []
        for i, val in enumerate(poly):
            if i % 2 == 0:  # x coordinate
                norm_poly.append(val / width)
            else:  # y coordinate
                norm_poly.append(val / height)
        normalized.append(norm_poly)
    return normalized


def extract_text_from_page(page: Dict) -> str:
    """Extract all text content from a page."""
    lines = page.get('lines', [])
    return ' '.join([l.get('content', '') for l in lines])


def text_similarity(text_a: str, text_b: str) -> float:
    """Calculate Jaccard similarity between texts."""
    if not text_a or not text_b:
        return 0

    words_a = set(text_a.split())
    words_b = set(text_b.split())

    if not words_a or not words_b:
        return 0

    intersection = words_a & words_b
    union = words_a | words_b

    return len(intersection) / len(union) if union else 0


def compute_similarity_matrix(
    data_pages: List[Dict],
    template_pages: List[Dict],
    layout_weight: float = 0.7,
    text_weight: float = 0.3
) -> np.ndarray:
    """Compute similarity matrix between data pages and template pages."""
    n_data = len(data_pages)
    n_template = len(template_pages)

    similarity_matrix = np.zeros((n_data, n_template))

    # Pre-compute template data
    template_polygons_list = []
    template_texts = []
    for template_page in template_pages:
        template_width = template_page.get('width', 8.5)
        template_height = template_page.get('height', 11)
        template_polygons = extract_polygons_from_page(template_page)
        if template_polygons:
            norm_polys = normalize_polygons(template_polygons, template_width, template_height)
        else:
            norm_polys = []
        template_polygons_list.append(norm_polys)
        template_texts.append(extract_text_from_page(template_page))

    # Compute similarity for each pair
    for i, data_page in enumerate(data_pages):
        data_width = data_page.get('width', 8.5)
        data_height = data_page.get('height', 11)
        data_polygons = extract_polygons_from_page(data_page)
        data_text = extract_text_from_page(data_page)

        if data_polygons:
            data_polygons_norm = normalize_polygons(data_polygons, data_width, data_height)
        else:
            data_polygons_norm = []

        for j, (template_polygons_norm, template_text) in enumerate(zip(template_polygons_list, template_texts)):
            if data_polygons_norm and template_polygons_norm:
                layout_sim = page_layout_similarity(data_polygons_norm, template_polygons_norm)
            else:
                layout_sim = 0

            txt_sim = text_similarity(data_text, template_text)
            similarity_matrix[i, j] = layout_weight * layout_sim + text_weight * txt_sim

    return similarity_matrix


def match_pages_to_template(
    data_pages: List[Dict],
    template_pages: List[Dict],
    similarity_threshold: float = 0.3
) -> List[Tuple[int, Optional[int], float]]:
    """
    Match pages using Hungarian algorithm for optimal assignment.

    Returns:
        list of tuples (data_page_num, matched_template_page_num, similarity)
    """
    from scipy.optimize import linear_sum_assignment

    if not data_pages:
        return []

    n_data = len(data_pages)
    n_template = len(template_pages)

    similarity_matrix = compute_similarity_matrix(data_pages, template_pages)

    # Convert to cost matrix with order penalty
    order_penalty_weight = 0.1
    cost_matrix = np.zeros((n_data, n_template))

    for i in range(n_data):
        for j in range(n_template):
            expected_j = int((i / n_data) * n_template)
            order_penalty = abs(j - expected_j) / n_template
            cost_matrix[i, j] = (1 - similarity_matrix[i, j]) + order_penalty_weight * order_penalty

    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    results = []
    data_page_nums = [p.get('page_number', i+1) for i, p in enumerate(data_pages)]
    template_page_nums = [p.get('page_number', i+1) for i, p in enumerate(template_pages)]

    for i, data_page in enumerate(data_pages):
        data_page_num = data_page.get('page_number', i + 1)

        if i in row_ind:
            idx = list(row_ind).index(i)
            matched_template_idx = col_ind[idx]
            similarity = similarity_matrix[i, matched_template_idx]

            if similarity >= similarity_threshold:
                matched_template_page = template_page_nums[matched_template_idx]
                results.append((data_page_num, matched_template_page, similarity))
            else:
                results.append((data_page_num, None, similarity))
        else:
            results.append((data_page_num, None, 0))

    return results


def create_matched_json(
    data_path: Path,
    template_path: Path,
    output_path: Path,
    similarity_threshold: float = 0.3
) -> Dict:
    """
    Create matched JSON with pages aligned to template structure.

    Args:
        data_path: Path to processed JSON file (from Phase 1a)
        template_path: Path to template JSON
        output_path: Path to save matched JSON

    Returns:
        Dict with matching statistics
    """
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    with open(template_path, 'r', encoding='utf-8') as f:
        template = json.load(f)

    data_pages = data.get('pages', [])
    template_pages = template.get('pages', [])

    matches = match_pages_to_template(data_pages, template_pages, similarity_threshold)

    # Create mapping from template page to data page
    template_to_data = {}
    unmatched_data_pages = []

    for data_page_num, template_page_num, similarity in matches:
        if template_page_num is not None:
            data_page = next(
                (p for p in data_pages if p.get('page_number') == data_page_num),
                None
            )
            if data_page:
                template_to_data[template_page_num] = {
                    'page': data_page,
                    'similarity': similarity,
                    'original_page_num': data_page_num
                }
        else:
            data_page = next(
                (p for p in data_pages if p.get('page_number') == data_page_num),
                None
            )
            if data_page:
                unmatched_data_pages.append({
                    'page': data_page,
                    'original_page_num': data_page_num
                })

    # Build result pages following template structure
    result_pages = []

    for template_page in template_pages:
        template_page_num = template_page['page_number']

        if template_page_num in template_to_data:
            matched = template_to_data[template_page_num]
            result_page = {
                'page_number': template_page_num,
                'width': matched['page'].get('width', 8.2639),
                'height': matched['page'].get('height', 11.6944),
                'unit': matched['page'].get('unit', 'inch'),
                'lines': matched['page'].get('lines', []),
                '_match_info': {
                    'matched': True,
                    'similarity': matched['similarity'],
                    'original_page_number': matched['original_page_num']
                }
            }
            # Preserve page_info if present
            if '_page_info' in matched['page']:
                result_page['_page_info'] = matched['page']['_page_info']
        else:
            result_page = {
                'page_number': template_page_num,
                'width': template_page.get('width', 8.2639),
                'height': template_page.get('height', 11.6944),
                'unit': template_page.get('unit', 'inch'),
                'lines': [],
                '_match_info': {
                    'matched': False,
                    'similarity': 0,
                    'original_page_number': None
                }
            }

        result_pages.append(result_page)

    result = {
        'file_name': data.get('file_name', ''),
        'content': data.get('content', ''),
        'pages': result_pages,
        '_matching_info': {
            'template_total_pages': len(template_pages),
            'data_total_pages': len(data_pages),
            'matched_pages': len(template_to_data),
            'unmatched_data_pages': [p['original_page_num'] for p in unmatched_data_pages]
        }
    }

    # Preserve metadata
    if '_metadata' in data:
        result['_metadata'] = data['_metadata']
    if '_processing_info' in data:
        result['_processing_info'] = data['_processing_info']

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return {
        'matched': len(template_to_data),
        'unmatched': len(unmatched_data_pages),
        'total_data': len(data_pages),
        'total_template': len(template_pages)
    }


# ============================================================================
# Main Processing Functions
# ============================================================================

def process_phase1a(
    input_dir: Path,
    output_dir: Path,
    skip_existing: bool = True
) -> Dict:
    """
    Run Phase 1a: Process all raw JSON files to identify page types.

    Args:
        input_dir: Directory with raw OCR JSON files
        output_dir: Directory to save processed JSON files

    Returns:
        Processing statistics
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(input_dir.glob('*.json'))

    stats = {
        'total_files': len(json_files),
        'processed': 0,
        'skipped': 0,
        'errors': []
    }

    print(f"{'='*60}")
    print("Phase 1a: JSON Processing (Page Type Identification)")
    print(f"{'='*60}")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Total files: {len(json_files)}")
    print(f"{'='*60}")

    for i, json_file in enumerate(sorted(json_files), 1):
        output_file = output_dir / json_file.name

        if skip_existing and output_file.exists():
            print(f"[{i}/{len(json_files)}] SKIP: {json_file.name[:50]}...")
            stats["skipped"] += 1
            continue

        try:
            result = process_json_file_phase1a(json_file, output_file)

            if result.get('success'):
                stats['processed'] += 1
                print(f"[{i}/{len(json_files)}] OK: {json_file.name[:40]} ({result['pages']} pages)")
            else:
                stats['errors'].append((json_file.name, result.get('error', 'Unknown error')))
                print(f"[{i}/{len(json_files)}] ERROR: {json_file.name[:40]}: {result.get('error')}")

        except Exception as e:
            stats['errors'].append((json_file.name, str(e)))
            print(f"[{i}/{len(json_files)}] ERROR: {json_file.name[:40]}: {e}")

    return stats


def process_phase1b(
    input_dir: Path,
    output_dir: Path,
    template_path: Path,
    similarity_threshold: float = 0.3,
    skip_existing: bool = True
) -> Dict:
    """
    Run Phase 1b: Match pages to template.

    Args:
        input_dir: Directory with processed JSON files (from Phase 1a)
        output_dir: Directory to save matched JSON files
        template_path: Path to template JSON

    Returns:
        Processing statistics
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    json_files = list(input_dir.glob('*.json'))

    stats = {
        'total_files': len(json_files),
        'processed': 0,
        'skipped': 0,
        'total_matched': 0,
        'total_unmatched': 0,
        'errors': []
    }

    print(f"{'='*60}")
    print("Phase 1b: Page Matching")
    print(f"{'='*60}")
    print(f"Input: {input_dir}")
    print(f"Output: {output_dir}")
    print(f"Template: {template_path}")
    print(f"Total files: {len(json_files)}")
    print(f"{'='*60}")

    for i, json_file in enumerate(sorted(json_files), 1):
        output_file = output_dir / json_file.name

        if skip_existing and output_file.exists():
            print(f"[{i}/{len(json_files)}] SKIP: {json_file.name[:50]}...")
            stats["skipped"] += 1
            continue

        try:
            result = create_matched_json(
                json_file,
                template_path,
                output_file,
                similarity_threshold
            )

            stats['processed'] += 1
            stats['total_matched'] += result['matched']
            stats['total_unmatched'] += result['unmatched']
            print(f"[{i}/{len(json_files)}] OK: {json_file.name[:40]} ({result['matched']}/{result['total_data']} matched)")

        except Exception as e:
            stats['errors'].append((json_file.name, str(e)))
            print(f"[{i}/{len(json_files)}] ERROR: {json_file.name[:40]}: {e}")

    return stats


def run_phase1(
    input_dir: Path,
    output_dir: Path,
    template_path: Path,
    is_final: bool = False,
    skip_existing: bool = True
) -> Dict:
    """
    Run complete Phase 1 (1a + 1b).

    Args:
        input_dir: Directory with raw OCR JSON files
        output_dir: Base output directory
        template_path: Path to template JSON
        is_final: Whether processing test final data

    Returns:
        Combined statistics
    """
    # Phase 1a output goes to a temp directory, then Phase 1b produces final output
    phase1a_output = output_dir.parent / "extract_processed"

    print("\n" + "="*70)
    print("PHASE 1: JSON Processing and Page Matching")
    print("="*70 + "\n")

    # Phase 1a
    stats_1a = process_phase1a(input_dir, phase1a_output, skip_existing)

    print("\n")

    # Phase 1b
    stats_1b = process_phase1b(phase1a_output, output_dir, template_path, skip_existing=skip_existing)

    print("\n" + "="*60)
    print("Phase 1 Complete")
    print("="*60)
    print(f"Phase 1a: {stats_1a['processed']} processed, {stats_1a['skipped']} skipped, {len(stats_1a['errors'])} errors")
    print(f"Phase 1b: {stats_1b['processed']} processed, {stats_1b['skipped']} skipped, {len(stats_1b['errors'])} errors")
    print(f"Total matched pages: {stats_1b['total_matched']}")
    print("="*60)

    return {
        'phase1a': stats_1a,
        'phase1b': stats_1b
    }


def process_phase1c(
    input_dir: Path,
    output_dir: Path,
    clean: bool = True,
    skip_existing: bool = False
) -> Dict:
    """
    Run Phase 1c: Extract text content from each page.

    Args:
        input_dir: Directory with matched JSON files (extract_matched/)
        output_dir: Directory to save text_each_page output

    Returns:
        Processing statistics
    """
    from .phase1c_text_extract import process_phase1c as run_phase1c
    return run_phase1c(input_dir, output_dir, clean, skip_existing)


def process_phase1d(
    input_dir: Path,
    output_dir: Path,
    skip_existing: bool = False
) -> Dict:
    """
    Run Phase 1d: Map pages to extraction steps.

    Args:
        input_dir: Directory with matched JSON files
        output_dir: Directory to save metadata output

    Returns:
        Processing statistics
    """
    from .phase1d_metadata import process_phase1d as run_phase1d
    return run_phase1d(input_dir, output_dir, skip_existing)


def main():
    """Main entry point for Phase 1."""
    parser = argparse.ArgumentParser(
        description="Phase 1: JSON Processing and Page Matching"
    )
    parser.add_argument(
        "--phase",
        choices=["1a", "1b", "1c", "1d", "all"],
        default="all",
        help="Which phase to run (1a=page ID, 1b=matching, 1c=text extract, 1d=metadata, all=1a+1b)"
    )
    parser.add_argument(
        "--final",
        action="store_true",
        help="Process test final data instead of training data"
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Don't skip existing files"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Override input directory"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Override output directory"
    )
    parser.add_argument(
        "--template",
        type=str,
        default=None,
        help="Override template JSON path"
    )

    args = parser.parse_args()

    # Determine base paths
    src_dir = Path(__file__).parent.parent
    utils_dir = src_dir / "utils"

    # Template path
    template_path = Path(args.template) if args.template else utils_dir / "template-docs_raw.json"

    skip_existing = not args.no_skip

    # Determine directories based on phase
    if args.phase == "1c":
        # Phase 1c reads from extract_matched, outputs to text_each_page
        if args.input_dir:
            input_dir = Path(args.input_dir)
        elif args.final:
            input_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
        else:
            input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

        if args.output_dir:
            output_dir = Path(args.output_dir)
        elif args.final:
            output_dir = src_dir / "result" / "final" / "processing_input" / "text_each_page"
        else:
            output_dir = src_dir / "result" / "from_train" / "processing_input" / "text_each_page"

        process_phase1c(input_dir, output_dir, clean=not skip_existing, skip_existing=skip_existing)

    elif args.phase == "1d":
        # Phase 1d reads from extract_matched, outputs to page_metadata
        if args.input_dir:
            input_dir = Path(args.input_dir)
        elif args.final:
            input_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
        else:
            input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

        if args.output_dir:
            output_dir = Path(args.output_dir)
        elif args.final:
            output_dir = src_dir / "result" / "final" / "processing_input" / "page_metadata"
        else:
            output_dir = src_dir / "result" / "from_train" / "processing_input" / "page_metadata"

        process_phase1d(input_dir, output_dir, skip_existing=skip_existing)

    else:
        # Phases 1a, 1b, all
        if args.input_dir:
            input_dir = Path(args.input_dir)
        elif args.final:
            input_dir = src_dir / "result" / "final" / "processing_input" / "extract_raw"
        else:
            input_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_raw"

        if args.output_dir:
            output_dir = Path(args.output_dir)
        elif args.final:
            output_dir = src_dir / "result" / "final" / "processing_input" / "extract_matched"
        else:
            output_dir = src_dir / "result" / "from_train" / "processing_input" / "extract_matched"

        if args.phase == "1a":
            process_phase1a(input_dir, output_dir, skip_existing)
        elif args.phase == "1b":
            process_phase1b(input_dir, output_dir, template_path, skip_existing=skip_existing)
        else:
            run_phase1(input_dir, output_dir, template_path, args.final, skip_existing)


if __name__ == "__main__":
    main()
